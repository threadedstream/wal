package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	KB = 1 * 1024
	MB = KB * 1024

	entrySizeBytes    = 4
	checksumSizeBytes = 4
)

const (
	checkpointPeriod = time.Second * 10

	maxSegmentSize = 100 * MB
)

var (
	enc = binary.BigEndian
)

// Opts contains WAL options
type Opts struct {
	Dir         string
	SegmentSize int
}

type segmentIterator struct {
	reader   io.Reader
	segments []string
	currIdx  int
}

func newSegmentIterator(segments []string) (*segmentIterator, error) {
	file, err := os.OpenFile(segments[0], os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	return &segmentIterator{
		segments: segments,
		reader:   file,
	}, nil
}

func (si *segmentIterator) Next() ([]byte, error) {
	res, err := si.readRecord()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if errors.Is(err, io.EOF) {
		// open next segment and read record from there
		if err := si.nextSegment(); err != nil {
			return nil, err
		}

		return si.readRecord()
	}

	return res, nil
}

func (si *segmentIterator) nextSegment() error {
	if (si.currIdx + 1) < len(si.segments) {
		si.currIdx++
		file, err := os.OpenFile(si.segments[si.currIdx], os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		si.reader = file
		return nil
	}
	return io.EOF
}

func (si *segmentIterator) readRecord() ([]byte, error) {
	var dataLen [4]byte
	_, err := si.reader.Read(dataLen[:])
	if err != nil {
		return nil, err
	}
	var chksm [4]byte
	_, err = si.reader.Read(chksm[:])
	if err != nil {
		return nil, err
	}

	data := make([]byte, enc.Uint32(dataLen[:]))
	_, err = si.reader.Read(data)
	if data == nil {
		println("break")
	}

	if computeChecksum(data) != enc.Uint32(chksm[:]) {
		return nil, errors.New("checksums don't match")
	}

	return data, err
}

// WriteAheadLog implements WAL functionality
type WriteAheadLog struct {
	writer      *bytes.Buffer
	file        *os.File
	off         int
	segmentsNum int
	opts        Opts
	done        chan struct{}
	logger      *zap.Logger
	mtx         sync.RWMutex
}

// NewWriteAheadLog returns a WAL instance, should be called once
func NewWriteAheadLog(logger *zap.Logger, opts Opts) (*WriteAheadLog, error) {
	wal := &WriteAheadLog{
		done:   make(chan struct{}),
		logger: logger,
		opts:   opts,
	}

	var err error
	wal.segmentsNum, err = wal.getLastSegmentNum()
	if err != nil {
		return nil, err
	}

	wal.file, err = os.OpenFile(fmt.Sprintf("%s/%d", wal.opts.Dir, wal.segmentsNum), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	wal.fixOpts()

	wal.writer = bytes.NewBuffer(make([]byte, 0, opts.SegmentSize))

	go wal.checkpoint()

	return wal, nil
}

func (wal *WriteAheadLog) getSegments() ([]string, error) {
	filenames, err := filepath.Glob(fmt.Sprintf("%s/*", wal.opts.Dir))
	if err != nil {
		wal.logger.Error("failed to glob", zap.Error(err))
	}
	if len(filenames) == 0 {
		return nil, nil
	}

	sort.Strings(filenames)
	return filenames, nil
}

func (wal *WriteAheadLog) getLastSegmentNum() (int, error) {
	segments, err := wal.getSegments()
	if err != nil {
		return 0, err
	}
	if len(segments) == 0 {
		return 0, nil
	}

	split := strings.Split(segments[len(segments)-1], "/")

	return strconv.Atoi(split[len(split)-1])
}

func (wal *WriteAheadLog) fixOpts() {
	if wal.opts.SegmentSize == 0 || wal.opts.SegmentSize > maxSegmentSize {
		wal.opts.SegmentSize = maxSegmentSize
	}
}

// Write writes data to internal buffer
func (wal *WriteAheadLog) Write(data []byte) error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	return wal.write(data)
}

// Replay calls f on wal data
func (wal *WriteAheadLog) Replay(f func(chunk []byte) error) error {
	wal.mtx.RLock()
	defer wal.mtx.RUnlock()

	segments, err := wal.getSegments()
	if err != nil {
		return err
	}

	if len(segments) == 0 {
		return nil
	}

	iterator, err := newSegmentIterator(segments)
	if err != nil {
		return err
	}

	for {
		data, err := iterator.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		if errors.Is(err, io.EOF) || data == nil {
			break
		}

		if err = f(data); err != nil {
			return err
		}
	}

	return nil
}

func (wal *WriteAheadLog) commit() error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	if _, err := wal.writer.WriteTo(wal.file); err != nil {
		return err
	}
	wal.writer.Reset()

	return wal.file.Sync()
}

// Close closes WAL
func (wal *WriteAheadLog) Close() error {
	if err := wal.commit(); err != nil {
		return fmt.Errorf("failed to sync data: %w", err)
	}

	if err := wal.file.Close(); err != nil {
		wal.logger.Error("failed to close file", zap.Error(err))
	}
	wal.done <- struct{}{}
	return nil
}

func (wal *WriteAheadLog) checkpoint() {
	ticker := time.NewTicker(checkpointPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-wal.done:
			wal.logger.Info("quit checkpoint routine")
			return
		case <-ticker.C:
			if err := wal.commit(); err != nil {
				wal.logger.Error("failed to commit data to disk", zap.Error(err))
			}
		}
	}
}

func (wal *WriteAheadLog) write(data []byte) error {
	if wal.off+len(data)+checksumSizeBytes+entrySizeBytes >= wal.opts.SegmentSize {
		// open a new file
		if err := wal.openNewSegment(); err != nil {
			return err
		}
	}
	var dataLen [entrySizeBytes]byte
	enc.PutUint32(dataLen[:], uint32(len(data)))
	written, err := wal.writer.Write(dataLen[:])
	if err != nil {
		return err
	}
	if written != len(dataLen) {
		return fmt.Errorf("expected to write %d bytes, wrote %d", 4, written)
	}

	var checksum [checksumSizeBytes]byte
	enc.PutUint32(checksum[:], computeChecksum(data))
	written, err = wal.writer.Write(checksum[:])
	if err != nil {
		return err
	}
	if written != checksumSizeBytes {
		return fmt.Errorf("expected to write %d bytes, wrote %d", checksumSizeBytes, written)
	}

	written, err = wal.writer.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return fmt.Errorf("expected to write %d bytes, wrote %d", len(data), written)
	}

	wal.off += entrySizeBytes + checksumSizeBytes + len(data)
	return nil
}

func (wal *WriteAheadLog) openNewSegment() error {
	wal.segmentsNum++

	// flush previous buffer, just in case it had any data
	if _, err := wal.writer.WriteTo(wal.file); err != nil {
		return err
	}

	// garbage collect the old writer
	wal.writer.Reset()

	if err := wal.file.Close(); err != nil {
		return err
	}

	newFile, err := os.OpenFile(fmt.Sprintf("%s/%d", wal.opts.Dir, wal.segmentsNum), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	wal.file = newFile

	// zero off out
	wal.off = 0

	return nil
}

func computeChecksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}
