package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	KB = 1 * 1024
	MB = KB * 1024
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
	reader *bufio.Reader
}

func (si *segmentIterator) Next() ([]byte, error) {
	return si.readRecord()
}

func (si *segmentIterator) readRecord() ([]byte, error) {
	var dataLen [4]byte
	_, err := si.reader.Read(dataLen[:])
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	data := make([]byte, enc.Uint32(dataLen[:]))
	_, err = si.reader.Read(data)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return data, err
}

// WriteAheadLog implements WAL functionality
type WriteAheadLog struct {
	buf         *bufio.Writer
	file        *os.File
	off         int
	segmentsNum int
	opts        Opts
	done        chan struct{}
	logger      *zap.Logger
}

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

	go wal.checkpoint()

	return wal, nil
}

func (wal *WriteAheadLog) getLastSegmentNum() (int, error) {
	filenames, err := filepath.Glob(fmt.Sprintf("%s/*", wal.opts.Dir))
	if err != nil {
		wal.logger.Error("failed to glob", zap.Error(err))
	}
	if len(filenames) == 0 {
		return 0, nil
	}

	sort.Strings(filenames)

	split := strings.Split(filenames[len(filenames)-1], "/")

	return strconv.Atoi(split[len(split)-1])
}

func (wal *WriteAheadLog) fixOpts() {
	if wal.opts.SegmentSize == 0 || wal.opts.SegmentSize > maxSegmentSize {
		wal.opts.SegmentSize = maxSegmentSize
	}
}

// Write writes data to internal buffer
func (wal *WriteAheadLog) Write(data []byte) error {
	return wal.write(data)
}

// Replay calls f on wal data
func (wal *WriteAheadLog) Replay(idx int, f func(chunk []byte) error) error {
	file, err := os.OpenFile(fmt.Sprintf("%s/%d", wal.opts.Dir, idx), os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	iterator := &segmentIterator{
		reader: bufio.NewReader(file),
	}

	for {
		data, err := iterator.Next()
		if err != nil {
			return err
		}
		if data == nil {
			break
		}
		if err = f(data); err != nil {
			return err
		}
	}

	return nil
}

func (wal *WriteAheadLog) commit() error {
	if err := wal.buf.Flush(); err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

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
	if wal.off+len(data)+4 >= wal.opts.SegmentSize {
		// open a new file
		if err := wal.openNewSegment(); err != nil {
			return err
		}
	}
	var dataLen [4]byte
	enc.PutUint32(dataLen[:], uint32(len(data)))
	written, err := wal.buf.Write(dataLen[:])
	if err != nil {
		return err
	}
	if written != len(dataLen) {
		return fmt.Errorf("expected to write %d bytes, wrote %d", 4, written)
	}

	written, err = wal.buf.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return fmt.Errorf("expected to write %d bytes, wrote %d", len(data), written)
	}

	wal.off += len(dataLen) + len(data)
	return nil
}

func (wal *WriteAheadLog) openNewSegment() error {
	wal.segmentsNum++
	if err := wal.file.Close(); err != nil {
		return err
	}

	newFile, err := os.OpenFile(fmt.Sprintf("%s/%d", wal.opts.Dir, wal.segmentsNum), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	wal.file = newFile

	// flush previous buffer, just in case it had any data
	if err = wal.buf.Flush(); err != nil {
		return err
	}

	wal.buf = bufio.NewWriter(wal.file)

	return nil
}
