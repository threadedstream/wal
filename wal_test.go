package wal

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWALWrite(t *testing.T) {
	logger := newLogger()
	dirPath := "/Users/gildarov/toys/wal/data"
	require.NoError(t, os.Mkdir(dirPath, 0777))
	defer func() {
		require.NoError(t, os.RemoveAll(dirPath))
	}()

	opts := Opts{
		Dir:         dirPath,
		SegmentSize: KB * 5,
	}
	wal, err := NewWriteAheadLog(logger, opts)
	require.NoError(t, err)

	dataToWrite := make([][]byte, 1000)
	for i := range 1000 {
		dataToWrite[i] = []byte(fmt.Sprintf("datadata-%d", i))
		require.NoError(t, wal.Write(dataToWrite[i]))
	}

	replayedData := make([][]byte, 0, 1000)
	fn := func(data []byte) error {
		replayedData = append(replayedData, data)
		return nil
	}

	require.NoError(t, wal.Replay(fn))

	require.Equal(t, replayedData, dataToWrite)
}

func BenchmarkTestWALWrite(b *testing.B) {
}

func newLogger() *zap.Logger {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return logger
}
