package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWALWrite(t *testing.T) {
	logger := newLogger()
	opts := Opts{
		Dir:         "/Users/gildarov/toys/wal/data",
		SegmentSize: KB * 13,
	}
	wal, err := NewWriteAheadLog(logger, opts)
	require.NoError(t, err)
	require.NoError(t, wal.Write([]byte("hello")))
}

func newLogger() *zap.Logger {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return logger
}
