package io

import (
	"errors"
	"io"
)

// Credits to https://github.com/google/wuffs/blob/v0.2.0/lib/readerat/readerat.go at this is a near-verbatim copy

var (
	ErrInvalidSize            = errors.New("readerat: invalid size")
	ErrSeekToInvalidWhence    = errors.New("readerat: seek to invalid whence")
	ErrSeekToNegativePosition = errors.New("readerat: seek to negative position")
)

type ReadSeekerAdapter struct {
	r      io.ReaderAt
	size   int64
	offset int64
}

func NewReadSeekerAdapter(r io.ReaderAt, size int64) *ReadSeekerAdapter {
	return &ReadSeekerAdapter{
		r:    r,
		size: size,
	}
}

func (rs *ReadSeekerAdapter) Read(p []byte) (int, error) {
	if rs.size < 0 {
		return 0, ErrInvalidSize
	}

	if rs.size <= rs.offset {
		return 0, io.EOF
	}

	var length = rs.size - rs.offset

	if int64(len(p)) > length {
		p = p[:length]
	}

	if len(p) == 0 {
		return 0, nil
	}

	var actual, err = rs.r.ReadAt(p, rs.offset)

	rs.offset += int64(actual)

	if (err == nil) && (rs.offset == rs.size) {
		err = io.EOF
	}

	return actual, err
}

func (rs *ReadSeekerAdapter) Seek(offset int64, whence int) (int64, error) {
	if rs.size < 0 {
		return 0, ErrInvalidSize
	}

	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += rs.offset
	case io.SeekEnd:
		offset += rs.size
	default:
		return 0, ErrSeekToInvalidWhence
	}

	if rs.offset < 0 {
		return 0, ErrSeekToNegativePosition
	}

	rs.offset = offset
	return rs.offset, nil
}

func (rs *ReadSeekerAdapter) ReadAt(p []byte, off int64) (int, error) {
	return rs.r.ReadAt(p, off)
}

func (res *ReadSeekerAdapter) Close() error {
	return nil
}
