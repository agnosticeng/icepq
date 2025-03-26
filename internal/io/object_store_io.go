package io

import (
	"context"
	"errors"
	stdio "io"
	"io/fs"
	"net/url"
	"path/filepath"
	"time"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/agnosticeng/objstr/utils"
	"github.com/apache/iceberg-go/io"
)

var _ io.WriteFileIO = &ObjectStoreIO{}

type ObjectStoreIO struct {
	os *objstr.ObjectStore
}

func NewObjectStoreIO(os *objstr.ObjectStore) *ObjectStoreIO {
	return &ObjectStoreIO{os: os}
}

func (ad *ObjectStoreIO) Remove(name string) error {
	u, err := url.Parse(name)

	if err != nil {
		return err
	}

	return ad.os.Delete(context.Background(), u)
}

func (ad *ObjectStoreIO) Open(name string) (io.File, error) {
	u, err := url.Parse(name)

	if err != nil {
		return nil, err
	}

	md, err := ad.os.ReadMetadata(context.Background(), u)

	if err != nil {
		return nil, err
	}

	r, err := ad.os.ReaderAt(context.Background(), u)

	if err != nil {
		return nil, err
	}

	return &fileAdapter{
		ReadSeekerAdapter: NewReadSeekerAdapter(r, int64(md.Size)),
		ad:                ad,
		path:              u,
		md:                md,
	}, nil
}

func (ad *ObjectStoreIO) Create(name string) (io.FileWriter, error) {
	u, err := url.Parse(name)

	if err != nil {
		return nil, err
	}

	w, err := ad.os.Writer(context.Background(), u)

	if err != nil {
		return nil, err
	}

	return fileWriterAdapter{Writer: w}, nil
}

func (ad *ObjectStoreIO) WriteFile(name string, p []byte) error {
	u, err := url.Parse(name)

	if err != nil {
		return err
	}

	return utils.CreateObject(context.Background(), ad.os, u, p)
}

type fileWriterAdapter struct {
	types.Writer
}

func (fwa fileWriterAdapter) ReadFrom(r stdio.Reader) (n int64, err error) {
	var total int

	for {
		var buf = make([]byte, 32*1024)
		n, err := r.Read(buf)

		if errors.Is(err, stdio.EOF) {
			return int64(total), nil
		}

		n, err = fwa.Write(buf[:n])
		total += n

		if err != nil {
			return int64(total), err
		}
	}
}

type fileAdapter struct {
	*ReadSeekerAdapter
	ad   *ObjectStoreIO
	path *url.URL
	md   *types.ObjectMetadata
}

func (fa *fileAdapter) Stat() (fs.FileInfo, error) {
	return &fileInfoAdapter{
		fa: fa,
	}, nil
}

type fileInfoAdapter struct {
	fa *fileAdapter
}

func (fia *fileInfoAdapter) Name() string {
	return filepath.Base(fia.fa.path.Path)
}

func (fia *fileInfoAdapter) Size() int64 {
	return int64(fia.fa.md.Size)
}

func (fia *fileInfoAdapter) Mode() fs.FileMode {
	return 0
}

func (fia *fileInfoAdapter) ModTime() time.Time {
	return fia.fa.md.ModificationDate
}

func (fia *fileInfoAdapter) IsDir() bool {
	return false
}

func (fia *fileInfoAdapter) Sys() any {
	return nil
}
