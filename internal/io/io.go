package io

import (
	"context"
	"io/fs"
	"net/url"
	"path/filepath"
	"time"

	"github.com/agnosticeng/objstr"
	"github.com/agnosticeng/objstr/types"
	"github.com/apache/iceberg-go/io"
)

var _ io.IO = &ObjectStorageAdapter{}

type ObjectStorageAdapter struct {
	os *objstr.ObjectStore
}

func NewObjectStorageAdapter(os *objstr.ObjectStore) *ObjectStorageAdapter {
	return &ObjectStorageAdapter{os: os}
}

func (ad *ObjectStorageAdapter) Remove(name string) error {
	u, err := url.Parse(name)

	if err != nil {
		return err
	}

	return ad.os.Delete(context.Background(), u)
}

func (ad *ObjectStorageAdapter) Open(name string) (io.File, error) {
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

type fileAdapter struct {
	*ReadSeekerAdapter
	ad   *ObjectStorageAdapter
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
