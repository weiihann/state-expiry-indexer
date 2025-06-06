package storage

import (
	"os"
	"path/filepath"
)

type FileStore struct {
	Path string
}

func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}
	return &FileStore{Path: path}, nil
}

func (fs *FileStore) Save(filename string, data []byte) error {
	return os.WriteFile(filepath.Join(fs.Path, filename), data, 0o644)
}
