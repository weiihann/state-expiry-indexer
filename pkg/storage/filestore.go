package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/weiihann/state-expiry-indexer/pkg/utils"
)

type FileStore struct {
	Path    string
	encoder *utils.ZstdEncoder
}

func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}
	return &FileStore{Path: path}, nil
}

func NewFileStoreWithCompression(path string, compressionEnabled bool) (*FileStore, error) {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}

	fs := &FileStore{
		Path: path,
	}

	// Initialize encoder if compression is enabled
	if compressionEnabled {
		encoder, err := utils.NewZstdEncoder()
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
		}
		fs.encoder = encoder
	}

	return fs, nil
}

func (fs *FileStore) Save(filename string, data []byte) error {
	return os.WriteFile(filepath.Join(fs.Path, filename), data, 0o644)
}

func (fs *FileStore) SaveCompressed(filename string, data []byte) error {
	if fs.encoder == nil {
		return fmt.Errorf("compression is not enabled for this FileStore")
	}

	if len(data) == 0 {
		return fmt.Errorf("cannot compress empty data")
	}

	// Compress the data
	compressed, err := fs.encoder.Compress(data)
	if err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	// Append .zst extension to filename
	compressedFilename := filename + ".zst"

	// Write compressed data to file
	return os.WriteFile(filepath.Join(fs.Path, compressedFilename), compressed, 0o644)
}

func (fs *FileStore) Close() error {
	if fs.encoder != nil {
		return fs.encoder.Close()
	}
	return nil
}
