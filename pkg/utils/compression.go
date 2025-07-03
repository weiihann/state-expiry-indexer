package utils

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

type ZstdEncoder struct {
	encoder *zstd.Encoder
}

func NewZstdEncoder() (*ZstdEncoder, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	return &ZstdEncoder{encoder: encoder}, nil
}

func (c *ZstdEncoder) Close() error {
	return c.encoder.Close()
}

func (c *ZstdEncoder) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot compress empty data")
	}

	compressed := c.encoder.EncodeAll(data, nil)
	if len(compressed) == 0 {
		return nil, fmt.Errorf("compression failed: output is empty")
	}
	return compressed, nil
}

type ZstdDecoder struct {
	decoder *zstd.Decoder
}

func NewZstdDecoder() (*ZstdDecoder, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	return &ZstdDecoder{decoder: decoder}, nil
}

func (c *ZstdDecoder) Close() {
	c.decoder.Close()
}

func (c *ZstdDecoder) Decompress(compressedData []byte) ([]byte, error) {
	if len(compressedData) == 0 {
		return nil, fmt.Errorf("cannot decompress empty data")
	}

	decompressed, err := c.decoder.DecodeAll(compressedData, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	if len(decompressed) == 0 {
		return nil, fmt.Errorf("decompression failed: output is empty")
	}

	return decompressed, nil
}

// GetCompressionRatio calculates the compression ratio as a percentage.
// Returns the space saved: 0% = no compression, 90% = excellent compression.
func GetCompressionRatio(originalSize, compressedSize int) float64 {
	if originalSize == 0 {
		return 0
	}
	return float64(originalSize-compressedSize) / float64(originalSize) * 100
}

// ValidateCompressedData checks if the compressed data is valid zstd format.
// This is useful for validating compressed files before attempting decompression.
func ValidateCompressedData(compressedData []byte) error {
	if len(compressedData) == 0 {
		return fmt.Errorf("compressed data is empty")
	}

	// Try to create a decoder and validate the header
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	defer decoder.Close()

	// Attempt to decode just to validate format - use small buffer
	_, err = decoder.DecodeAll(compressedData, make([]byte, 0, 1024))
	if err != nil {
		return fmt.Errorf("invalid zstd compressed data: %w", err)
	}

	return nil
}
