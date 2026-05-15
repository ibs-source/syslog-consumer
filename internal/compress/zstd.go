// Package compress provides Zstandard compression and decompression using
// channel-based freelists for encoders and decoders. Channel freelists
// survive GC sweeps (unlike sync.Pool) so the heavy zstd codec objects are
// not recreated after every GC cycle.
package compress

import (
	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/klauspost/compress/zstd"
)

var cfg *config.CompressConfig

var decoderFree chan *zstd.Decoder

// Init must be called once before any Decompress call. The config pointer
// is retained — no copy is made.
func Init(c *config.CompressConfig) {
	cfg = c
	decoderFree = make(chan *zstd.Decoder, cfg.FreelistSize)
	for range min(cfg.WarmupCount, cfg.FreelistSize) {
		decoderFree <- newDecoder()
	}
}

func newEncoder() *zstd.Encoder {
	e, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithSingleSegment(true),
		zstd.WithZeroFrames(true),
	)
	if err != nil {
		panic("compress: zstd encoder: " + err.Error())
	}
	return e
}

func newDecoder() *zstd.Decoder {
	d, err := zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(uint64(cfg.MaxDecompressBytes)), //nolint:gosec // validated > 0 at startup
	)
	if err != nil {
		panic("compress: zstd decoder: " + err.Error())
	}
	return d
}

func getDecoder() *zstd.Decoder {
	select {
	case d := <-decoderFree:
		return d
	default:
		return newDecoder()
	}
}

func putDecoder(d *zstd.Decoder) {
	select {
	case decoderFree <- d:
	default:
		d.Close()
	}
}

// NewEncoder returns an encoder for exclusive use by a single goroutine
// (typically a publish worker), avoiding the freelist overhead of Compress.
func NewEncoder() *zstd.Encoder {
	return newEncoder()
}

// EncodeWith compresses src into dst[:0]. The caller must hold exclusive
// access to enc.
func EncodeWith(enc *zstd.Encoder, dst, src []byte) []byte {
	return enc.EncodeAll(src, dst[:0])
}

// Decompress is thread-safe and zero-alloc in steady state.
func Decompress(dst, src []byte) ([]byte, error) {
	dec := getDecoder()
	out, err := dec.DecodeAll(src, dst[:0])
	putDecoder(dec)
	return out, err
}

// IsCompressed checks for the Zstandard magic 0xFD2FB528 (RFC 8878 §3.1.1).
func IsCompressed(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == 0x28 && data[1] == 0xB5 &&
		data[2] == 0x2F && data[3] == 0xFD
}
