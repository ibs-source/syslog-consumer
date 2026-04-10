// Package compress provides Zstandard compression and decompression using
// channel-based freelists for encoders and decoders.
// Channel freelists survive GC sweeps unlike sync.Pool, preventing
// re-creation of heavy zstd codec objects after each GC cycle.
package compress

import (
	"github.com/klauspost/compress/zstd"
)

// freelistSize is the capacity for encoder/decoder freelists.
// Sized for max(MQTT_POOL_SIZE * parseWorkersPerClient) concurrent users.
const freelistSize = 128

var decoderFree = make(chan *zstd.Decoder, freelistSize)

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
	d, err := zstd.NewReader(nil)
	if err != nil {
		panic("compress: zstd decoder: " + err.Error())
	}
	return d
}

// warmupCount is the number of encoders/decoders pre-created at init
// to eliminate cold-start allocation latency (~500µs per codec instance).
const warmupCount = 4

func init() {
	for range warmupCount {
		decoderFree <- newDecoder()
	}
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

// NewEncoder returns a new zstd encoder for exclusive use by a single
// goroutine (e.g. a publish worker). This avoids the channel freelist
// overhead of Compress when the caller can own the encoder for its lifetime.
func NewEncoder() *zstd.Encoder {
	return newEncoder()
}

// EncodeWith compresses src into dst[:0] using the provided encoder.
// The caller must ensure exclusive access to enc (one encoder per goroutine).
// This is the zero-overhead path for publish workers that own their encoder.
func EncodeWith(enc *zstd.Encoder, dst, src []byte) []byte {
	return enc.EncodeAll(src, dst[:0])
}

// Decompress decompresses a Zstandard payload into dst[:0].
// Thread-safe, zero allocations in steady state.
func Decompress(dst, src []byte) ([]byte, error) {
	dec := getDecoder()
	out, err := dec.DecodeAll(src, dst[:0])
	putDecoder(dec)
	return out, err
}

// IsCompressed reports whether data is a Zstandard frame.
// Zstandard magic: 0xFD2FB528 (RFC 8878 §3.1.1).
func IsCompressed(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == 0x28 && data[1] == 0xB5 &&
		data[2] == 0x2F && data[3] == 0xFD
}
