package compress

import (
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/klauspost/compress/zstd"
)

func TestMain(m *testing.M) {
	Init(&config.CompressConfig{
		FreelistSize:       128,
		MaxDecompressBytes: 256 << 20,
		WarmupCount:        4,
	})
	os.Exit(m.Run())
}

// encode is a test helper that compresses src using a fresh encoder.
func encode(dst, src []byte) []byte {
	enc := newEncoder()
	return enc.EncodeAll(src, dst[:0])
}

func TestDecompress_RoundTrip(t *testing.T) {
	src := []byte(`{"host":"srv1","msg":"hello world"}` + "\n" +
		`{"host":"srv2","msg":"test message"}` + "\n")

	compressed := encode(nil, src)

	got, err := Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("round-trip mismatch:\n got: %q\nwant: %q", got, src)
	}
}

func TestDecompress_InvalidInput(t *testing.T) {
	_, err := Decompress(nil, []byte("not zstd data"))
	if err == nil {
		t.Fatal("expected error for invalid input")
	}
}

func TestDecompress_DstReuse(t *testing.T) {
	src := []byte("reuse test payload")
	compressed := encode(nil, src)
	dst := make([]byte, 1024)

	got, err := Decompress(dst, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("mismatch after dst reuse")
	}
}

func TestDecompress_Concurrent(t *testing.T) {
	src := []byte("concurrent decompress test payload — should be safe")
	compressed := encode(nil, src)
	const goroutines = 8

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			var buf []byte
			for range 20 {
				var err error
				buf, err = Decompress(buf, compressed)
				if err != nil {
					t.Errorf("Decompress: %v", err)
					return
				}
				if !bytes.Equal(buf, src) {
					t.Error("mismatch in concurrent decompress")
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestEncodeWith_RoundTrip(t *testing.T) {
	src := []byte(`{"host":"srv1","msg":"hello world"}` + "\n" +
		`{"host":"srv2","msg":"test message"}` + "\n")

	enc := NewEncoder()
	compressed := EncodeWith(enc, nil, src)
	if !IsCompressed(compressed) {
		t.Fatal("output is not a valid zstd frame")
	}

	got, err := Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("round-trip mismatch:\n got: %q\nwant: %q", got, src)
	}
}

func TestEncodeWith_DstReuse(t *testing.T) {
	src := []byte("reuse test payload")
	enc := NewEncoder()
	dst := make([]byte, 1024)

	dst = EncodeWith(enc, dst, src)
	first := make([]byte, len(dst))
	copy(first, dst)

	dst = EncodeWith(enc, dst, src)
	if !bytes.Equal(dst, first) {
		t.Fatal("EncodeWith with reused dst produced different output")
	}
}

func TestNewEncoder_Reuse(t *testing.T) {
	enc := NewEncoder()
	payloads := [][]byte{
		[]byte(`{"host":"srv1","msg":"first"}`),
		[]byte(`{"host":"srv2","msg":"second payload, longer"}`),
		[]byte(`short`),
	}

	var dst []byte
	for _, src := range payloads {
		dst = EncodeWith(enc, dst, src)
		got, err := Decompress(nil, dst)
		if err != nil {
			t.Fatalf("Decompress after encoder reuse: %v (src=%q)", err, src)
		}
		if !bytes.Equal(got, src) {
			t.Fatalf("round-trip mismatch: got %q, want %q", got, src)
		}
	}
}

func TestIsCompressed_ZstdFrame(t *testing.T) {
	compressed := encode(nil, []byte("test data"))
	if !IsCompressed(compressed) {
		t.Fatal("IsCompressed should return true for zstd frame")
	}
}

func TestIsCompressed_PlainText(t *testing.T) {
	if IsCompressed([]byte(`{"key":"value"}`)) {
		t.Fatal("IsCompressed should return false for plain JSON")
	}
}

func TestIsCompressed_TooShort(t *testing.T) {
	if IsCompressed([]byte{0x28, 0xB5, 0x2F}) {
		t.Fatal("IsCompressed should return false for < 4 bytes")
	}
	if IsCompressed(nil) {
		t.Fatal("IsCompressed should return false for nil")
	}
}

func TestPutDecoder_FreeFull(t *testing.T) {
	// Fill the freelist completely.
	saved := make([]*zstd.Decoder, 0, cfg.FreelistSize)
	for range cfg.FreelistSize {
		d := getDecoder()
		saved = append(saved, d)
	}
	if len(saved) != cfg.FreelistSize {
		t.Fatalf("drained %d decoders, want %d", len(saved), cfg.FreelistSize)
	}
	for _, d := range saved {
		putDecoder(d)
	}

	// Now one more — should overflow (Close is called, no panic).
	extra := getDecoder()
	putDecoder(extra) // freelist full → Close() called
	t.Logf("overflow decoder closed without panic (freelist size=%d)", cfg.FreelistSize)

	// Verify the freelist is still functional after overflow.
	d := getDecoder()
	out, err := d.DecodeAll(encode(nil, []byte("verify")), nil)
	if err != nil {
		t.Fatalf("decoder from freelist broken after overflow: %v", err)
	}
	if string(out) != "verify" {
		t.Fatalf("round-trip after overflow = %q, want %q", out, "verify")
	}
	putDecoder(d)
}

func TestGetDecoder_FreelistReuse(t *testing.T) {
	src := []byte("decoder-reuse-test")
	compressed := encode(nil, src)

	d := getDecoder()
	out1, err := d.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatalf("first decode: %v", err)
	}
	putDecoder(d)

	d2 := getDecoder()
	out2, err := d2.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatalf("second decode: %v", err)
	}
	putDecoder(d2)

	if !bytes.Equal(out1, out2) {
		t.Errorf("reused decoder: %q != %q", out1, out2)
	}
}

func TestInit_WarmupPrePopulation(t *testing.T) {
	// Re-init with a small freelist and known warmup count.
	Init(&config.CompressConfig{
		FreelistSize:       8,
		MaxDecompressBytes: 256 << 20,
		WarmupCount:        3,
	})

	// Drain without blocking — should get at least WarmupCount decoders.
	var count int
	for {
		select {
		case <-decoderFree:
			count++
		default:
			goto done
		}
	}
done:
	if count != 3 {
		t.Errorf("warmup pre-populated %d decoders, want 3", count)
	}

	// Restore original config for remaining tests.
	Init(&config.CompressConfig{
		FreelistSize:       128,
		MaxDecompressBytes: 256 << 20,
		WarmupCount:        4,
	})
}

func BenchmarkDecompress(b *testing.B) {
	src := bytes.Repeat([]byte(`{"host":"srv1","severity":"INFO","msg":"benchmark test message payload"}`+"\n"), 100)
	compressed := encode(nil, src)
	var dst []byte
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		var err error
		dst, err = Decompress(dst, compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeWith(b *testing.B) {
	src := bytes.Repeat([]byte(`{"host":"srv1","severity":"INFO","msg":"benchmark test message payload"}`+"\n"), 100)
	enc := NewEncoder()
	var dst []byte
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		dst = EncodeWith(enc, dst, src)
	}
}
