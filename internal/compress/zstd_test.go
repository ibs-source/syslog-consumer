package compress

import (
	"bytes"
	"sync"
	"testing"
)

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
	const goroutines = 32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			var buf []byte
			for range 100 {
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
