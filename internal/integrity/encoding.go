package integrity

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"deinopis/internal/model"
)

const EncodingVersion uint16 = 1

type Digest [32]byte

type Entry struct {
	StreamID   Digest
	Slot       uint64
	AcceptSeq  uint64
	AcceptedAt int64
	RawID      string
	ReceivedAt int64
	Transport  string
	SourceIP   string
	SourcePort uint16
	Metadata   model.ReceiveMetadata
	Payload    []byte
}

type Coverage uint8

const (
	CoverageComplete Coverage = iota
	CoverageInterrupted
	CoverageGap
)

type Manifest struct {
	StreamID Digest
	Slot     uint64
	Start    uint64
	End      uint64
	Coverage Coverage
	Previous Digest
	Entries  []Entry
}

func EncodeEntry(e Entry) ([]byte, error) {
	if e.AcceptedAt < 0 || e.ReceivedAt < 0 || e.AcceptSeq == 0 || e.RawID == "" || !asciiBounded(e.RawID, 128) || !asciiBounded(e.Transport, 64) || !asciiOrEmpty(e.SourceIP, 256) {
		return nil, errors.New("invalid integrity entry identity or time")
	}
	var meta bytes.Buffer
	for _, s := range []string{e.Metadata.ConnectionID, e.Metadata.Listener, e.Metadata.RequestID, e.Metadata.FilePath, e.Metadata.FileID} {
		if err := putString(&meta, s, 4096); err != nil {
			return nil, err
		}
	}
	if e.Metadata.KafkaPartition < math.MinInt32 || e.Metadata.KafkaPartition > math.MaxInt32 {
		return nil, errors.New("kafka partition outside int32")
	}
	if err := binary.Write(&meta, binary.BigEndian, e.Metadata.FileOffset); err != nil {
		return nil, err
	}
	for _, s := range []string{e.Metadata.ObjectKey, e.Metadata.Watermark, e.Metadata.KafkaTopic} {
		if err := putString(&meta, s, 4096); err != nil {
			return nil, err
		}
	}
	if err := binary.Write(&meta, binary.BigEndian, int32(e.Metadata.KafkaPartition)); err != nil {
		return nil, err
	}
	if err := binary.Write(&meta, binary.BigEndian, e.Metadata.KafkaOffset); err != nil {
		return nil, err
	}
	if err := putString(&meta, e.Metadata.KafkaKey, 4096); err != nil {
		return nil, err
	}
	if meta.Len() > 16*1024 {
		return nil, errors.New("receive metadata exceeds 16 KiB")
	}

	var b bytes.Buffer
	b.WriteString("DEINOPIS-ENTRY-V1")
	b.Write(e.StreamID[:])
	_ = binary.Write(&b, binary.BigEndian, e.Slot)
	_ = binary.Write(&b, binary.BigEndian, e.AcceptSeq)
	_ = binary.Write(&b, binary.BigEndian, uint64(e.AcceptedAt))
	if err := putString(&b, e.RawID, 128); err != nil {
		return nil, err
	}
	_ = binary.Write(&b, binary.BigEndian, uint64(e.ReceivedAt))
	if err := putString(&b, e.Transport, 64); err != nil {
		return nil, err
	}
	if err := putString(&b, e.SourceIP, 256); err != nil {
		return nil, err
	}
	_ = binary.Write(&b, binary.BigEndian, e.SourcePort)
	b.Write(meta.Bytes())
	_ = binary.Write(&b, binary.BigEndian, uint64(len(e.Payload)))
	payloadHash := sha256.Sum256(e.Payload)
	b.Write(payloadHash[:])
	return b.Bytes(), nil
}

func EncodeManifest(m Manifest) ([]byte, error) {
	if m.End <= m.Start || m.Coverage > CoverageGap || len(m.Entries) > 100_000 || (m.Coverage == CoverageGap && len(m.Entries) != 0) {
		return nil, errors.New("invalid manifest bounds or coverage")
	}
	encoded := make([][]byte, len(m.Entries))
	leaves := make([]Digest, len(m.Entries))
	for i, e := range m.Entries {
		if e.StreamID != m.StreamID || e.Slot != m.Slot || (i > 0 && (m.Entries[i-1].AcceptSeq == math.MaxUint64 || e.AcceptSeq != m.Entries[i-1].AcceptSeq+1)) {
			return nil, fmt.Errorf("manifest entry %d has wrong stream, slot, or order", i)
		}
		v, err := EncodeEntry(e)
		if err != nil {
			return nil, fmt.Errorf("encode manifest entry %d: %w", i, err)
		}
		encoded[i] = v
		leaves[i] = leaf(v)
	}
	root := Root(leaves)
	var b bytes.Buffer
	b.WriteString("DEINOPIS-MANIFEST-V1")
	_ = binary.Write(&b, binary.BigEndian, EncodingVersion)
	b.Write(m.StreamID[:])
	_ = binary.Write(&b, binary.BigEndian, m.Slot)
	_ = binary.Write(&b, binary.BigEndian, m.Start)
	_ = binary.Write(&b, binary.BigEndian, m.End)
	b.WriteByte(byte(m.Coverage))
	_ = binary.Write(&b, binary.BigEndian, uint64(len(encoded)))
	b.Write(m.Previous[:])
	b.Write(root[:])
	for _, entry := range encoded {
		if len(entry) > math.MaxUint32 {
			return nil, errors.New("encoded entry too large")
		}
		_ = binary.Write(&b, binary.BigEndian, uint32(len(entry)))
		b.Write(entry)
		if b.Len() > 64*1024*1024 {
			return nil, errors.New("manifest exceeds 64 MiB")
		}
	}
	return b.Bytes(), nil
}

func ManifestDigest(encoded []byte) Digest { return sha256.Sum256(encoded) }

func WindowCommitment(m Manifest, manifestBytes []byte) (Digest, error) {
	expected, err := EncodeManifest(m)
	if err != nil {
		return Digest{}, err
	}
	if !bytes.Equal(expected, manifestBytes) {
		return Digest{}, errors.New("manifest bytes do not match the supplied manifest")
	}
	manifestDigest := ManifestDigest(manifestBytes)
	leaves := make([]Digest, len(m.Entries))
	for i, e := range m.Entries {
		b, err := EncodeEntry(e)
		if err != nil {
			return Digest{}, err
		}
		leaves[i] = leaf(b)
	}
	root := Root(leaves)
	var b bytes.Buffer
	b.WriteString("DEINOPIS-WINDOW-V1")
	_ = binary.Write(&b, binary.BigEndian, EncodingVersion)
	b.Write(m.StreamID[:])
	_ = binary.Write(&b, binary.BigEndian, m.Slot)
	_ = binary.Write(&b, binary.BigEndian, m.Start)
	_ = binary.Write(&b, binary.BigEndian, m.End)
	b.WriteByte(byte(m.Coverage))
	_ = binary.Write(&b, binary.BigEndian, uint64(len(m.Entries)))
	b.Write(m.Previous[:])
	b.Write(root[:])
	b.Write(manifestDigest[:])
	return sha256.Sum256(b.Bytes()), nil
}

func putString(b *bytes.Buffer, s string, max int) error {
	if !utf8.ValidString(s) || len(s) > max || uint64(len(s)) > math.MaxUint32 {
		return errors.New("invalid or oversized UTF-8 field")
	}
	_ = binary.Write(b, binary.BigEndian, uint32(len(s)))
	b.WriteString(s)
	return nil
}

func asciiBounded(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7f {
			return false
		}
	}
	return true
}

func asciiOrEmpty(s string, max int) bool {
	return s == "" || asciiBounded(s, max)
}
