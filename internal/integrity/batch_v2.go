package integrity

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const BatchEncodingVersion uint16 = 2

// BatchEntry preserves the accepted raw parent fields while binding it to a
// sequential v2 batch instead of a v1 UTC slot.
type BatchEntry struct {
	Entry
	BatchNo uint64
}

type BatchManifest struct {
	StreamID      Digest
	BatchNo       uint64
	FirstSequence uint64
	LastSequence  uint64
	StartedAt     int64
	ClosedAt      int64
	Previous      Digest
	Entries       []BatchEntry
}

type ParsedBatchManifest struct {
	StreamID      Digest
	BatchNo       uint64
	FirstSequence uint64
	LastSequence  uint64
	StartedAt     int64
	ClosedAt      int64
	Count         uint64
	Previous      Digest
	Root          Digest
	Digest        Digest
	Commitment    Digest
	Entries       [][]byte
}

func EncodeBatchEntry(e BatchEntry) ([]byte, error) {
	entry := e.Entry
	entry.Slot = e.BatchNo
	return encodeEntry(entry, "DEINOPIS-ENTRY-V2")
}

func EncodeBatchManifest(m BatchManifest) ([]byte, error) {
	count := len(m.Entries)
	if count == 0 || count > 50_000 || m.FirstSequence == 0 || m.LastSequence < m.FirstSequence ||
		uint64(m.LastSequence)-uint64(m.FirstSequence)+1 != uint64(count) || m.StartedAt < 0 || m.ClosedAt < m.StartedAt {
		return nil, errors.New("invalid count-batch bounds")
	}
	encoded := make([][]byte, count)
	leaves := make([]Digest, count)
	for i, e := range m.Entries {
		if e.StreamID != m.StreamID || e.BatchNo != m.BatchNo || e.AcceptSeq != m.FirstSequence+uint64(i) ||
			e.AcceptedAt < m.StartedAt || e.AcceptedAt > m.ClosedAt {
			return nil, fmt.Errorf("batch entry %d has wrong identity, order, or time bounds", i)
		}
		b, err := EncodeBatchEntry(e)
		if err != nil {
			return nil, fmt.Errorf("encode batch entry %d: %w", i, err)
		}
		encoded[i], leaves[i] = b, leaf(b)
	}
	root := Root(leaves)
	var b bytes.Buffer
	b.WriteString("DEINOPIS-BATCH-MANIFEST-V2")
	_ = binary.Write(&b, binary.BigEndian, BatchEncodingVersion)
	b.Write(m.StreamID[:])
	for _, value := range []uint64{m.BatchNo, m.FirstSequence, m.LastSequence, uint64(count), uint64(m.StartedAt), uint64(m.ClosedAt)} {
		_ = binary.Write(&b, binary.BigEndian, value)
	}
	b.Write(m.Previous[:])
	b.Write(root[:])
	for _, entry := range encoded {
		_ = binary.Write(&b, binary.BigEndian, uint32(len(entry)))
		b.Write(entry)
		if b.Len() > 64*1024*1024 {
			return nil, errors.New("batch manifest exceeds 64 MiB")
		}
	}
	return b.Bytes(), nil
}

// BatchManifestPrefix writes the versioned header so batch entries can be
// streamed into a bounded buffer while their Merkle root is accumulated.
func BatchManifestPrefix(stream Digest, batchNo, first, last, count uint64, startedAt, closedAt int64, previous, root Digest) ([]byte, error) {
	if count == 0 || count > 50_000 || first == 0 || last < first || last-first+1 != count || startedAt < 0 || closedAt < startedAt {
		return nil, errors.New("invalid count-batch bounds")
	}
	var b bytes.Buffer
	b.WriteString("DEINOPIS-BATCH-MANIFEST-V2")
	_ = binary.Write(&b, binary.BigEndian, BatchEncodingVersion)
	b.Write(stream[:])
	for _, value := range []uint64{batchNo, first, last, count, uint64(startedAt), uint64(closedAt)} {
		_ = binary.Write(&b, binary.BigEndian, value)
	}
	b.Write(previous[:])
	b.Write(root[:])
	return b.Bytes(), nil
}

func BatchCommitmentFields(stream Digest, batchNo, first, last, count uint64, startedAt, closedAt int64, previous, root, manifestDigest Digest) (Digest, error) {
	if count == 0 || count > 50_000 || first == 0 || last < first || last-first+1 != count || startedAt < 0 || closedAt < startedAt {
		return Digest{}, errors.New("invalid batch commitment bounds")
	}
	var b bytes.Buffer
	b.WriteString("DEINOPIS-BATCH-COMMITMENT-V2")
	_ = binary.Write(&b, binary.BigEndian, BatchEncodingVersion)
	b.Write(stream[:])
	for _, value := range []uint64{batchNo, first, last, count, uint64(startedAt), uint64(closedAt)} {
		_ = binary.Write(&b, binary.BigEndian, value)
	}
	b.Write(previous[:])
	b.Write(root[:])
	b.Write(manifestDigest[:])
	return sha256.Sum256(b.Bytes()), nil
}

func ParseBatchManifest(data []byte) (ParsedBatchManifest, error) {
	var m ParsedBatchManifest
	if len(data) == 0 || len(data) > 64*1024*1024 {
		return m, errors.New("batch manifest is empty or exceeds 64 MiB")
	}
	r := bytes.NewReader(data)
	domain := make([]byte, len("DEINOPIS-BATCH-MANIFEST-V2"))
	if _, err := io.ReadFull(r, domain); err != nil || string(domain) != "DEINOPIS-BATCH-MANIFEST-V2" {
		return m, errors.New("invalid batch manifest domain")
	}
	var version uint16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil || version != BatchEncodingVersion {
		return m, errors.New("unsupported batch manifest version")
	}
	if _, err := io.ReadFull(r, m.StreamID[:]); err != nil {
		return m, err
	}
	values := []*uint64{&m.BatchNo, &m.FirstSequence, &m.LastSequence, &m.Count}
	var started, closed uint64
	values = append(values, &started, &closed)
	for _, value := range values {
		if err := binary.Read(r, binary.BigEndian, value); err != nil {
			return m, err
		}
	}
	if started > math.MaxInt64 || closed > math.MaxInt64 {
		return m, errors.New("batch time exceeds supported range")
	}
	m.StartedAt, m.ClosedAt = int64(started), int64(closed)
	if _, err := io.ReadFull(r, m.Previous[:]); err != nil {
		return m, err
	}
	if _, err := io.ReadFull(r, m.Root[:]); err != nil {
		return m, err
	}
	if m.Count == 0 || m.Count > 50_000 || m.FirstSequence == 0 || m.LastSequence < m.FirstSequence ||
		m.LastSequence-m.FirstSequence == math.MaxUint64 || m.LastSequence-m.FirstSequence+1 != m.Count || m.ClosedAt < m.StartedAt ||
		m.Count > uint64(r.Len()/4) {
		return m, errors.New("invalid batch manifest bounds")
	}
	m.Entries = make([][]byte, 0, m.Count)
	leaves := make([]Digest, 0, m.Count)
	for i := uint64(0); i < m.Count; i++ {
		var size uint32
		if err := binary.Read(r, binary.BigEndian, &size); err != nil || size == 0 || uint64(size) > uint64(r.Len()) {
			return m, errors.New("invalid batch entry length")
		}
		entry := make([]byte, size)
		if _, err := io.ReadFull(r, entry); err != nil {
			return m, err
		}
		legacyDomain := []byte("DEINOPIS-ENTRY-V1")
		if len(entry) < len(legacyDomain) || string(entry[:len(legacyDomain)]) != "DEINOPIS-ENTRY-V2" {
			return m, errors.New("invalid batch entry domain")
		}
		validation := append([]byte(nil), entry...)
		copy(validation, legacyDomain)
		seq, accepted, _, err := validateEntryBytes(validation, m.StreamID, m.BatchNo, 0, math.MaxUint64)
		if err != nil || seq != m.FirstSequence+i || accepted < m.StartedAt || accepted > m.ClosedAt {
			return m, fmt.Errorf("batch entry %d failed identity, sequence, or time validation", i)
		}
		m.Entries = append(m.Entries, entry)
		leaves = append(leaves, leaf(entry))
	}
	if r.Len() != 0 {
		return m, errors.New("batch manifest has trailing bytes")
	}
	if Root(leaves) != m.Root {
		return m, errors.New("batch manifest Merkle root mismatch")
	}
	m.Digest = sha256.Sum256(data)
	m.Commitment = batchCommitment(m)
	return m, nil
}

func BatchCommitment(m BatchManifest, manifest []byte) (Digest, error) {
	expected, err := EncodeBatchManifest(m)
	if err != nil {
		return Digest{}, err
	}
	if !bytes.Equal(expected, manifest) {
		return Digest{}, errors.New("batch manifest bytes do not match supplied manifest")
	}
	parsed, err := ParseBatchManifest(manifest)
	if err != nil {
		return Digest{}, err
	}
	return parsed.Commitment, nil
}

func batchCommitment(m ParsedBatchManifest) Digest {
	commitment, _ := BatchCommitmentFields(m.StreamID, m.BatchNo, m.FirstSequence, m.LastSequence, m.Count, m.StartedAt, m.ClosedAt, m.Previous, m.Root, m.Digest)
	return commitment
}
