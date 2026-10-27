package integrity

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

type ParsedManifest struct {
	StreamID          Digest
	Slot              uint64
	Start             uint64
	End               uint64
	Coverage          Coverage
	Count             uint64
	FirstSequence     uint64
	HighWaterSequence uint64
	Previous          Digest
	Root              Digest
	Digest            Digest
	Commitment        Digest
	Entries           [][]byte
}

func ParseManifest(data []byte) (ParsedManifest, error) {
	var m ParsedManifest
	if len(data) > 64*1024*1024 {
		return m, errors.New("manifest exceeds 64 MiB")
	}
	r := bytes.NewReader(data)
	domain := make([]byte, len("DEINOPIS-MANIFEST-V1"))
	if _, err := io.ReadFull(r, domain); err != nil || string(domain) != "DEINOPIS-MANIFEST-V1" {
		return m, errors.New("invalid manifest domain")
	}
	var version uint16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil || version != EncodingVersion {
		return m, errors.New("unsupported manifest encoding version")
	}
	if _, err := io.ReadFull(r, m.StreamID[:]); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Slot); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.Start); err != nil {
		return m, err
	}
	if err := binary.Read(r, binary.BigEndian, &m.End); err != nil {
		return m, err
	}
	coverage, err := r.ReadByte()
	if err != nil {
		return m, err
	}
	m.Coverage = Coverage(coverage)
	if err := binary.Read(r, binary.BigEndian, &m.Count); err != nil {
		return m, err
	}
	if _, err := io.ReadFull(r, m.Previous[:]); err != nil {
		return m, err
	}
	if _, err := io.ReadFull(r, m.Root[:]); err != nil {
		return m, err
	}
	if m.End <= m.Start || m.Coverage > CoverageGap || m.Count > 100_000 || (m.Coverage == CoverageGap && m.Count != 0) {
		return m, errors.New("invalid manifest bounds")
	}
	if m.Count > uint64(r.Len()/4) {
		return m, errors.New("manifest count exceeds remaining bytes")
	}
	m.Entries = make([][]byte, 0, m.Count)
	leaves := make([]Digest, 0, m.Count)
	seen := make(map[string]struct{}, m.Count)
	var prevSeq uint64
	for i := uint64(0); i < m.Count; i++ {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return m, err
		}
		if n == 0 || uint64(n) > uint64(r.Len()) {
			return m, errors.New("invalid manifest entry length")
		}
		entry := make([]byte, n)
		if _, err := io.ReadFull(r, entry); err != nil {
			return m, err
		}
		seq, accepted, rawID, err := validateEntryBytes(entry, m.StreamID, m.Slot, m.Start, m.End)
		if err != nil {
			return m, fmt.Errorf("manifest entry %d: %w", i, err)
		}
		if (i > 0 && (prevSeq == math.MaxUint64 || seq != prevSeq+1)) || (i == 0 && seq == 0) {
			return m, errors.New("manifest acceptance sequence is not contiguous")
		}
		if _, ok := seen[rawID]; ok {
			return m, errors.New("manifest repeats a raw parent")
		}
		seen[rawID] = struct{}{}
		prevSeq = seq
		if i == 0 {
			m.FirstSequence = seq
		}
		m.HighWaterSequence = seq
		_ = accepted
		m.Entries = append(m.Entries, entry)
		leaves = append(leaves, leaf(entry))
	}
	if r.Len() != 0 {
		return m, errors.New("manifest has trailing bytes")
	}
	computedRoot := Root(leaves)
	if computedRoot != m.Root {
		return m, errors.New("manifest Merkle root mismatch")
	}
	m.Digest = sha256.Sum256(data)
	m.Commitment = commitmentFromFields(m.StreamID, m.Slot, m.Start, m.End, m.Coverage, m.Count, m.Previous, m.Root, m.Digest)
	return m, nil
}

func validateEntryBytes(data []byte, stream Digest, slot, start, end uint64) (uint64, int64, string, error) {
	r := bytes.NewReader(data)
	domain := make([]byte, len("DEINOPIS-ENTRY-V1"))
	if _, err := io.ReadFull(r, domain); err != nil || string(domain) != "DEINOPIS-ENTRY-V1" {
		return 0, 0, "", errors.New("invalid entry domain")
	}
	var gotStream Digest
	if _, err := io.ReadFull(r, gotStream[:]); err != nil {
		return 0, 0, "", err
	}
	var gotSlot, seq, accepted uint64
	for _, v := range []*uint64{&gotSlot, &seq, &accepted} {
		if err := binary.Read(r, binary.BigEndian, v); err != nil {
			return 0, 0, "", err
		}
	}
	if gotStream != stream || gotSlot != slot || seq == 0 || accepted > math.MaxInt64 || accepted/1_000_000_000 < start || accepted/1_000_000_000 >= end {
		return 0, 0, "", errors.New("entry stream, slot, or acceptance time mismatch")
	}
	rawID, err := readString(r, 128)
	if err != nil || !asciiBounded(rawID, 128) {
		return 0, 0, "", errors.New("invalid raw ID")
	}
	var received uint64
	if err = binary.Read(r, binary.BigEndian, &received); err != nil || received > math.MaxInt64 {
		return 0, 0, "", errors.New("invalid receive time")
	}
	transport, err := readString(r, 64)
	if err != nil || !asciiBounded(transport, 64) {
		return 0, 0, "", errors.New("invalid transport")
	}
	sourceIP, err := readString(r, 256)
	if err != nil || !asciiOrEmpty(sourceIP, 256) {
		return 0, 0, "", errors.New("invalid source identity")
	}
	var port uint16
	if err = binary.Read(r, binary.BigEndian, &port); err != nil {
		return 0, 0, "", err
	}
	metadataStart := r.Len()
	for i := 0; i < 5; i++ {
		if _, err = readString(r, 4096); err != nil {
			return 0, 0, "", err
		}
	}
	var offset int64
	if err = binary.Read(r, binary.BigEndian, &offset); err != nil {
		return 0, 0, "", err
	}
	for i := 0; i < 3; i++ {
		if _, err = readString(r, 4096); err != nil {
			return 0, 0, "", err
		}
	}
	var partition int32
	if err = binary.Read(r, binary.BigEndian, &partition); err != nil {
		return 0, 0, "", err
	}
	var kafkaOffset int64
	if err = binary.Read(r, binary.BigEndian, &kafkaOffset); err != nil {
		return 0, 0, "", err
	}
	if _, err = readString(r, 4096); err != nil {
		return 0, 0, "", err
	}
	if metadataStart-r.Len() > 16*1024 {
		return 0, 0, "", errors.New("metadata exceeds 16 KiB")
	}
	var payloadLen uint64
	if err = binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
		return 0, 0, "", err
	}
	var payloadDigest Digest
	if _, err = io.ReadFull(r, payloadDigest[:]); err != nil {
		return 0, 0, "", err
	}
	if r.Len() != 0 {
		return 0, 0, "", errors.New("entry has trailing bytes")
	}
	_ = port
	_ = offset
	_ = partition
	_ = kafkaOffset
	_ = payloadLen
	_ = payloadDigest
	return seq, int64(accepted), rawID, nil
}

func readString(r *bytes.Reader, max uint32) (string, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", err
	}
	if n > max || uint64(n) > uint64(r.Len()) {
		return "", errors.New("invalid string length")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", errors.New("invalid UTF-8 string")
	}
	return string(b), nil
}

func commitmentFromFields(stream Digest, slot, start, end uint64, coverage Coverage, count uint64, previous, root, digest Digest) Digest {
	var b bytes.Buffer
	b.WriteString("DEINOPIS-WINDOW-V1")
	_ = binary.Write(&b, binary.BigEndian, EncodingVersion)
	b.Write(stream[:])
	_ = binary.Write(&b, binary.BigEndian, slot)
	_ = binary.Write(&b, binary.BigEndian, start)
	_ = binary.Write(&b, binary.BigEndian, end)
	b.WriteByte(byte(coverage))
	_ = binary.Write(&b, binary.BigEndian, count)
	b.Write(previous[:])
	b.Write(root[:])
	b.Write(digest[:])
	return sha256.Sum256(b.Bytes())
}
