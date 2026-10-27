package integrity

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
)

// MerkleAccumulator computes the project's canonical largest-power split root
// without retaining all leaves. Add entries in their committed order.
type MerkleAccumulator struct {
	count uint64
	stack [64]*Digest
}

func (a *MerkleAccumulator) AddEntry(encoded []byte) error {
	if a.count == ^uint64(0) {
		return errors.New("merkle leaf count exhausted")
	}
	carry := leaf(encoded)
	level := 0
	for a.count&(uint64(1)<<level) != 0 {
		if a.stack[level] == nil {
			return errors.New("corrupt merkle accumulator")
		}
		carry = node(*a.stack[level], carry)
		a.stack[level] = nil
		level++
	}
	a.stack[level] = &carry
	a.count++
	return nil
}

func (a *MerkleAccumulator) Root() Digest {
	if a.count == 0 {
		return sha256.Sum256(nil)
	}
	var root Digest
	set := false
	for level := 0; level < len(a.stack); level++ {
		if a.stack[level] == nil {
			continue
		}
		if !set {
			root, set = *a.stack[level], true
			continue
		}
		root = node(*a.stack[level], root)
	}
	return root
}

func (a *MerkleAccumulator) Count() uint64 { return a.count }

// ManifestPrefix returns the fixed version-1 manifest header. Callers append
// each canonical uint32-length-prefixed entry after computing the root.
func ManifestPrefix(stream Digest, slot, start, end uint64, coverage Coverage, count uint64, previous, root Digest) ([]byte, error) {
	if end <= start || coverage > CoverageGap || (coverage == CoverageGap && count != 0) || count > 100_000 {
		return nil, errors.New("invalid manifest bounds or coverage")
	}
	b := make([]byte, 0, len("DEINOPIS-MANIFEST-V1")+2+32+8*3+1+8+64)
	b = append(b, []byte("DEINOPIS-MANIFEST-V1")...)
	var v2 [2]byte
	binary.BigEndian.PutUint16(v2[:], EncodingVersion)
	b = append(b, v2[:]...)
	b = append(b, stream[:]...)
	var v8 [8]byte
	for _, v := range []uint64{slot, start, end} {
		binary.BigEndian.PutUint64(v8[:], v)
		b = append(b, v8[:]...)
	}
	b = append(b, byte(coverage))
	binary.BigEndian.PutUint64(v8[:], count)
	b = append(b, v8[:]...)
	b = append(b, previous[:]...)
	b = append(b, root[:]...)
	return b, nil
}

func WindowCommitmentFields(stream Digest, slot, start, end uint64, coverage Coverage, count uint64, previous, root, manifestDigest Digest) (Digest, error) {
	if end <= start || coverage > CoverageGap || (coverage == CoverageGap && count != 0) || count > 100_000 {
		return Digest{}, errors.New("invalid window commitment bounds")
	}
	b := make([]byte, 0, len("DEINOPIS-WINDOW-V1")+2+32+8*3+1+8+32*3)
	b = append(b, []byte("DEINOPIS-WINDOW-V1")...)
	var v2 [2]byte
	binary.BigEndian.PutUint16(v2[:], EncodingVersion)
	b = append(b, v2[:]...)
	b = append(b, stream[:]...)
	var v8 [8]byte
	for _, v := range []uint64{slot, start, end} {
		binary.BigEndian.PutUint64(v8[:], v)
		b = append(b, v8[:]...)
	}
	b = append(b, byte(coverage))
	binary.BigEndian.PutUint64(v8[:], count)
	b = append(b, v8[:]...)
	b = append(b, previous[:]...)
	b = append(b, root[:]...)
	b = append(b, manifestDigest[:]...)
	return sha256.Sum256(b), nil
}

func MerkleRootEntries(entries []Entry) (Digest, error) {
	leaves := make([]Digest, len(entries))
	for i, entry := range entries {
		encoded, err := EncodeEntry(entry)
		if err != nil {
			return Digest{}, err
		}
		leaves[i] = leaf(encoded)
	}
	return Root(leaves), nil
}

type Proof struct {
	Version  uint16
	Stream   Digest
	Slot     uint64
	Index    uint64
	Size     uint64
	Entry    []byte
	Siblings []Digest
}

func Root(leaves []Digest) Digest {
	if len(leaves) == 0 {
		return sha256.Sum256(nil)
	}
	if len(leaves) == 1 {
		return leaves[0]
	}
	split := largestPowerBelow(len(leaves))
	return node(Root(leaves[:split]), Root(leaves[split:]))
}

func BuildProof(entries [][]byte, index int, stream Digest, slot uint64) (Proof, Digest, error) {
	if len(entries) > 100_000 || index < 0 || index >= len(entries) {
		return Proof{}, Digest{}, errors.New("proof index out of range")
	}
	total := 0
	leaves := make([]Digest, len(entries))
	for i, e := range entries {
		total += 4 + len(e)
		if len(e) > 32*1024 || total > 64*1024*1024 {
			return Proof{}, Digest{}, errors.New("proof input exceeds integrity bounds")
		}
		leaves[i] = leaf(e)
	}
	siblings := make([]Digest, 0)
	var path func([]Digest, int) Digest
	path = func(part []Digest, at int) Digest {
		if len(part) == 1 {
			return part[0]
		}
		split := largestPowerBelow(len(part))
		if at < split {
			left := path(part[:split], at)
			siblings = append(siblings, Root(part[split:]))
			return node(left, siblings[len(siblings)-1])
		}
		right := path(part[split:], at-split)
		siblings = append(siblings, Root(part[:split]))
		return node(siblings[len(siblings)-1], right)
	}
	r := path(leaves, index)
	return Proof{Version: EncodingVersion, Stream: stream, Slot: slot, Index: uint64(index), Size: uint64(len(entries)), Entry: append([]byte(nil), entries[index]...), Siblings: siblings}, r, nil
}

func VerifyProof(p Proof, expectedStream Digest, expectedSlot uint64, expectedRoot Digest) bool {
	if p.Version != EncodingVersion || p.Stream != expectedStream || p.Slot != expectedSlot || p.Size == 0 || p.Size > 100_000 || p.Index >= p.Size || len(p.Entry) < len("DEINOPIS-ENTRY-V1")+32+8 || len(p.Entry) > 32*1024 || len(p.Siblings) > 17 {
		return false
	}
	if _, _, _, err := validateEntryBytes(p.Entry, expectedStream, expectedSlot, 0, math.MaxUint64); err != nil {
		return false
	}
	offset := len("DEINOPIS-ENTRY-V1")
	var embedded Digest
	copy(embedded[:], p.Entry[offset:offset+32])
	if embedded != expectedStream {
		return false
	}
	offset += 32
	var slot uint64
	for _, v := range p.Entry[offset : offset+8] {
		slot = slot<<8 | uint64(v)
	}
	if slot != expectedSlot {
		return false
	}
	leafHash := leaf(p.Entry)
	proofIndex := 0
	var reconstruct func(uint64, uint64) (Digest, bool)
	reconstruct = func(size, index uint64) (Digest, bool) {
		if size == 1 {
			return leafHash, true
		}
		split := uint64(largestPowerBelow(int(size)))
		if index < split {
			child, ok := reconstruct(split, index)
			if !ok || proofIndex >= len(p.Siblings) {
				return Digest{}, false
			}
			sibling := p.Siblings[proofIndex]
			proofIndex++
			return node(child, sibling), true
		}
		child, ok := reconstruct(size-split, index-split)
		if !ok || proofIndex >= len(p.Siblings) {
			return Digest{}, false
		}
		sibling := p.Siblings[proofIndex]
		proofIndex++
		return node(sibling, child), true
	}
	root, ok := reconstruct(p.Size, p.Index)
	return ok && proofIndex == len(p.Siblings) && root == expectedRoot
}

func leaf(data []byte) Digest {
	b := make([]byte, 1+len(data))
	copy(b[1:], data)
	return sha256.Sum256(b)
}

func node(left, right Digest) Digest {
	b := make([]byte, 1+len(left)+len(right))
	b[0] = 1
	copy(b[1:], left[:])
	copy(b[1+len(left):], right[:])
	return sha256.Sum256(b)
}

func largestPowerBelow(n int) int {
	p := 1
	for p<<1 < n {
		p <<= 1
	}
	return p
}
