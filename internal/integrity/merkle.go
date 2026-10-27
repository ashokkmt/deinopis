package integrity

import (
	"crypto/sha256"
	"errors"
)

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
	if index < 0 || index >= len(entries) {
		return Proof{}, Digest{}, errors.New("proof index out of range")
	}
	leaves := make([]Digest, len(entries))
	for i, e := range entries {
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
	if p.Version != EncodingVersion || p.Stream != expectedStream || p.Slot != expectedSlot || p.Size == 0 || p.Index >= p.Size || len(p.Entry) < len("DEINOPIS-ENTRY-V1")+32+8 {
		return false
	}
	if string(p.Entry[:len("DEINOPIS-ENTRY-V1")]) != "DEINOPIS-ENTRY-V1" {
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
