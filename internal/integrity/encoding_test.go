package integrity

import (
	"encoding/hex"
	"fmt"
	"testing"

	"deinopis/internal/model"
)

func TestMerkleKnownVectors(t *testing.T) {
	empty := Root(nil)
	if got := hex.EncodeToString(empty[:]); got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatalf("empty root = %s", got)
	}
	abc := leaf([]byte("abc"))
	if got := hex.EncodeToString(abc[:]); got != "609f6e36d2405585188d5cfd761f407c7cc46a7d3f314c88270469dde315fcd1" {
		t.Fatalf("abc leaf vector mismatch: %s", got)
	}
}

func TestManifestAndProofBindOrderContextAndOccurrence(t *testing.T) {
	var stream Digest
	stream[31] = 1
	entries := []Entry{
		{StreamID: stream, Slot: 2, AcceptSeq: 1, AcceptedAt: 600_000_000_000, RawID: "deinopis-raw-a", ReceivedAt: 600_000_000_000, Transport: "http_post", SourceIP: "127.0.0.1", Payload: []byte("same")},
		{StreamID: stream, Slot: 2, AcceptSeq: 2, AcceptedAt: 600_000_000_001, RawID: "deinopis-raw-b", ReceivedAt: 600_000_000_001, Transport: "http_post", SourceIP: "127.0.0.1", Metadata: model.ReceiveMetadata{RequestID: "é"}, Payload: []byte("same")},
		{StreamID: stream, Slot: 2, AcceptSeq: 3, AcceptedAt: 600_000_000_002, RawID: "deinopis-raw-c", ReceivedAt: 600_000_000_002, Transport: "http_post", SourceIP: "127.0.0.1", Payload: []byte("third")},
	}
	m := Manifest{StreamID: stream, Slot: 2, Start: 600, End: 900, Coverage: CoverageComplete, Entries: entries}
	encoded, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseManifest(encoded)
	if err != nil || parsed.Count != 3 || parsed.StreamID != stream || parsed.Slot != 2 {
		t.Fatalf("parsed manifest = %+v, err=%v", parsed, err)
	}
	commitment, err := WindowCommitment(m, encoded)
	if err != nil {
		t.Fatal(err)
	}
	entryBytes := make([][]byte, len(entries))
	for i := range entries {
		entryBytes[i], err = EncodeEntry(entries[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	proof, root, err := BuildProof(entryBytes, 1, stream, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyProof(proof, stream, 2, root) {
		t.Fatal("valid proof rejected")
	}
	if VerifyProof(proof, stream, 3, root) {
		t.Fatal("proof accepted for another slot")
	}
	if VerifyProof(proof, Digest{}, 2, root) {
		t.Fatal("proof accepted for another stream")
	}
	if VerifyProof(proof, stream, 2, Digest{}) {
		t.Fatal("proof accepted for wrong root")
	}
	proof.Siblings = append(proof.Siblings, Digest{})
	if VerifyProof(proof, stream, 2, root) {
		t.Fatal("proof accepted extra siblings")
	}
	if _, err := WindowCommitment(m, append(encoded, 0)); err == nil {
		t.Fatal("commitment accepted mutated manifest bytes")
	}
	mutated := append([]byte(nil), encoded...)
	mutated[len(mutated)-1] ^= 1
	if _, err := ParseManifest(mutated); err == nil {
		t.Fatal("manifest accepted modified member bytes")
	}
	if commitment == (Digest{}) {
		t.Fatal("empty commitment")
	}
	if string(encoded[:len("DEINOPIS-MANIFEST-V1")]) != "DEINOPIS-MANIFEST-V1" {
		t.Fatal("manifest domain missing")
	}
}

func TestEntryAndManifestRejectInvalidMetadataAndOrder(t *testing.T) {
	var stream Digest
	stream[0] = 1
	entry := Entry{StreamID: stream, Slot: 0, AcceptSeq: 1, AcceptedAt: 1, RawID: "deinopis-raw-1", ReceivedAt: 1, Transport: "stdin", SourceIP: "local", Metadata: model.ReceiveMetadata{RequestID: string([]byte{0xff})}}
	if _, err := EncodeEntry(entry); err == nil {
		t.Fatal("invalid UTF-8 metadata accepted")
	}
	entry.Metadata = model.ReceiveMetadata{}
	m := Manifest{StreamID: stream, Slot: 0, Start: 0, End: 300, Entries: []Entry{entry, entry}}
	if _, err := EncodeManifest(m); err == nil {
		t.Fatal("duplicate acceptance sequence accepted")
	}
}

func TestCountBatchV2ManifestIsDeterministicAndSeparateFromV1(t *testing.T) {
	stream := Digest{1, 2, 3}
	entries := []BatchEntry{
		{Entry: Entry{StreamID: stream, AcceptSeq: 1, AcceptedAt: 1_000, RawID: "deinopis-raw-a", ReceivedAt: 900, Transport: "stdin", Payload: []byte("same")}, BatchNo: 7},
		{Entry: Entry{StreamID: stream, AcceptSeq: 2, AcceptedAt: 1_100, RawID: "deinopis-raw-b", ReceivedAt: 950, Transport: "stdin", Payload: []byte("same")}, BatchNo: 7},
		{Entry: Entry{StreamID: stream, AcceptSeq: 3, AcceptedAt: 1_200, RawID: "deinopis-raw-c", ReceivedAt: 1_000, Transport: "stdin", Payload: []byte("third")}, BatchNo: 7},
	}
	m := BatchManifest{StreamID: stream, BatchNo: 7, FirstSequence: 1, LastSequence: 3, StartedAt: 1_000, ClosedAt: 1_500, Entries: entries}
	encoded, err := EncodeBatchManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseBatchManifest(encoded)
	if err != nil || parsed.Count != 3 || parsed.BatchNo != 7 || parsed.FirstSequence != 1 || parsed.LastSequence != 3 {
		t.Fatalf("parsed v2 batch = %+v, err=%v", parsed, err)
	}
	commitment, err := BatchCommitment(m, encoded)
	if err != nil || commitment != parsed.Commitment {
		t.Fatalf("commitment mismatch: %x != %x (err=%v)", commitment, parsed.Commitment, err)
	}
	if _, err = ParseManifest(encoded); err == nil {
		t.Fatal("v1 parser accepted a v2 batch")
	}
	mutated := append([]byte(nil), encoded...)
	mutated[len(mutated)-1] ^= 1
	if _, err = ParseBatchManifest(mutated); err == nil {
		t.Fatal("v2 parser accepted a changed entry")
	}
	entries[2].AcceptSeq = 4
	if _, err = EncodeBatchManifest(m); err == nil {
		t.Fatal("v2 encoder accepted a noncontiguous sequence")
	}
}

func TestStreamingMerkleAccumulatorMatchesCanonicalRoot(t *testing.T) {
	for size := 0; size <= 257; size++ {
		entries := make([][]byte, size)
		leaves := make([]Digest, size)
		var accumulator MerkleAccumulator
		for i := range entries {
			entries[i] = []byte(fmt.Sprintf("entry-%d", i))
			leaves[i] = leaf(entries[i])
			if err := accumulator.AddEntry(entries[i]); err != nil {
				t.Fatal(err)
			}
		}
		if got, want := accumulator.Root(), Root(leaves); got != want {
			t.Fatalf("size %d: streaming root %x != canonical %x", size, got, want)
		}
		if accumulator.Count() != uint64(size) {
			t.Fatalf("size %d: accumulator count %d", size, accumulator.Count())
		}
	}
}

func TestVerifyProofRejectsHostileBoundsAndNoncanonicalEntries(t *testing.T) {
	stream := Digest{1}
	entry := Entry{StreamID: stream, Slot: 2, AcceptSeq: 1, AcceptedAt: 600_000_000_000, RawID: "deinopis-raw-a", ReceivedAt: 600_000_000_000, Transport: "stdin", SourceIP: "local"}
	encoded, err := EncodeEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	proof, root, err := BuildProof([][]byte{encoded}, 0, stream, 2)
	if err != nil || !VerifyProof(proof, stream, 2, root) {
		t.Fatalf("valid proof rejected: %v", err)
	}
	proof.Size = ^uint64(0)
	if VerifyProof(proof, stream, 2, root) {
		t.Fatal("accepted hostile tree size")
	}
	proof.Size = 1
	proof.Entry = append(append([]byte(nil), encoded...), 0)
	if VerifyProof(proof, stream, 2, root) {
		t.Fatal("accepted noncanonical entry bytes")
	}
}
