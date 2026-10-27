package integrity

import (
	"encoding/hex"
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
