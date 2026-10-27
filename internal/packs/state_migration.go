package packs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	legacyStateDir  = ".marketplace"
	neutralStateDir = ".pack-state"
	migrationFile   = "migration.json"
)

var parserStateFiles = []string{"manifest.json", "rollback.json", "transaction.json"}

type stateMigration struct {
	Version  int               `json:"version"`
	Files    map[string]string `json:"files"`
	Complete bool              `json:"complete"`
}

// migrateLegacyState runs while both the legacy and neutral directory locks are held.
func migrateLegacyState(dir string) error {
	legacy := filepath.Join(dir, legacyStateDir)
	neutral := filepath.Join(dir, neutralStateDir)
	if err := RejectSymlinks(dir, legacy); err != nil {
		return err
	}
	if err := RejectSymlinks(dir, neutral); err != nil {
		return err
	}
	if _, err := os.Stat(legacy); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	sources := make(map[string][]byte)
	for _, name := range parserStateFiles {
		b, err := os.ReadFile(filepath.Join(legacy, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err = validateLegacyState(dir, name, b); err != nil {
			return fmt.Errorf("invalid legacy parser state %s: %w", name, err)
		}
		sources[name] = b
	}
	if len(sources) == 0 {
		return nil
	}

	markerPath := filepath.Join(neutral, migrationFile)
	markerData, err := os.ReadFile(markerPath)
	markerExists := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var marker stateMigration
	if markerExists {
		if err = json.Unmarshal(markerData, &marker); err != nil || marker.Version != 1 {
			return errors.New("invalid parser state migration marker")
		}
	} else {
		for _, name := range parserStateFiles {
			if _, err = os.Stat(filepath.Join(neutral, name)); err == nil {
				return fmt.Errorf("legacy and neutral parser state conflict at %s", name)
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		marker = stateMigration{Version: 1, Files: map[string]string{}}
		for name, b := range sources {
			sum := sha256.Sum256(b)
			marker.Files[name] = hex.EncodeToString(sum[:])
		}
		if err = writeMigrationMarker(markerPath, marker); err != nil {
			return err
		}
	}
	for name, b := range sources {
		sum := sha256.Sum256(b)
		if marker.Files[name] != hex.EncodeToString(sum[:]) {
			return fmt.Errorf("legacy parser state changed during migration: %s", name)
		}
	}
	if !marker.Complete {
		for name, b := range sources {
			if err = atomicWrite(filepath.Join(neutral, name), b, 0600); err != nil {
				return err
			}
		}
		if err = syncDirectory(neutral); err != nil {
			return err
		}
		marker.Complete = true
		if err = writeMigrationMarker(markerPath, marker); err != nil {
			return err
		}
	}
	return RecoverTransactions(dir)
}

func writeMigrationMarker(path string, marker stateMigration) error {
	b, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	if err = atomicWrite(path, b, 0600); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func validateLegacyState(dir, name string, data []byte) error {
	switch name {
	case "manifest.json":
		var m PackManifest
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		if m.Version != 1 {
			return errors.New("unsupported parser pack manifest version")
		}
		for key, entry := range m.Entries {
			if entry.Name != key {
				return fmt.Errorf("manifest entry identity mismatch for %s", key)
			}
			if err := validateRecord(rollbackRecord{Filename: entry.Filename}); err != nil {
				return err
			}
			content, err := os.ReadFile(filepath.Join(dir, entry.Filename))
			if err != nil {
				return fmt.Errorf("read active parser %s: %w", entry.Filename, err)
			}
			if digestBytes(content) != entry.Digest {
				return fmt.Errorf("active parser %s conflicts with its manifest digest", entry.Filename)
			}
			pack, err := LoadPack(bytes.NewReader(content))
			if err != nil {
				return err
			}
			if pack.Metadata.Name != entry.Name || pack.Metadata.Version != entry.Version {
				return fmt.Errorf("active parser %s identity/version conflicts with its manifest", entry.Filename)
			}
		}
		for _, revisions := range m.History {
			for _, revision := range revisions {
				if err := validateRecord(rollbackRecord{Filename: revision.Filename}); err != nil {
					return err
				}
				if revision.Existed && len(revision.Content) == 0 {
					return errors.New("parser history is missing rollback bytes")
				}
			}
		}
	case "rollback.json":
		var r rollbackRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return err
		}
		return validateRecord(r)
	case "transaction.json":
		var r transactionRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return err
		}
		if err := validateRecord(r.Before); err != nil {
			return err
		}
		if err := validateRecord(r.Undo); err != nil {
			return err
		}
	}
	return nil
}
