package bitcask

import (
	"encoding/gob"
	"os"
)

// key → (file_id, offset, size)
type KeyDirEntry struct {
	FileID int
	Offset int64
	Size   int64
}

type KeyDir map[string]KeyDirEntry

func (kd *KeyDir) encode(filepath string) error {
	// Encode the keyDir to a file in the same format as the hint file
	// The hint file format is:
	// ┌──────────┬──────────┬──────────┬───────────┬────────┬─────────────┐
	// │  KeyLen  │ FileID   │ Offset   │ Size      │ Key    │
	// │ 4 bytes  │ 4 bytes  │ 8 bytes  │ 8 bytes   │ (n bytes)│
	// └──────────┴──────────┴──────────┴───────────┴────────┴─────────────┘
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(kd); err != nil {
		return err
	}

	return nil
}

func (kd *KeyDir) decode(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewDecoder(file)
	newKd := make(KeyDir)
	if err := encoder.Decode(&newKd); err != nil {
		return err
	}
	*kd = newKd
	return nil
}
