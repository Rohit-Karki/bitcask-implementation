package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	DATA_DIRECTORY = "./data"
)

/*
	┌──────────┬──────────┬──────────┬───────────┬────────┬─────────────┬──────────┬───────────────┐
	│  CRC     │  Expiry  │ Keysize  │ Valuesize │ KeyLen │     Key     │ ValueLen │     Value     │
	│ 4 bytes  │ 8 bytes  │ 8 bytes  │  8 bytes  │4 bytes │  (n bytes)  │ 4 bytes  │   (m bytes)   │
	└──────────┴──────────┴──────────┴───────────┴────────┴─────────────┴──────────┴───────────────┘
*/

type Record struct {
	Crc       uint32
	Expiry    int64
	Keysize   int64
	Valuesize int64
	Key       string
	Value     []byte
}

// Bytes → Struct
func (r *Record) Decode(record []byte) error {
	reader := bytes.NewReader(record)
	binary.Read(reader, binary.LittleEndian, &r.Crc)
	binary.Read(reader, binary.LittleEndian, &r.Expiry)
	binary.Read(reader, binary.LittleEndian, &r.Keysize)
	binary.Read(reader, binary.LittleEndian, &r.Valuesize)
	var keyLen uint32
	binary.Read(reader, binary.LittleEndian, &keyLen)
	keyBytes := make([]byte, keyLen)
	reader.Read(keyBytes)
	r.Key = string(keyBytes)
	var valueLen uint32
	binary.Read(reader, binary.LittleEndian, &valueLen)
	r.Value = make([]byte, valueLen)
	reader.Read(r.Value)
	return nil
}

// Struct → Bytes
func (r *Record) encode(buf *bytes.Buffer) error {
	binary.Write(buf, binary.LittleEndian, r.Crc)
	binary.Write(buf, binary.LittleEndian, r.Expiry)
	binary.Write(buf, binary.LittleEndian, r.Keysize)
	binary.Write(buf, binary.LittleEndian, r.Valuesize)
	binary.Write(buf, binary.LittleEndian, uint32(len(r.Key)))
	buf.WriteString(r.Key)
	binary.Write(buf, binary.LittleEndian, uint32(len(r.Value)))
	buf.Write(r.Value)
	return nil
}

type DataFile struct {
	reader *os.File
	writer *os.File

	FileID int
	path   string
	Offset int64
}

func (df *DataFile) Read(offset int64, size int64) ([]byte, error) {
	// Read a record from the data file at the given offset
	record := make([]byte, size)
	n, err := df.reader.ReadAt(record, offset)
	if err != nil {
		log.Fatal(err)
	}
	if int64(n) != size {
		return nil, fmt.Errorf("expected to read %d bytes, but read %d bytes", size, n)
	}
	return record, nil
}

func (df *DataFile) Write(key string, value []byte) (int64, error) {
	var buf bytes.Buffer
	// Write the key-value pair to the data file and update the offset

	keySize := int64(len(key))
	valueSize := int64(len(value))
	crc := uint32(0)   // TODO: calculate CRC
	expiry := int64(0) // No expiry for now

	record := Record{
		Crc:       crc,
		Expiry:    expiry,
		Keysize:   keySize,
		Valuesize: valueSize,
		Key:       key,
		Value:     value,
	}

	err := record.encode(&buf)
	if err != nil {
		return 0, err
	}
	size := int64(buf.Len())
	df.Offset += size
	_, err = df.writer.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	err = df.writer.Sync()
	if err != nil {
		return 0, err
	}
	return size, nil
}

func NewDataFile(fileID int, path string) *DataFile {
	writer, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	stat, err := writer.Stat()
	if err != nil {
		log.Fatal(err)
	}

	return &DataFile{
		reader: reader,
		writer: writer,
		FileID: fileID,
		path:   path,
		Offset: int64(stat.Size()),
	}
}

func CreateNewDataFile() *DataFile {
	// create a new data file with a unique file ID and return it
	fileID := int(time.Now().Unix())
	path := fmt.Sprintf("./data/datafile_%d.db", fileID)
	return NewDataFile(fileID, path)
}

func GetStaleDataFiles() []*DataFile {
	// get all stale data files in the data directory
	data_files_path := DATA_DIRECTORY
	var staleDataFiles []*DataFile
	files, err := os.ReadDir(data_files_path)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if !file.IsDir() {
			// create a DataFile instance for each stale data file and add it to the slice
			fileID := extractFileID(file.Name())
			path := fmt.Sprintf("%s/%s", data_files_path, file.Name())
			staleDataFile := NewDataFile(fileID, path)

			staleDataFiles = append(staleDataFiles, staleDataFile)
		}
	}
	return staleDataFiles
}

func extractFileID(filename string) int {
	// extract the file ID from the filename
	var fileID int
	_, err := fmt.Sscanf(filename, "datafile_%d.db", &fileID)
	if err != nil {
		log.Fatal(err)
	}
	return fileID
}

func (df *DataFile) Close() {
	// Close the data file
	df.writer.Sync()
	df.writer = nil
	df.reader = nil
}
