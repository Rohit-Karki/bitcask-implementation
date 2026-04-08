package bitcask

import (
	"os"

	datafile "bitcask/internal"
)

type Bitcask struct {
	keyDir          KeyDir
	activeDataFile  *datafile.DataFile
	stale_DataFiles []*datafile.DataFile
}

func (bc *Bitcask) Set(key string, value []byte) error {
	// Write the key-value pair to the active data file and update the keyDir
	offset := bc.activeDataFile.Offset
	size, err := bc.activeDataFile.Write(key, value)
	if err != nil {
		return err
	}
	// Update keyDir with the new offset and fileID
	bc.keyDir[key] = KeyDirEntry{
		FileID: bc.activeDataFile.FileID,
		Offset: offset,
		Size:   size,
	}
	return nil
}

func (bc *Bitcask) Get(key string) ([]byte, error) {
	// Look up the key in the keyDir to find the corresponding data file and offset, then read the value from the data file
	keyDirEntry, ok := bc.keyDir[key]
	if !ok {
		return nil, nil
	}
	// Find the corresponding data file based on the fileID
	var dataFile *datafile.DataFile
	if keyDirEntry.FileID == bc.activeDataFile.FileID {
		dataFile = bc.activeDataFile
	} else {
		for _, df := range bc.stale_DataFiles {
			if df.FileID == keyDirEntry.FileID {
				dataFile = df
				break
			}
		}
	}
	if dataFile == nil {
		return nil, nil
	}
	r, err := dataFile.Read(keyDirEntry.Offset, keyDirEntry.Size)
	if err != nil {
		return nil, err
	}
	var record = &datafile.Record{}
	err = record.Decode(r)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

func NewInitBitcask() (*Bitcask, error) {
	// get all data files, read their key-value pairs, and populate the keyDir
	// first check for existing data files and populate keyDir
	db_files_path := DATA_DIRECTORY
	// create directory if it doesn't exist
	if _, err := os.Stat(db_files_path); os.IsNotExist(err) {
		os.Mkdir(db_files_path, 0755)
	}
	var active_dataFile *datafile.DataFile

	stale_dataFiles := datafile.GetStaleDataFiles()
	for _, df := range stale_dataFiles {
		populateKeyDirFromDataFile(df)
	}
	active_dataFile = datafile.CreateNewDataFile()
	keyDir := make(KeyDir, 0)
	// fill the keyDir with the key-value pairs from the stale data files
	// keyDir =
	var bitcask = &Bitcask{
		keyDir:          keyDir,
		activeDataFile:  active_dataFile,
		stale_DataFiles: stale_dataFiles,
	}

	return bitcask, nil
}

func populateKeyDirFromDataFile(df *datafile.DataFile) {

}

func (bc *Bitcask) Close() {
	// Close the active data file and all stale data files
	bc.activeDataFile.Close()
	for _, df := range bc.stale_DataFiles {
		df.Close()
	}
}
