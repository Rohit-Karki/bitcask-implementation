package bitcask

// key → (file_id, offset, size)
type KeyDirEntry struct {
	FileID int
	Offset int64
	Size   int64
}

type KeyDir map[string]KeyDirEntry
