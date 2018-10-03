package db

import "fmt"

//----------------------------------------
// Main entry

type DBBackendType string

const (
	LevelDBBackend       DBBackendType = "leveldb" // legacy, defaults to goleveldb unless +gcc
	CLevelDBBackend      DBBackendType = "cleveldb"
	GoLevelDBBackend     DBBackendType = "goleveldb"
	MemDBBackend         DBBackendType = "memdb"
	FSDBBackend          DBBackendType = "fsdb"          // using the filesystem naively
	S3DBBackend          DBBackendType = "s3db"          // Only use for write-once datasets, not as default for node
	SelectiveS3DBBackend DBBackendType = "selectives3db" // Partial S3, partial LevelDB
)

type dbCreator func(name string, dir string) (DB, error)

var backends = map[DBBackendType]dbCreator{}

func registerDBCreator(backend DBBackendType, creator dbCreator, force bool) {
	_, ok := backends[backend]
	if !force && ok {
		return
	}
	backends[backend] = creator
}

func NewDB(name string, backend DBBackendType, dir string) DB {
	db, err := backends[backend](name, dir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing DB: %v", err))
	}
	return db
}

// Build a new DB using the configured S3 Bucket
// We will *always* store the 'blockstore' in S3 when a bucket is provided.
// We *can* store *all* data in S3 if configured as the default backend, but that isn't recommended.
func NewDBWithS3Bucket(name string, backend DBBackendType, dir string, bucket string) DB {
	switch backend {
	case S3DBBackend:
		// If we're using S3, pass bucket instead of dir to NewDB
		return NewDB(name, backend, bucket)
	case SelectiveS3DBBackend:
		levelDB := NewDB(name, LevelDBBackend, dir)
		db, err := NewSelectiveS3DB(name, bucket, levelDB)
		if err != nil {
			panic(fmt.Sprintf("Error initializing DB: %v", err))
		}
		return db
	default:
		switch name {
		case "blockstore":
			// Override to always use S3 for blockstore when bucket is specified
			return NewDB(name, S3DBBackend, bucket)
		default:
			return NewDB(name, backend, dir)
		}
	}
}
