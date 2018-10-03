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
