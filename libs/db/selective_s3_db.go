package db

import (
	"regexp"
)

// SelectiveS3DB combines two databases, matching a regex against the key to determine which database to use
// for a given item. This is intended to allow for certain high-volatility records to be stored in a local
// database while storing the larger, read-once records off-box in an S3 bucket.
// The current implementation is not substantially faster than CachedS3DB under real-world conditions
// and is not recommended unless performance testing and/or tuning proves it to be worth the trade-off
// of separating the data.
type SelectiveS3DB struct {
	DB
	slow    *CachedS3DB
	useSlow func([]byte) bool
}

var _ DB = (*SelectiveS3DB)(nil)

func NewSelectiveS3DB(name string, bucket string, fasterDB DB) (*SelectiveS3DB, error) {
	s3DB, err := NewCachedS3DB(name, bucket)
	if err != nil {
		panic(err)
	}
	return &SelectiveS3DB{DB: fasterDB, slow: s3DB, useSlow: func(key []byte) bool {
		switch name {
		case "blockstore":
			matched, err := regexp.Match("^(C|P|SC):", key)
			if err != nil {
				panic(err)
			}
			return matched
		default:
			return false
		}
	}}, nil
}

// Implements DB.
func (db *SelectiveS3DB) Get(key []byte) []byte {
	if db.useSlow(key) {
		return db.slow.Get(key)
	} else {
		return db.DB.Get(key)
	}
}

// Implements DB.
func (db *SelectiveS3DB) Has(key []byte) bool {
	if db.useSlow(key) {
		return db.slow.Has(key)
	} else {
		return db.DB.Has(key)
	}
}

// Implements DB.
func (db *SelectiveS3DB) Set(key []byte, value []byte) {
	if db.useSlow(key) {
		db.slow.Set(key, value)
	} else {
		db.DB.Set(key, value)
	}
}

// Implements DB.
func (db *SelectiveS3DB) SetSync(key []byte, value []byte) {
	if db.useSlow(key) {
		db.slow.SetSync(key, value)
	} else {
		db.DB.SetSync(key, value)
	}
}

// Implements DB.
func (db *SelectiveS3DB) Delete(key []byte) {
	if db.useSlow(key) {
		db.slow.Delete(key)
	} else {
		db.DB.Delete(key)
	}
}

// Implements DB.
func (db *SelectiveS3DB) DeleteSync(key []byte) {
	if db.useSlow(key) {
		db.slow.DeleteSync(key)
	} else {
		db.DB.DeleteSync(key)
	}
}

// Implements DB.
func (db *SelectiveS3DB) Close() {
	db.DB.Close()
	db.slow.Close()
}

//----------------------------------------
// Batch

// Implements DB.
func (db *SelectiveS3DB) NewBatch() Batch {

	batch := db.DB.NewBatch()
	slow_batch := db.slow.NewBatch()
	return &selectiveS3DBBatch{db, batch, slow_batch}
}

type selectiveS3DBBatch struct {
	db         *SelectiveS3DB
	batch      Batch
	slow_batch Batch
}

// Implements Batch.
func (mBatch *selectiveS3DBBatch) Set(key, value []byte) {
	if mBatch.db.useSlow(key) {
		mBatch.slow_batch.Set(key, value)
	} else {
		mBatch.batch.Set(key, value)
	}
}

// Implements Batch.
func (mBatch *selectiveS3DBBatch) Delete(key []byte) {
	if mBatch.db.useSlow(key) {
		mBatch.slow_batch.Delete(key)
	} else {
		mBatch.batch.Delete(key)
	}
}

// Implements Batch.
func (mBatch *selectiveS3DBBatch) Write() {
	// Write both batches in parallel using channel to sync
	c := make(chan bool, 1)
	go func() bool {
		mBatch.slow_batch.Write()
		return true
	}()
	mBatch.batch.Write()
	<-c
}

// Implements Batch.
func (mBatch *selectiveS3DBBatch) WriteSync() {
	// Write both batches in parallel using channel to sync
	c := make(chan bool, 1)
	go func() {
		mBatch.slow_batch.WriteSync()
		c <- true
	}()
	mBatch.batch.WriteSync()
	<-c
}

//----------------------------------------
// Iterator

// Avoid complex iterator, we shouldn't be iterating the slow keys...

// Implements DB.
//func (db *SelectiveS3DB) Iterator(start, end []byte) Iterator {
//}

// Implements DB.
//func (db *SelectiveS3DB) ReverseIterator(start, end []byte) Iterator {
//}
