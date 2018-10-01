package db

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"sort"
	"strings"
	"sync"
	"time"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewS3DB(name, dir)
	}
	registerDBCreator(S3DBBackend, dbCreator, true)
}

var _ DB = (*MemDB)(nil)

type S3DB struct {
	bucket     *string
	client     *s3.S3
	downloader *s3manager.Downloader
	prefix     string
}

func NewS3DB(name string, dir string) (*S3DB, error) {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	bucketName := aws.String(dir)
	_, err := svc.HeadBucket(&s3.HeadBucketInput{Bucket: bucketName})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
			case s3.ErrCodeNoSuchKey:
				// Attempt to create the requested bucket
				svc.CreateBucket(&s3.CreateBucketInput{Bucket: bucketName})
				svc.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: bucketName})
			default:
				panic(aerr.Message())
			}
		} else {
			panic(err)
		}
	}
	downloader := s3manager.NewDownloader(sess)
	database := &S3DB{
		bucket:     bucketName,
		client:     svc,
		downloader: downloader,
		prefix:     name,
	}
	return database, nil
}

// Implements atomicSetDeleter.
func (db *S3DB) Mutex() *sync.Mutex {
	//return &(db.mtx)
	return nil
}

// Implements DB.
func (db *S3DB) Get(key []byte) []byte {
	buf := &aws.WriteAtBuffer{}

	// Setup timeout
	timeout := 3 * time.Second
	ctx := context.Background()
	var cancelFn func()
	ctx, cancelFn = context.WithTimeout(ctx, timeout)
	defer cancelFn()

	numBytes, err := db.downloader.DownloadWithContext(ctx, buf,
		&s3.GetObjectInput{
			Bucket: db.bucket,
			Key:    db.s3Key(key),
		})

	if err != nil {
		panic(err)
	}
	if numBytes <= 0 {
		panic("Empty download")
	}
	return buf.Bytes()
}

func (db *S3DB) s3Key(key []byte) *string {
	return aws.String(db.prefix + string(nonNilBytes(key)))
}

// Implements DB.
func (db *S3DB) Has(key []byte) bool {
	_, err := db.client.HeadObject(&s3.HeadObjectInput{Bucket: db.bucket, Key: db.s3Key(key)})
	return err == nil
}

// Implements DB.
func (db *S3DB) Set(key []byte, value []byte) {
	_, err := db.client.PutObject(&s3.PutObjectInput{Bucket: db.bucket, Key: db.s3Key(key), Body: bytes.NewReader(value)})
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *S3DB) SetSync(key []byte, value []byte) {
	// Sync not supported
	db.Set(key, value)
}

// Implements atomicSetDeleter.
func (db *S3DB) SetNoLock(key []byte, value []byte) {
	// Lock not supported
}

// Implements atomicSetDeleter.
func (db *S3DB) SetNoLockSync(key []byte, value []byte) {
	// Sync and/or Lock not supported
	db.Set(key, value)
}

// Implements DB.
func (db *S3DB) Delete(key []byte) {
	s3Key := db.s3Key(key)
	_, err := db.client.DeleteObject(&s3.DeleteObjectInput{Bucket: db.bucket, Key: s3Key})
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *S3DB) DeleteSync(key []byte) {
	db.Delete(key)
}

// Implements atomicSetDeleter.
func (db *S3DB) DeleteNoLock(key []byte) {
	db.Delete(key)
}

// Implements atomicSetDeleter.
func (db *S3DB) DeleteNoLockSync(key []byte) {
	db.Delete(key)
}

// Implements DB.
func (db *S3DB) Close() {
	// Close is unnecessary
}

// Implements DB.
func (db *S3DB) Print() {
	fmt.Printf(" --- S3 DB -- Print TBD ---")
	//for key, value := range db.db {
	//	fmt.Printf("[%X]:\t[%X]\n", []byte(key), value)
	//}
}

// Implements DB.
func (db *S3DB) Stats() map[string]string {

	stats := make(map[string]string)
	stats["database.type"] = "s3DB"
	return stats
}

// Implements DB.
// Just leverage memDB version to keep things simple
func (db *S3DB) NewBatch() Batch {
	return &memBatch{db, nil}
}

//----------------------------------------
// Iterator

// Note: Initial version pulls all keys into memory -- rather inefficient.
// Ideally, we'd pull a page-at-a-time off of a channel being populated async,
// but leaving optimizations for later...

// Implements DB.
func (db *S3DB) Iterator(start, end []byte) Iterator {
	keys := db.getSortedKeys(start, end, false, db.prefix)
	return newS3DBIterator(db, keys, start, end)
}

// Implements DB.
func (db *S3DB) ReverseIterator(start, end []byte) Iterator {
	keys := db.getSortedKeys(start, end, true, db.prefix)
	return newS3DBIterator(db, keys, start, end)
}

// We need a copy of all of the keys.
// Not the best, but probably not a bottleneck depending.
type s3DBIterator struct {
	db           *S3DB
	cur          int
	keys         []string
	start        []byte
	end          []byte
	prefixLength int
}

var _ Iterator = (*s3DBIterator)(nil)

// Keys is expected to be in reverse order for reverse iterators.
func newS3DBIterator(db *S3DB, keys []string, start, end []byte) *s3DBIterator {
	return &s3DBIterator{
		db:           db,
		cur:          0,
		keys:         keys,
		start:        start,
		end:          end,
		prefixLength: len(db.prefix),
	}
}

// Implements Iterator.
func (itr *s3DBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Implements Iterator.
func (itr *s3DBIterator) Valid() bool {
	return 0 <= itr.cur && itr.cur < len(itr.keys)
}

// Implements Iterator.
func (itr *s3DBIterator) Next() {
	itr.assertIsValid()
	itr.cur++
}

// Implements Iterator.
func (itr *s3DBIterator) Key() []byte {
	itr.assertIsValid()
	return []byte(itr.keys[itr.cur][itr.prefixLength:])
}

// Implements Iterator.
func (itr *s3DBIterator) Value() []byte {
	itr.assertIsValid()
	key := []byte(itr.keys[itr.cur])
	return itr.db.Get(key)
}

// Implements Iterator.
func (itr *s3DBIterator) Close() {
	itr.keys = nil
	itr.db = nil
}

func (itr *s3DBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("s3DBIterator is invalid")
	}
}

//----------------------------------------
// Misc.

func (db *S3DB) getSortedKeys(start, end []byte, reverse bool, prefix string) []string {
	var keys []string

	err := db.client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: db.bucket,
	}, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, metadata := range output.Contents {
			fullKey := *metadata.Key
			if strings.HasPrefix(fullKey, prefix) {
				key := (*metadata.Key)[len(prefix):]
				inDomain := IsKeyInDomain([]byte(key), start, end, reverse)
				if inDomain {
					keys = append(keys, key)
				}
			}
		}
		return true
	})
	if err != nil {
		panic(err)
	}

	// Don't need to remove prefix before sort since it's same for all entries

	var dataSet sort.Interface
	dataSet = sort.StringSlice(keys)
	if reverse {
		dataSet = sort.Reverse(dataSet)
	}
	sort.Sort(dataSet)
	return keys
}
