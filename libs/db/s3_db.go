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
	"sync"
	"time"
)

// Not registering in favor of registering CachedS3DB as the S3DB backend
//func init() {
//	dbCreator := func(name string, dir string) (DB, error) {
//		return NewS3DB(name, dir)
//	}
//	registerDBCreator(S3DBBackend, dbCreator, true)
//}

var _ DB = (*S3DB)(nil)

// Datastore using an S3 bucket as the backing store.
// This is slower than a local database, but more durable.
// Does not provide synchronous commit and only promises eventual consistency.
// As such, it should not be used for volatile data,
// but is perfectly sufficient for write-once data like a blockchain.
//
// Can be shared by multiple nodes when used with write-once globally-consistent data.
// Bucket permissions should be limited, allowing data to be trusted (no need to re-validate).
//
// TODO: To allow for sharing the blockstore data, Node needs additional logic to allow blockstore height to be ahead.
type S3DB struct {
	bucket     *string
	client     *s3.S3
	downloader *s3manager.Downloader
	prefix     string
}

// Initialize a new S3 client and downloaded helper.
// We assume that `dir` is the bucket name and ensure that the bucket exists.
// If bucket does not exist, we cannot read it, or we cannot create it, we `panic()`
// Data will be stored under a prefix key of the db name (eg, 'blockstore/C:102')
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
		prefix:     name + "/",
	}
	return database, nil
}

// Implements atomicSetDeleter.
// Blocking on a mutex doesn't make a meaningful difference,
// so don't add the overhead. Return nil so callers don't bother.
func (db *S3DB) Mutex() *sync.Mutex {
	//return &(db.mtx)
	return nil
}

// Implements DB.
// Utilize the s3 downloaded helper to pull down the content from our bucket.
// Assume we never store 0-byte records (and panic if we see that)
// TODO: verify assumption about 0-byte records
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
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil
			default:
				panic(aerr.Message())
			}
		} else {
			panic(err)
		}
	}
	if numBytes <= 0 {
		panic("Empty download")
	}
	return buf.Bytes()
}

// Helper function to build an appropriate S3 key given the datastore key
// Joins the db name with the requested key using '/' (eg, 'blockstore/C:102')
func (db *S3DB) s3Key(key []byte) *string {
	return aws.String(db.prefix + string(nonNilBytes(key)))
}

// Implements DB.
// s3.HeadObject
func (db *S3DB) Has(key []byte) bool {
	_, err := db.client.HeadObject(&s3.HeadObjectInput{Bucket: db.bucket, Key: db.s3Key(key)})
	return err == nil
}

// Implements DB.
// s3.PutObject
func (db *S3DB) Set(key []byte, value []byte) {
	_, err := db.client.PutObject(&s3.PutObjectInput{Bucket: db.bucket, Key: db.s3Key(key), Body: bytes.NewReader(value)})
	if err != nil {
		panic(err)
	}
}

// Implements DB.
// No Sync support, just use normal `Set()`
func (db *S3DB) SetSync(key []byte, value []byte) {
	// Sync not supported
	db.Set(key, value)
}

// Implements atomicSetDeleter.
// No Lock support, just use normal `Set()`
func (db *S3DB) SetNoLock(key []byte, value []byte) {
	// Lock not supported
}

// Implements atomicSetDeleter.
// No Lock or Sync support, just use normal `Set()`
func (db *S3DB) SetNoLockSync(key []byte, value []byte) {
	// Sync and/or Lock not supported
	db.Set(key, value)
}

// Implements DB.
// s3.DeleteObject
func (db *S3DB) Delete(key []byte) {
	s3Key := db.s3Key(key)
	_, err := db.client.DeleteObject(&s3.DeleteObjectInput{Bucket: db.bucket, Key: s3Key})
	if err != nil {
		panic(err)
	}
}

// Implements DB.
// No Lock or Sync support, just use normal `Delete()`
func (db *S3DB) DeleteSync(key []byte) {
	db.Delete(key)
}

// Implements atomicSetDeleter.
// No Lock or Sync support, just use normal `Delete()`
func (db *S3DB) DeleteNoLock(key []byte) {
	db.Delete(key)
}

// Implements atomicSetDeleter.
// No Lock or Sync support, just use normal `Delete()`
func (db *S3DB) DeleteNoLockSync(key []byte) {
	db.Delete(key)
}

// Implements DB.
// Nothing to do to close s3 client
func (db *S3DB) Close() {
	// no-op
}

// Implements DB.
// Dumping out complete dataset is not a good idea
// TODO: implement more meaningful output
func (db *S3DB) Print() {
	fmt.Printf(" --- S3 DB -- Bucket: [%X] ---", db.bucket)
}

// Implements DB.
// TODO: dump interesting bucket stats
func (db *S3DB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["database.type"] = "s3DB"
	return stats
}

// Implements DB.
// We can leverage the memDB version to keep things simple.
// This simply executes a batch as a series of single calls.
func (db *S3DB) NewBatch() Batch {
	return &memBatch{db, nil}
}

//----------------------------------------
// Iterator

// Note: Initial version pulls all keys into memory -- rather inefficient.
// TODO: Pull a page-at-a-time off of a channel being populated via go routine
// Not high-priority, as we shouldn't be iterating the bucket in normal case.

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

// Fetch all keys from the bucket, filtered by prefix
func (db *S3DB) getSortedKeys(start, end []byte, reverse bool, prefix string) []string {
	var keys []string

	err := db.client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: db.bucket,
		Prefix: aws.String(db.prefix + "/"),
	}, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, metadata := range output.Contents {
			key := (*metadata.Key)[len(prefix):]
			if IsKeyInDomain([]byte(key), start, end, reverse) {
				keys = append(keys, key)
			}
		}
		return true
	})
	if err != nil {
		panic(err)
	}

	var dataSet sort.Interface
	dataSet = sort.StringSlice(keys)
	if reverse {
		dataSet = sort.Reverse(dataSet)
	}
	sort.Sort(dataSet)
	return keys
}
