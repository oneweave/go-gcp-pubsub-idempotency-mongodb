package pubsubidempotencymongodb

import (
	"context"
	"errors"
	"testing"
	"time"

	idempotency "github.com/oneweave/go-gcp-pubsub-idempotency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type fakeSingleResult struct {
	decode func(v any) error
}

func (f fakeSingleResult) Decode(v any) error {
	if f.decode == nil {
		return nil
	}
	return f.decode(v)
}

func newStubStore() *Store {
	return &Store{
		leaseTimeout: 5 * time.Minute,
		now:          func() time.Time { return time.Date(2026, 4, 27, 12, 0, 0, 0, time.UTC) },
		updateOne: func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return &mongo.UpdateResult{}, nil
		},
		deleteOne: func(context.Context, any, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
			return &mongo.DeleteResult{}, nil
		},
		findOne: func(context.Context, any, ...*options.FindOneOptions) singleResultDecoder {
			return fakeSingleResult{decode: func(v any) error { return mongo.ErrNoDocuments }}
		},
	}
}

func TestNewStoreValidation(t *testing.T) {
	store, err := NewStore(Config{})
	require.Error(t, err)
	assert.Nil(t, store)
	assert.Contains(t, err.Error(), "collection is required")

	store, err = NewStore(Config{Collection: &mongo.Collection{}})
	require.NoError(t, err)
	require.NotNil(t, store)
	assert.Equal(t, 5*time.Minute, store.leaseTimeout)
}

func TestIsDuplicateKeyError(t *testing.T) {
	assert.False(t, isDuplicateKeyError(nil))
	assert.False(t, isDuplicateKeyError(errors.New("plain error")))

	err := mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}
	assert.True(t, isDuplicateKeyError(err))
	err = mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 42}}}
	assert.False(t, isDuplicateKeyError(err))

	bulkErr := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000}}}}
	assert.True(t, isDuplicateKeyError(bulkErr))
}

func TestClaim(t *testing.T) {
	t.Run("validates message id", func(t *testing.T) {
		store := newStubStore()
		result, err := store.Claim(context.Background(), " ")
		require.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "message ID is required")
	})

	t.Run("returns started on update match", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(_ context.Context, filter any, _ any, _ ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			f := filter.(bson.M)
			assert.Equal(t, bson.M{"$eq": "m1"}, f["_id"])
			return &mongo.UpdateResult{MatchedCount: 1}, nil
		}

		result, err := store.Claim(context.Background(), "m1")
		require.NoError(t, err)
		assert.Equal(t, idempotency.ClaimResultStarted, result)
	})

	t.Run("returns started on upsert", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return &mongo.UpdateResult{UpsertedCount: 1}, nil
		}
		result, err := store.Claim(context.Background(), "m2")
		require.NoError(t, err)
		assert.Equal(t, idempotency.ClaimResultStarted, result)
	})

	t.Run("wraps non-duplicate errors", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return nil, errors.New("boom")
		}
		result, err := store.Claim(context.Background(), "m3")
		require.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "claim m3")
	})

	t.Run("duplicate key resolves to duplicate", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return nil, mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}
		}
		store.findOne = func(context.Context, any, ...*options.FindOneOptions) singleResultDecoder {
			return fakeSingleResult{decode: func(v any) error {
				doc := v.(*stateDocument)
				doc.State = stateProcessed
				return nil
			}}
		}

		result, err := store.Claim(context.Background(), "m4")
		require.NoError(t, err)
		assert.Equal(t, idempotency.ClaimResultDuplicate, result)
	})

	t.Run("duplicate key resolves to in progress with valid lease", func(t *testing.T) {
		store := newStubStore()
		now := store.now()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return nil, mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}
		}
		store.findOne = func(context.Context, any, ...*options.FindOneOptions) singleResultDecoder {
			return fakeSingleResult{decode: func(v any) error {
				doc := v.(*stateDocument)
				doc.State = stateInProgress
				doc.LeaseExpiresAt = now.Add(1 * time.Minute)
				return nil
			}}
		}

		result, err := store.Claim(context.Background(), "m5")
		require.NoError(t, err)
		assert.Equal(t, idempotency.ClaimResultInProgress, result)
	})
}

func TestComplete(t *testing.T) {
	t.Run("validates message id", func(t *testing.T) {
		store := newStubStore()
		err := store.Complete(context.Background(), " ")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "message ID is required")
	})

	t.Run("update error is wrapped", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return nil, errors.New("write failed")
		}
		err := store.Complete(context.Background(), "m1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "complete m1")
	})

	t.Run("matched update succeeds", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return &mongo.UpdateResult{MatchedCount: 1}, nil
		}
		err := store.Complete(context.Background(), "m2")
		require.NoError(t, err)
	})

	t.Run("already processed succeeds", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return &mongo.UpdateResult{}, nil
		}
		store.findOne = func(context.Context, any, ...*options.FindOneOptions) singleResultDecoder {
			return fakeSingleResult{decode: func(v any) error {
				doc := v.(*stateDocument)
				doc.State = stateProcessed
				return nil
			}}
		}
		err := store.Complete(context.Background(), "m3")
		require.NoError(t, err)
	})

	t.Run("missing state returns explicit error", func(t *testing.T) {
		store := newStubStore()
		store.updateOne = func(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
			return &mongo.UpdateResult{}, nil
		}
		store.findOne = func(context.Context, any, ...*options.FindOneOptions) singleResultDecoder {
			return fakeSingleResult{decode: func(v any) error { return mongo.ErrNoDocuments }}
		}
		err := store.Complete(context.Background(), "m4")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "message has no prior state")
	})
}

func TestRelease(t *testing.T) {
	t.Run("validates message id", func(t *testing.T) {
		store := newStubStore()
		err := store.Release(context.Background(), " ")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "message ID is required")
	})

	t.Run("delete error is wrapped", func(t *testing.T) {
		store := newStubStore()
		store.deleteOne = func(context.Context, any, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
			return nil, errors.New("delete failed")
		}
		err := store.Release(context.Background(), "m1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release m1")
	})

	t.Run("success", func(t *testing.T) {
		store := newStubStore()
		err := store.Release(context.Background(), "m2")
		require.NoError(t, err)
	})
}
