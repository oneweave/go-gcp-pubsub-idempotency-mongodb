package pubsubidempotencymongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	idempotency "github.com/oneweave/go-gcp-pubsub-idempotency"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	stateInProgress = "in_progress"
	stateProcessed  = "processed"
)

// Config controls MongoDB Store behavior.
type Config struct {
	Collection   *mongo.Collection
	LeaseTimeout time.Duration
}

// Store persists idempotency state in MongoDB and implements the core Store contract.
type Store struct {
	collection   *mongo.Collection
	leaseTimeout time.Duration
	now          func() time.Time
	updateOne    func(ctx context.Context, filter any, update any, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	deleteOne    func(ctx context.Context, filter any, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	findOne      func(ctx context.Context, filter any, opts ...*options.FindOneOptions) singleResultDecoder
}

type singleResultDecoder interface {
	Decode(v any) error
}

var _ idempotency.Store = (*Store)(nil)

// NewStore creates a MongoDB-backed idempotency store.
func NewStore(config Config) (*Store, error) {
	if config.Collection == nil {
		return nil, fmt.Errorf("collection is required")
	}
	if config.LeaseTimeout <= 0 {
		config.LeaseTimeout = 5 * time.Minute
	}

	return &Store{
		collection:   config.Collection,
		leaseTimeout: config.LeaseTimeout,
		now:          time.Now,
		updateOne:    config.Collection.UpdateOne,
		deleteOne:    config.Collection.DeleteOne,
		findOne: func(ctx context.Context, filter any, opts ...*options.FindOneOptions) singleResultDecoder {
			return config.Collection.FindOne(ctx, filter, opts...)
		},
	}, nil
}

// Claim atomically transitions unknown IDs to in-progress in MongoDB.
func (s *Store) Claim(ctx context.Context, messageID string) (idempotency.ClaimResult, error) {
	id := strings.TrimSpace(messageID)
	if id == "" {
		return "", fmt.Errorf("message ID is required")
	}

	now := s.now().UTC()
	leaseUntil := now.Add(s.leaseTimeout)

	filter := bson.M{
		"_id": bson.M{"$eq": id},
		"$or": bson.A{
			bson.M{"state": bson.M{"$exists": false}},
			bson.M{"state": bson.M{"$eq": stateInProgress}, "lease_expires_at": bson.M{"$lte": now}},
		},
	}
	update := bson.M{
		"$set": bson.M{
			"state":            stateInProgress,
			"lease_expires_at": leaseUntil,
			"updated_at":       now,
		},
		"$setOnInsert": bson.M{"created_at": now},
	}

	result, err := s.updateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		if isDuplicateKeyError(err) {
			return s.resolveState(ctx, id, now)
		}
		return "", fmt.Errorf("claim %s: %w", id, err)
	}
	if result.MatchedCount > 0 || result.UpsertedCount > 0 {
		return idempotency.ClaimResultStarted, nil
	}

	return s.resolveState(ctx, id, now)
}

// Complete marks an in-progress ID as processed.
func (s *Store) Complete(ctx context.Context, messageID string) error {
	id := strings.TrimSpace(messageID)
	if id == "" {
		return fmt.Errorf("message ID is required")
	}

	now := s.now().UTC()
	result, err := s.updateOne(
		ctx,
		bson.M{"_id": bson.M{"$eq": id}, "state": bson.M{"$eq": stateInProgress}},
		bson.M{
			"$set":   bson.M{"state": stateProcessed, "updated_at": now},
			"$unset": bson.M{"lease_expires_at": ""},
		},
	)
	if err != nil {
		return fmt.Errorf("complete %s: %w", id, err)
	}
	if result.MatchedCount > 0 {
		return nil
	}

	state, exists, err := s.getState(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("complete %s: message has no prior state", id)
	}
	if state == stateProcessed {
		return nil
	}

	return fmt.Errorf("complete %s: unexpected state %s", id, state)
}

// Release removes in-progress state so a failed message can be retried.
func (s *Store) Release(ctx context.Context, messageID string) error {
	id := strings.TrimSpace(messageID)
	if id == "" {
		return fmt.Errorf("message ID is required")
	}

	_, err := s.deleteOne(ctx, bson.M{"_id": bson.M{"$eq": id}, "state": bson.M{"$eq": stateInProgress}})
	if err != nil {
		return fmt.Errorf("release %s: %w", id, err)
	}
	return nil
}

func (s *Store) resolveState(ctx context.Context, messageID string, now time.Time) (idempotency.ClaimResult, error) {
	doc, err := s.findStateDoc(ctx, messageID)
	if err != nil {
		return "", err
	}

	if doc.State == stateProcessed {
		return idempotency.ClaimResultDuplicate, nil
	}

	if doc.State == stateInProgress {
		if doc.LeaseExpiresAt.IsZero() || doc.LeaseExpiresAt.After(now) {
			return idempotency.ClaimResultInProgress, nil
		}

		// Lease has expired; try again so this caller can refresh claim ownership.
		return s.Claim(ctx, messageID)
	}

	return "", fmt.Errorf("claim %s: unknown stored state %s", messageID, doc.State)
}

func (s *Store) getState(ctx context.Context, messageID string) (string, bool, error) {
	doc, err := s.findStateDoc(ctx, messageID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", false, nil
		}
		return "", false, err
	}
	return doc.State, true, nil
}

type stateDocument struct {
	State          string    `bson:"state"`
	LeaseExpiresAt time.Time `bson:"lease_expires_at,omitempty"`
}

func (s *Store) findStateDoc(ctx context.Context, messageID string) (stateDocument, error) {
	var doc stateDocument
	err := s.findOne(
		ctx,
		bson.M{"_id": bson.M{"$eq": messageID}},
		options.FindOne().SetProjection(bson.M{"state": 1, "lease_expires_at": 1}),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return doc, fmt.Errorf("state lookup %s: %w", messageID, err)
		}
		return doc, fmt.Errorf("state lookup %s: %w", messageID, err)
	}
	return doc, nil
}

func isDuplicateKeyError(err error) bool {
	var writeErr mongo.WriteException
	if errors.As(err, &writeErr) {
		for _, e := range writeErr.WriteErrors {
			if e.Code == 11000 {
				return true
			}
		}
	}

	var bulkErr mongo.BulkWriteException
	if errors.As(err, &bulkErr) {
		for _, e := range bulkErr.WriteErrors {
			if e.Code == 11000 {
				return true
			}
		}
	}

	return false
}
