# go-gcp-pubsub-idempotency-mongodb

go-gcp-pubsub-idempotency-mongodb provides a MongoDB-backed Store implementation for [go-gcp-pubsub-idempotency](https://github.com/oneweave/go-gcp-pubsub-idempotency).

It implements the core Store contract:
- `Claim` atomically transitions unknown IDs to `in_progress`.
- `Complete` transitions `in_progress` to `processed`.
- `Release` removes `in_progress` so retries can run after failures.

This follows the idempotency pattern from the OneUptime Cloud Run push subscriber example: mark success only after successful completion, allow retries on failures, and prevent concurrent duplicate handling.

## Install

```bash
go get github.com/oneweave/go-gcp-pubsub-idempotency-mongodb
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    pubsubidempotency "github.com/oneweave/go-gcp-pubsub-idempotency"
    pubsubidempotencymongodb "github.com/oneweave/go-gcp-pubsub-idempotency-mongodb"
    "go.mongodb.org/mongo-driver/mongo"
)

func example(ctx context.Context, collection *mongo.Collection, messageID string) error {
    store, err := pubsubidempotencymongodb.NewStore(pubsubidempotencymongodb.Config{
        Collection:   collection,
        LeaseTimeout: 2 * time.Minute,
    })
    if err != nil {
        return err
    }

    guard := pubsubidempotency.NewGuard(store)
    outcome, err := guard.Execute(ctx, messageID, func(context.Context) error {
        // Your business logic here.
        return nil
    })
    if err != nil {
        return err
    }

    log.Printf("outcome=%s", outcome)
    return nil
}
```

## MongoDB Behavior

- State is stored per message ID using `_id` as the key.
- The store uses `in_progress` and `processed` states.
- Claim applies a lease (`LeaseTimeout`) to `in_progress` entries so stale work can be reclaimed.
- Release deletes only `in_progress` records; `processed` records remain deduplicated.

## Requirements

- A MongoDB collection handle from `go.mongodb.org/mongo-driver/mongo`.
- A core guard from `github.com/oneweave/oneweave-go-pubsub-idempotency`.
- Message IDs should be stable and unique for dedupe.

## Development

```bash
./test.sh
./lint.sh
```
