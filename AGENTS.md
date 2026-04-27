# go-gcp-pubsub-idempotency-mongodb Agent Guide

Treat [README.md](README.md) as the primary usage and package overview.

## Scope

- This repository is a Go library, not an application service.
- Keep APIs small and focused on MongoDB-backed state for idempotent message processing.
- Preserve backwards compatibility for exported symbols.

## Package Boundaries

- `mongodb_store.go`: MongoDB `Store` implementation for the core idempotency contract.
- `mongodb_store_test.go`: unit tests for constructor and error classification logic.
- `README.md`: integration and behavior guidance for using this store with the core guard.

## Idempotency Conventions

- Claim an ID in the Store on arrival before running handler logic.
- Mark a message ID as processed only after successful handler completion.
- Release in-progress state on handler failure so retries are possible.
- If completion persistence fails, release in-progress state and return an error.
- Treat concurrency and deduplication guarantees as Store semantics and MongoDB atomic updates.
- Keep schema/state transitions explicit (`in_progress` and `processed`).
- Keep persistence concerns behind the core `Store` interface.

## Go Conventions

- Propagate caller context; avoid introducing `context.Background()` in library paths.
- Handle and return errors explicitly.
- Wrap propagated errors with `fmt.Errorf(... %w ...)`.

## Validation

Run before handoff:

```bash
./test.sh
```

```
./lint.sh
```