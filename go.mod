module github.com/oneweave/go-gcp-pubsub-idempotency-mongodb

go 1.23.0

require (
	github.com/oneweave/oneweave-go-pubsub-idempotency v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.11.0
	go.mongodb.org/mongo-driver v1.17.9
)

replace github.com/oneweave/oneweave-go-pubsub-idempotency => ../oneweave-go-pubsub-idempotency

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
