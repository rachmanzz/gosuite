module github.com/rachmanzz/gosuite/v3/queue-nats

go 1.26.0

toolchain go1.27.2

require (
	github.com/nats-io/nats-server/v2 v2.15.2
	github.com/nats-io/nats.go v1.55.0
	github.com/rachmanzz/gosuite/v3/queue v0.2.0
	github.com/rachmanzz/gosuite/v3/worker v0.2.0
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.8.0-default-no-op // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.2 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.16.0 // indirect
)

replace github.com/rachmanzz/gosuite/v3/queue => ../queue

replace github.com/rachmanzz/gosuite/v3/worker => ../worker
