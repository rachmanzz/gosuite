module github.com/rachmanzz/gosuite/v3/queue-redis

go 1.26.0

toolchain go1.27.2

require (
	github.com/rachmanzz/gosuite/v3/queue v0.2.3
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/rachmanzz/gosuite/v3/worker v0.2.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/rachmanzz/gosuite/v3/queue => ../queue

replace github.com/rachmanzz/gosuite/v3/worker => ../worker
