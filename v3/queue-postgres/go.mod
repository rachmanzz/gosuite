module github.com/rachmanzz/gosuite/v3/queue-postgres

go 1.26.0

toolchain go1.27.2

require (
	github.com/jackc/pgx/v5 v5.12.0
	github.com/rachmanzz/gosuite/v3/queue v0.2.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/rachmanzz/gosuite/v3/worker v0.2.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/rachmanzz/gosuite/v3/queue => ../queue

replace github.com/rachmanzz/gosuite/v3/worker => ../worker
