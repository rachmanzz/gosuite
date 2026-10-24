module github.com/rachmanzz/gosuite/v3/queue-rabbitmq

go 1.26.0

toolchain go1.27.2

require (
	github.com/rabbitmq/amqp091-go v1.16.0
	github.com/rachmanzz/gosuite/v3/queue v0.2.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/rachmanzz/gosuite/v3/worker v0.2.0 // indirect
)

replace github.com/rachmanzz/gosuite/v3/queue => ../queue

replace github.com/rachmanzz/gosuite/v3/worker => ../worker
