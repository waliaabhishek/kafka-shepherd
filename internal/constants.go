package internal

type ClientType int

const (
	PRODUCER ClientType = iota
	CONSUMER
	SOURCE_CONNECTOR
	SINK_CONNECTOR
	STREAM_READ
	STREAM_WRITE
	KSQL
)

func (r ClientType) String() string {
	return [...]string{"PRODUCER", "CONSUMER", "SOURCE_CONNECTOR", "SINK_CONNECTOR", "STREAM_READ", "STREAM_WRITE", "KSQL"}[r]
}
