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

/* Create a Struct for Channel signature
 */
type TopicStatusDetails struct {
	topicName  string
	status     StatusType
	errorStr   string
	retryCount int
}

/* 	Set up a new constant for possible topic management Function
to standardize action to be performed.
*/
type TopicManagementFunctionType int

const (
	CREATE_TOPIC TopicManagementFunctionType = iota
	MODIFY_TOPIC
	DELETE_TOPIC
	ALTER_PARTITION_REQUEST
)

func (r TopicManagementFunctionType) String() string {
	return [...]string{"CREATE_TOPIC", "MODIFY_TOPIC", "DELETE_TOPIC", "ALTER_PARTITION_REQUEST"}[r]
}

/*	Set up constants for topic current status.
 */
type StatusType int

const (
	NOT_CREATED StatusType = iota
	CREATING
	CREATED
	NOT_DELETED
	DELETING
	DELETED
	MODIFYING
	MODIFIED
	NOT_MODIFIED
	PARTITION_ALTERED_SUCCESSFULLY
	PARTITION_NOT_ALTERED
)

func (r StatusType) String() string {
	return [...]string{"NOT_CREATED", "CREATING", "CREATED", "NOT_DELETED", "DELETING", "DELETED", "MODIFYING", "MODIFIED", "NOT_MODIFIED", "PARTITION_ALTERED_SUCCESSFULLY", "PARTITION_NOT_ALTERED"}[r]
}
