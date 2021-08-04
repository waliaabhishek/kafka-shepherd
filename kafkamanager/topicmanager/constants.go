package topicmanager

type (
	/* 	Set up a new constant for possible topic management Function
	to standardize action to be performed.
	*/
	TopicManagementType int
	/*	Set up constants for topic current status.
	 */
	StatusType int
)

const (
	TopicManagementType_UNKNOWN TopicManagementType = iota
	TopicManagementType_CREATE_TOPIC
	TopicManagementType_MODIFY_TOPIC
	TopicManagementType_DELETE_TOPIC
	TopicManagementType_ALTER_PARTITION_REQUEST
)

func (a *TopicManagementType) String() string {
	mapping := map[TopicManagementType]string{
		TopicManagementType_UNKNOWN:                 "UNKNOWN",
		TopicManagementType_CREATE_TOPIC:            "CREATE_TOPIC",
		TopicManagementType_MODIFY_TOPIC:            "MODIFY_TOPIC",
		TopicManagementType_DELETE_TOPIC:            "DELETE_TOPIC",
		TopicManagementType_ALTER_PARTITION_REQUEST: "ALTER_PARTITION_REQUEST",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[TopicManagementType_UNKNOWN]
	}
	return s
}

const (
	StatusType_UNKNOWN StatusType = iota
	StatusType_NOT_CREATED
	StatusType_CREATING
	StatusType_CREATED
	StatusType_NOT_DELETED
	StatusType_DELETING
	StatusType_DELETED
	StatusType_NOT_MODIFIED
	StatusType_MODIFYING
	StatusType_MODIFIED
	StatusType_PARTITION_NOT_ALTERED
	StatusType_PARTITION_ALTERED_SUCCESSFULLY
)

func (a *StatusType) String() string {
	mapping := map[StatusType]string{
		StatusType_UNKNOWN:                        "UNKNOWN",
		StatusType_NOT_CREATED:                    "NOT_CREATED",
		StatusType_CREATING:                       "CREATING",
		StatusType_CREATED:                        "CREATED",
		StatusType_NOT_DELETED:                    "NOT_DELETED",
		StatusType_DELETING:                       "DELETING",
		StatusType_DELETED:                        "DELETED",
		StatusType_NOT_MODIFIED:                   "NOT_MODIFIED",
		StatusType_MODIFYING:                      "MODIFYING",
		StatusType_MODIFIED:                       "MODIFIED",
		StatusType_PARTITION_NOT_ALTERED:          "PARTITION_NOT_ALTERED",
		StatusType_PARTITION_ALTERED_SUCCESSFULLY: "PARTITION_ALTERED_SUCCESSFULLY",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[StatusType_UNKNOWN]
	}
	return s
}
