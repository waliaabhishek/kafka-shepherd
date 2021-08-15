package topicmanagers

type (
	/* 	Set up a new constant for possible topic management Function
	to standardize action to be performed.
	*/
	TopicManagementType int
	/*	Set up constants for topic current status.
	 */
	statusType int
)

const (
	topicManagementType_UNKNOWN TopicManagementType = iota
	topicManagementType_ALTER_PARTITION_REQUEST
	TopicManagementType_CREATE_TOPIC
	TopicManagementType_MODIFY_TOPIC
	TopicManagementType_DELETE_CONFIG_TOPIC
	TopicManagementType_DELETE_UNKNOWN_TOPIC
	TopicManagementType_LIST_CLUSTER_TOPICS
	TopicManagementType_LIST_CONFIG_TOPICS
)

func (a *TopicManagementType) String() string {
	mapping := map[TopicManagementType]string{
		topicManagementType_UNKNOWN:                 "UNKNOWN",
		topicManagementType_ALTER_PARTITION_REQUEST: "ALTER_PARTITION_REQUEST",
		TopicManagementType_CREATE_TOPIC:            "CREATE_TOPIC",
		TopicManagementType_MODIFY_TOPIC:            "MODIFY_TOPIC",
		TopicManagementType_DELETE_CONFIG_TOPIC:     "DELETE_CONFIG_TOPIC",
		TopicManagementType_DELETE_UNKNOWN_TOPIC:    "DELETE_UNKNOWN_TOPIC",
		TopicManagementType_LIST_CLUSTER_TOPICS:     "LIST_CLUSTER_TOPICS",
		TopicManagementType_LIST_CONFIG_TOPICS:      "LIST_CONFIG_TOPICS",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[topicManagementType_UNKNOWN]
	}
	return s
}

const (
	statusType_UNKNOWN statusType = iota
	statusType_NOT_CREATED
	statusType_CREATING
	statusType_CREATED
	statusType_NOT_DELETED
	statusType_DELETING
	statusType_DELETED
	statusType_NOT_MODIFIED
	statusType_MODIFYING
	statusType_MODIFIED
	statusType_PARTITION_NOT_ALTERED
	statusType_PARTITION_ALTERED_SUCCESSFULLY
)

func (a *statusType) String() string {
	mapping := map[statusType]string{
		statusType_UNKNOWN:                        "UNKNOWN",
		statusType_NOT_CREATED:                    "NOT_CREATED",
		statusType_CREATING:                       "CREATING",
		statusType_CREATED:                        "CREATED",
		statusType_NOT_DELETED:                    "NOT_DELETED",
		statusType_DELETING:                       "DELETING",
		statusType_DELETED:                        "DELETED",
		statusType_NOT_MODIFIED:                   "NOT_MODIFIED",
		statusType_MODIFYING:                      "MODIFYING",
		statusType_MODIFIED:                       "MODIFIED",
		statusType_PARTITION_NOT_ALTERED:          "PARTITION_NOT_ALTERED",
		statusType_PARTITION_ALTERED_SUCCESSFULLY: "PARTITION_ALTERED_SUCCESSFULLY",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[statusType_UNKNOWN]
	}
	return s
}
