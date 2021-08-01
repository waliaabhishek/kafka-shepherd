package core

import (
	"fmt"
	"strings"
)

///////////////////////////////////////////////////////////////////////////////
//// ACL Resources Interface for helping with different Resource patterns /////
///////////////////////////////////////////////////////////////////////////////

type ACLResourceInterface interface {
	String() string
	GetValue(in string) (ACLResourceInterface, error)
}

const (
	KafkaResourceType_UNKNOWN KafkaResourceType = iota
	KafkaResourceType_ANY
	KafkaResourceType_TOPIC
	KafkaResourceType_CLUSTER
	KafkaResourceType_GROUP
	KafkaResourceType_TRANSACTIONALID
	// KafkaResourceType_ResourceDelegationToken
	KafkaResourceType_CONNECTOR
	KafkaResourceType_SUBJECT
	KafkaResourceType_KSQL_CLUSTER
)

func (in KafkaResourceType) String() string {
	m := map[KafkaResourceType]string{
		KafkaResourceType_UNKNOWN:         "KafkaResourceType_UNKNOWN",
		KafkaResourceType_ANY:             "ANY",
		KafkaResourceType_TOPIC:           "Topic",
		KafkaResourceType_CLUSTER:         "Cluster",
		KafkaResourceType_GROUP:           "Group",
		KafkaResourceType_TRANSACTIONALID: "TransactionalId",
		KafkaResourceType_CONNECTOR:       "Connector",
		KafkaResourceType_SUBJECT:         "Subject",
		KafkaResourceType_KSQL_CLUSTER:    "KsqlCluster",
	}
	s, ok := m[in]
	if !ok {
		s = m[KafkaResourceType_UNKNOWN]
	}
	return s
}

func (c KafkaResourceType) GetValue(in string) (ACLResourceInterface, error) {
	m := map[string]KafkaResourceType{
		"KafkaResourceType_UNKNOWN": KafkaResourceType_UNKNOWN,
		"ANY":                       KafkaResourceType_ANY,
		"Topic":                     KafkaResourceType_TOPIC,
		"Cluster":                   KafkaResourceType_CLUSTER,
		"Group":                     KafkaResourceType_GROUP,
		"TransactionalId":           KafkaResourceType_TRANSACTIONALID,
		"Connector":                 KafkaResourceType_CONNECTOR,
		"Subject":                   KafkaResourceType_SUBJECT,
		"KsqlCluster":               KafkaResourceType_KSQL_CLUSTER,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["KafkaResourceType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl Operation string provided. input string: %s", in)
	}
	return s, nil
}
