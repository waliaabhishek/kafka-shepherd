package engine

import (
	"fmt"
	"strings"
)

///////////////////////////////////////////////////////////////////////////////
//// ACL Resources Interface for helping with different Resource patterns /////
///////////////////////////////////////////////////////////////////////////////

type (
	KafkaResourceType      int
	KafkaACLPermissionType int
	KafkaACLPatternType    int
)

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
	KafkaResourceType_RESOURCE_DELEGATION_TOKEN
	KafkaResourceType_CONNECTOR
	KafkaResourceType_SUBJECT
	KafkaResourceType_KSQL_CLUSTER
	KafkaResourceType_SCHEMA_REGISTRY_CLUSTER
	KafkaResourceType_CONNECT_CLUSTER
)

func (in KafkaResourceType) String() string {
	m := map[KafkaResourceType]string{
		KafkaResourceType_UNKNOWN:                   "KafkaResourceType_UNKNOWN",
		KafkaResourceType_ANY:                       "ANY",
		KafkaResourceType_TOPIC:                     "Topic",
		KafkaResourceType_CLUSTER:                   "Cluster",
		KafkaResourceType_GROUP:                     "Group",
		KafkaResourceType_TRANSACTIONALID:           "TransactionalId",
		KafkaResourceType_CONNECTOR:                 "Connector",
		KafkaResourceType_SUBJECT:                   "Subject",
		KafkaResourceType_KSQL_CLUSTER:              "KsqlCluster",
		KafkaResourceType_SCHEMA_REGISTRY_CLUSTER:   "SchemaRegistryCluster",
		KafkaResourceType_CONNECT_CLUSTER:           "ConnectCluster",
		KafkaResourceType_RESOURCE_DELEGATION_TOKEN: "ResourceDelegationToken",
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
		"SchemaRegistryCluster":     KafkaResourceType_SCHEMA_REGISTRY_CLUSTER,
		"ConnectCluster":            KafkaResourceType_CONNECT_CLUSTER,
		"ResourceDelegationToken":   KafkaResourceType_RESOURCE_DELEGATION_TOKEN,
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

const (
	KafkaACLPermissionType_UNKNOWN KafkaACLPermissionType = iota
	KafkaACLPermissionType_ANY
	KafkaACLPermissionType_DENY
	KafkaACLPermissionType_ALLOW
)

func (in *KafkaACLPermissionType) String() string {
	m := map[KafkaACLPermissionType]string{
		KafkaACLPermissionType_UNKNOWN: "KafkaACLPermissionType_UNKNOWN",
		KafkaACLPermissionType_ANY:     "Any",
		KafkaACLPermissionType_DENY:    "Deny",
		KafkaACLPermissionType_ALLOW:   "Allow",
	}
	s, ok := m[*in]
	if !ok {
		s = m[KafkaACLPermissionType_UNKNOWN]
	}
	return s
}

const (
	KafkaACLPatternType_UNKNOWN KafkaACLPatternType = iota
	KafkaACLPatternType_ANY
	KafkaACLPatternType_MATCH
	KafkaACLPatternType_LITERAL
	KafkaACLPatternType_PREFIXED
)

func (a *KafkaACLPatternType) String() string {
	mapping := map[KafkaACLPatternType]string{
		KafkaACLPatternType_UNKNOWN:  "KafkaACLPatternType_UNKNOWN",
		KafkaACLPatternType_ANY:      "Any",
		KafkaACLPatternType_MATCH:    "Match",
		KafkaACLPatternType_LITERAL:  "Literal",
		KafkaACLPatternType_PREFIXED: "Prefixed",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[KafkaACLPatternType_UNKNOWN]
	}
	return s
}
