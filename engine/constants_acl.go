package engine

import (
	"fmt"
	"strings"
)

///////////////////////////////////////////////////////////////////////////////
//// ACL Resources Interface for helping with different Resource patterns /////
///////////////////////////////////////////////////////////////////////////////

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
	KafkaResourceType_KAFKA_CLUSTER
	KafkaResourceType_KSQL_CLUSTER
	KafkaResourceType_SCHEMA_REGISTRY_CLUSTER
	KafkaResourceType_CONNECT_CLUSTER
)

func (in KafkaResourceType) GetACLResourceString() string {
	m := map[KafkaResourceType]string{
		KafkaResourceType_UNKNOWN:                   "KafkaResourceType_UNKNOWN",
		KafkaResourceType_ANY:                       "ANY",
		KafkaResourceType_TOPIC:                     "TOPIC",
		KafkaResourceType_CLUSTER:                   "CLUSTER",
		KafkaResourceType_GROUP:                     "GROUP",
		KafkaResourceType_TRANSACTIONALID:           "TRANSACTIONALID",
		KafkaResourceType_CONNECTOR:                 "CONNECTOR",
		KafkaResourceType_SUBJECT:                   "SUBJECT",
		KafkaResourceType_KAFKA_CLUSTER:             "KAFKA-CLUSTER",
		KafkaResourceType_KSQL_CLUSTER:              "KSQL-CLUSTER",
		KafkaResourceType_SCHEMA_REGISTRY_CLUSTER:   "SCHEMA-REGISTRY-CLUSTER",
		KafkaResourceType_CONNECT_CLUSTER:           "CONNECT-CLUSTER",
		KafkaResourceType_RESOURCE_DELEGATION_TOKEN: "RESOURCE-DELEGATION-TOKEN",
	}
	s, ok := m[in]
	if !ok {
		s = m[KafkaResourceType_UNKNOWN]
	}
	return s
}

func (c KafkaResourceType) GetACLResourceValue(in string) (ACLResourceInterface, error) {
	m := map[string]KafkaResourceType{
		"KafkaResourceType_UNKNOWN": KafkaResourceType_UNKNOWN,
		"ANY":                       KafkaResourceType_ANY,
		"TOPIC":                     KafkaResourceType_TOPIC,
		"CLUSTER":                   KafkaResourceType_CLUSTER,
		"GROUP":                     KafkaResourceType_GROUP,
		"TRANSACTIONALID":           KafkaResourceType_TRANSACTIONALID,
		"CONNECTOR":                 KafkaResourceType_CONNECTOR,
		"SUBJECT":                   KafkaResourceType_SUBJECT,
		"KAFKA-CLUSTER":             KafkaResourceType_KAFKA_CLUSTER,
		"KSQL-CLUSTER":              KafkaResourceType_KSQL_CLUSTER,
		"KSQLCLUSTER":               KafkaResourceType_KSQL_CLUSTER,
		"SCHEMA-REGISTRY-CLUSTER":   KafkaResourceType_SCHEMA_REGISTRY_CLUSTER,
		"CONNECT-CLUSTER":           KafkaResourceType_CONNECT_CLUSTER,
		"RESOURCE-DELEGATION-TOKEN": KafkaResourceType_RESOURCE_DELEGATION_TOKEN,
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
	KafkaACLPatternType_UNKNOWN KafkaACLPatternType = iota
	KafkaACLPatternType_ANY
	KafkaACLPatternType_MATCH
	KafkaACLPatternType_LITERAL
	KafkaACLPatternType_PREFIXED
)

func (a KafkaACLPatternType) GetACLPatternString() string {
	mapping := map[KafkaACLPatternType]string{
		KafkaACLPatternType_UNKNOWN:  "KafkaACLPatternType_UNKNOWN",
		KafkaACLPatternType_ANY:      "Any",
		KafkaACLPatternType_MATCH:    "Match",
		KafkaACLPatternType_LITERAL:  "Literal",
		KafkaACLPatternType_PREFIXED: "Prefixed",
	}
	s, ok := mapping[a]
	if !ok {
		s = mapping[KafkaACLPatternType_UNKNOWN]
	}
	return s
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
