package kafkamanagers

import (
	"fmt"
	"strings"
)

type (
	ACLType int
)

const (
	ConnectionType_UNKNOWN ACLType = iota
	ConnectionType_KAFKA_ACLS
	ConnectionType_CONFLUENT_RBAC
)

func (a ACLType) String() string {
	mapping := map[ACLType]string{
		ConnectionType_UNKNOWN:        "ConnectionType_UNKNOWN",
		ConnectionType_KAFKA_ACLS:     "kafka_acl",
		ConnectionType_CONFLUENT_RBAC: "confluent_rbac",
	}
	s, ok := mapping[a]
	if !ok {
		s = mapping[ConnectionType_UNKNOWN]
	}
	return s
}

func (a ACLType) stringJoin() (out []string) {
	mapping := map[ACLType]string{
		ConnectionType_KAFKA_ACLS:     "kafka_acl",
		ConnectionType_CONFLUENT_RBAC: "confluent_rbac",
	}
	for _, v := range mapping {
		out = append(out, v)
	}
	return out
}

func (c ACLType) GetValue(in string) (ACLType, error) {
	m := map[string]ACLType{
		"ConnectionType_UNKNOWN": ConnectionType_UNKNOWN,
		"kafka_acl":              ConnectionType_KAFKA_ACLS,
		"confluent_rbac":         ConnectionType_CONFLUENT_RBAC,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["ConnectionType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
}
