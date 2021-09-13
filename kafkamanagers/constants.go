package kafkamanagers

import (
	"fmt"
	"strings"
)

type (
	ACLType int
)

const (
	ACLType_UNKNOWN ACLType = iota
	ACLType_KAFKA_ACLS
	ACLType_CONFLUENT_RBAC
)

func (a ACLType) String() string {
	mapping := map[ACLType]string{
		ACLType_KAFKA_ACLS:     "kafka_acl",
		ACLType_CONFLUENT_RBAC: "confluent_rbac",
	}
	s, ok := mapping[a]
	if !ok {
		s = mapping[ACLType_UNKNOWN]
	}
	return s
}

func (a ACLType) stringJoin() (out []string) {
	mapping := map[ACLType]string{
		ACLType_KAFKA_ACLS:     "kafka_acl",
		ACLType_CONFLUENT_RBAC: "confluent_rbac",
	}
	for _, v := range mapping {
		out = append(out, v)
	}
	return out
}

func (c ACLType) GetValue(in string) (ACLType, error) {
	m := map[string]ACLType{
		"kafka_acl":      ACLType_KAFKA_ACLS,
		"confluent_rbac": ACLType_CONFLUENT_RBAC,
	}
	s, ok := m[strings.ToLower(strings.TrimSpace(in))]
	if !ok {
		s = m["ACLType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
}
