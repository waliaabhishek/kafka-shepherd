package kafkamanagers

import (
	"fmt"
	"strings"
)

type (
	ConnectionType int
)

const (
	ConnectionType_UNKNOWN ConnectionType = iota
	ConnectionType_SARAMA
	ConnectionType_KAFKA_ACLS
	ConnectionType_CONFLUENT_MDS
)

func (a ConnectionType) String() string {
	mapping := map[ConnectionType]string{
		ConnectionType_UNKNOWN:       "unknown",
		ConnectionType_SARAMA:        "sarama",
		ConnectionType_KAFKA_ACLS:    "kafka_acl",
		ConnectionType_CONFLUENT_MDS: "confluent_mds",
	}
	s, ok := mapping[a]
	if !ok {
		s = mapping[ConnectionType_UNKNOWN]
	}
	return s
}

func (a ConnectionType) stringJoin() (out []string) {
	mapping := map[ConnectionType]string{
		ConnectionType_UNKNOWN:       "unknown",
		ConnectionType_SARAMA:        "sarama",
		ConnectionType_KAFKA_ACLS:    "kafka_acl",
		ConnectionType_CONFLUENT_MDS: "confluent_mds",
	}
	for _, v := range mapping {
		out = append(out, v)
	}
	return out
}

func (c ConnectionType) GetValue(in string) (ConnectionType, error) {
	m := map[string]ConnectionType{
		"unknown":       ConnectionType_UNKNOWN,
		"sarama":        ConnectionType_SARAMA,
		"kafka_acl":     ConnectionType_KAFKA_ACLS,
		"confluent_mds": ConnectionType_CONFLUENT_MDS,
	}
	s, ok := m[strings.ToLower(strings.TrimSpace(in))]
	if !ok {
		s = m["ConnectionType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
}
