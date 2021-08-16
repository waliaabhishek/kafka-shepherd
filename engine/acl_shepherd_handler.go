package engine

import (
	"fmt"
	"strings"
)

// var SACLManager ShepherdACLManager = ShepherdACLEngineImpl{}

type ShepherdACLEngineImpl struct {
	ShepherdACLManagerBaseImpl
	ShepherdClientType
}

type (
	ShepherdClientType int
)

const (
	ShepherdClientType_UNKNOWN ShepherdClientType = iota
	ShepherdClientType_EMPTY
	ShepherdClientType_PRODUCER
	ShepherdClientType_TRANSACTIONAL_PRODUCER
	ShepherdClientType_PRODUCER_IDEMPOTENCE
	ShepherdClientType_CONSUMER
	ShepherdClientType_CONSUMER_GROUP
	ShepherdClientType_SOURCE_CONNECTOR
	ShepherdClientType_SINK_CONNECTOR
	ShepherdClientType_STREAM_READ
	ShepherdClientType_STREAM_WRITE
	ShepherdClientType_KSQL
)

func (in ShepherdClientType) String() string {
	m := map[ShepherdClientType]string{
		ShepherdClientType_UNKNOWN:                "ShepherdClientType_UNKNOWN",
		ShepherdClientType_EMPTY:                  "EMPTY",
		ShepherdClientType_PRODUCER:               "PRODUCER",
		ShepherdClientType_TRANSACTIONAL_PRODUCER: "TRANSACTIONAL_PRODUCER",
		ShepherdClientType_PRODUCER_IDEMPOTENCE:   "PRODUCER_IDEMPOTENCE",
		ShepherdClientType_CONSUMER:               "CONSUMER",
		ShepherdClientType_CONSUMER_GROUP:         "CONSUMER_GROUP",
		ShepherdClientType_SOURCE_CONNECTOR:       "SOURCE_CONNECTOR",
		ShepherdClientType_SINK_CONNECTOR:         "SINK_CONNECTOR",
		ShepherdClientType_STREAM_READ:            "STREAM_READ",
		ShepherdClientType_STREAM_WRITE:           "STREAM_WRITE",
		ShepherdClientType_KSQL:                   "KSQL",
	}
	ret, present := m[in]
	if !present {
		ret = m[ShepherdClientType_UNKNOWN]
	}
	return ret
}

func (c ShepherdClientType) GetValue(in string) (ACLOperationsInterface, error) {
	m := map[string]ShepherdClientType{
		"ShepherdClientType_UNKNOWN": ShepherdClientType_UNKNOWN,
		"EMPTY":                      ShepherdClientType_EMPTY,
		"PRODUCER":                   ShepherdClientType_PRODUCER,
		"TRANSACTIONAL_PRODUCER":     ShepherdClientType_TRANSACTIONAL_PRODUCER,
		"PRODUCER_IDEMPOTENCE":       ShepherdClientType_PRODUCER_IDEMPOTENCE,
		"CONSUMER":                   ShepherdClientType_CONSUMER,
		"CONSUMER_GROUP":             ShepherdClientType_CONSUMER_GROUP,
		"SOURCE_CONNECTOR":           ShepherdClientType_SOURCE_CONNECTOR,
		"SINK_CONNECTOR":             ShepherdClientType_SINK_CONNECTOR,
		"STREAM_READ":                ShepherdClientType_STREAM_READ,
		"STREAM_WRITE":               ShepherdClientType_STREAM_WRITE,
		"KSQL":                       ShepherdClientType_KSQL,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["ShepherdClientType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
}

// This function will generate the mappings in the ShepherdClientType internal structure type for all the mappings provided
// as the input `in` value. Whatever it is able to properly convert, those mappings will be added to the success
// map and the rest will be added to the failed map.
func (c ShepherdClientType) generateACLMappingStructures(in *ACLMapping) *ACLMapping {
	temp := make(ACLMapping)
	for k := range *in {
		switch k.Operation.(type) {
		case ShepherdClientType:
			temp[k] = nil
		default:
			logger.Warnf("Conversion from %T type to %T type is not supported yet. The ACL mapping will be added to the Failed list.", k.Operation, c)
			temp[k] = nil
		}
	}
	return &temp
}
