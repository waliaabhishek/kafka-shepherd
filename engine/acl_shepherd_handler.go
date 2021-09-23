package engine

import (
	"fmt"
	"strings"
)

var ShepherdEngineACLManager ShepherdACLConfigManager = ShepherdACLEngineImpl{}

type ShepherdACLEngineImpl struct {
	ShepherdACLConfigManagerBaseImpl
	ShepherdOperationType
}

type (
	ShepherdOperationType int
)

const (
	ShepherdOperationType_UNKNOWN ShepherdOperationType = iota
	ShepherdOperationType_EMPTY
	ShepherdOperationType_PRODUCER
	ShepherdOperationType_TRANSACTIONAL_PRODUCER
	ShepherdOperationType_PRODUCER_IDEMPOTENCE
	ShepherdOperationType_CONSUMER
	ShepherdOperationType_CONSUMER_GROUP
	ShepherdOperationType_SOURCE_CONNECTOR
	ShepherdOperationType_SINK_CONNECTOR
	ShepherdOperationType_STREAM_READ
	ShepherdOperationType_STREAM_WRITE
	ShepherdOperationType_KSQL_READ
	ShepherdOperationType_KSQL_WRITE
)

func (in ShepherdOperationType) String() string {
	m := map[ShepherdOperationType]string{
		ShepherdOperationType_UNKNOWN:                "ShepherdOperationType_UNKNOWN",
		ShepherdOperationType_EMPTY:                  "EMPTY",
		ShepherdOperationType_PRODUCER:               "PRODUCER",
		ShepherdOperationType_TRANSACTIONAL_PRODUCER: "TRANSACTIONAL_PRODUCER",
		ShepherdOperationType_PRODUCER_IDEMPOTENCE:   "PRODUCER_IDEMPOTENCE",
		ShepherdOperationType_CONSUMER:               "CONSUMER",
		ShepherdOperationType_CONSUMER_GROUP:         "CONSUMER_GROUP",
		ShepherdOperationType_SOURCE_CONNECTOR:       "SOURCE_CONNECTOR",
		ShepherdOperationType_SINK_CONNECTOR:         "SINK_CONNECTOR",
		ShepherdOperationType_STREAM_READ:            "STREAM_READ",
		ShepherdOperationType_STREAM_WRITE:           "STREAM_WRITE",
		ShepherdOperationType_KSQL_READ:              "KSQL_READ",
		ShepherdOperationType_KSQL_WRITE:             "KSQL_WRITE",
		// ShepherdOperationType_KSQL:                   "KSQL",
	}
	ret, present := m[in]
	if !present {
		ret = m[ShepherdOperationType_UNKNOWN]
	}
	return ret
}

func (c ShepherdOperationType) GetValue(in string) (ACLOperationsInterface, error) {
	m := map[string]ShepherdOperationType{
		"ShepherdOperationType_UNKNOWN": ShepherdOperationType_UNKNOWN,
		"EMPTY":                         ShepherdOperationType_EMPTY,
		"PRODUCER":                      ShepherdOperationType_PRODUCER,
		"TRANSACTIONAL_PRODUCER":        ShepherdOperationType_TRANSACTIONAL_PRODUCER,
		"PRODUCER_IDEMPOTENCE":          ShepherdOperationType_PRODUCER_IDEMPOTENCE,
		"CONSUMER":                      ShepherdOperationType_CONSUMER,
		"CONSUMER_GROUP":                ShepherdOperationType_CONSUMER_GROUP,
		"SOURCE_CONNECTOR":              ShepherdOperationType_SOURCE_CONNECTOR,
		"SINK_CONNECTOR":                ShepherdOperationType_SINK_CONNECTOR,
		"STREAM_READ":                   ShepherdOperationType_STREAM_READ,
		"STREAM_WRITE":                  ShepherdOperationType_STREAM_WRITE,
		"KSQL_READ":                     ShepherdOperationType_KSQL_READ,
		"KSQL_WRITE":                    ShepherdOperationType_KSQL_WRITE,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["ShepherdOperationType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
}

// This function will generate the mappings in the ShepherdOperationType internal structure type for all the mappings provided
// as the input `in` value. Whatever it is able to properly convert, those mappings will be added to the success
// map and the rest will be added to the failed map.
func (c ShepherdOperationType) GenerateACLMappingStructures(clusterName string, in *ACLMapping) *ACLMapping {
	logger.Info("Conversion back to Shepherd ACL is performed on a best effort basis. Please ensure that you back up the actual ACLs as well.")
	temp := make(ACLMapping)
	for k, v := range *in {
		switch k.Operation.(type) {
		case ShepherdOperationType:
			temp[k] = v
		case KafkaACLOperation:
			switch k.Operation {
			case KafkaACLOperation_READ:
				temp[constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ShepherdOperationType_CONSUMER, k.Hostname)] = v
			case KafkaACLOperation_WRITE:
			case KafkaACLOperation_IDEMPOTENTWRITE:
			case KafkaACLOperation_DESCRIBE:
			default:
				logger.Warnw(" Could not generate Shepherd ACL Mappings for the provided ACL Map. Details:",
					"Principal", k.Principal,
					"Resource Type", k.ResourceType.GetACLResourceString(),
					"Resource Value", k.ResourceName,
					"Hostname", k.Hostname,
					"ACL Operation Wanted", k.Operation.String())
			}
		default:
			logger.Warnf("Conversion from %T type to %T type is not supported yet. The ACL mapping will be added to the Failed list.", k.Operation, c)
			temp[k] = nil
		}
	}
	return &temp
}
