package engine

import (
	"fmt"
	"strings"
)

var KafkaACLManager ShepherdACLConfigManager = KafkaCoreACLEngineImpl{}

type KafkaCoreACLEngineImpl struct {
	ShepherdACLConfigManagerBaseImpl
	KafkaACLOperation
}

type (
	KafkaACLOperation int
)

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java
// This Constant list is a set of all the values listed in Apache Kafka. This helps to ensure that the complete and
// only fail in case of an outlier.
const (
	KafkaACLOperation_UNKNOWN KafkaACLOperation = iota
	KafkaACLOperation_ANY
	KafkaACLOperation_ALL
	KafkaACLOperation_READ
	KafkaACLOperation_WRITE
	KafkaACLOperation_CREATE
	KafkaACLOperation_DELETE
	KafkaACLOperation_ALTER
	KafkaACLOperation_DESCRIBE
	KafkaACLOperation_CLUSTERACTION
	KafkaACLOperation_DESCRIBECONFIGS
	KafkaACLOperation_ALTERCONFIGS
	KafkaACLOperation_IDEMPOTENTWRITE
)

func (in KafkaACLOperation) String() string {
	m := map[KafkaACLOperation]string{
		KafkaACLOperation_UNKNOWN:         "KafkaACLOperation_UNKNOWN",
		KafkaACLOperation_ANY:             "ANY",
		KafkaACLOperation_ALL:             "ALL",
		KafkaACLOperation_READ:            "READ",
		KafkaACLOperation_WRITE:           "WRITE",
		KafkaACLOperation_CREATE:          "CREATE",
		KafkaACLOperation_DELETE:          "DELETE",
		KafkaACLOperation_ALTER:           "ALTER",
		KafkaACLOperation_DESCRIBE:        "DESCRIBE",
		KafkaACLOperation_CLUSTERACTION:   "CLUSTER_ACTION",
		KafkaACLOperation_DESCRIBECONFIGS: "DESCRIBE_CONFIGS",
		KafkaACLOperation_ALTERCONFIGS:    "ALTER_CONFIGS",
		KafkaACLOperation_IDEMPOTENTWRITE: "IDEMPOTENT_WRITE",
	}
	s, ok := m[in]
	if !ok {
		s = m[KafkaACLOperation_UNKNOWN]
	}
	return s
}

func (c KafkaACLOperation) GetValue(in string) (ACLOperationsInterface, error) {
	m := map[string]KafkaACLOperation{
		"KafkaACLOperation_UNKNOWN": KafkaACLOperation_UNKNOWN,
		"ANY":                       KafkaACLOperation_ANY,
		"ALL":                       KafkaACLOperation_ALL,
		"READ":                      KafkaACLOperation_READ,
		"WRITE":                     KafkaACLOperation_WRITE,
		"CREATE":                    KafkaACLOperation_CREATE,
		"DELETE":                    KafkaACLOperation_DELETE,
		"ALTER":                     KafkaACLOperation_ALTER,
		"DESCRIBE":                  KafkaACLOperation_DESCRIBE,
		"CLUSTER_ACTION":            KafkaACLOperation_CLUSTERACTION,
		"DESCRIBE_CONFIGS":          KafkaACLOperation_DESCRIBECONFIGS,
		"ALTER_CONFIGS":             KafkaACLOperation_ALTERCONFIGS,
		"IDEMPOTENT_WRITE":          KafkaACLOperation_IDEMPOTENTWRITE,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["KafkaACLOperation_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl Operation string provided. input string: %s", in)
	}
	return s, nil
}

func (c KafkaACLOperation) GenerateACLMappingStructures(clusterName string, in *ACLMapping) *ACLMapping {
	temp := ACLMapping{}
	for k, v := range *in {
		switch k.Operation.(type) {
		case KafkaACLOperation:
			temp[k] = v
		case ShepherdOperationType:
			switch k.Operation {
			case ShepherdOperationType_PRODUCER:
				// Enable Write to Topic
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
			case ShepherdOperationType_TRANSACTIONAL_PRODUCER:
				temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, k.ResourceName, KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, k.ResourceName, KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
			case ShepherdOperationType_PRODUCER_IDEMPOTENCE:
				// Repeat of above for the corner case where the customer wants idempotence but not the transactional behavior
				temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, k.ResourceName, KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_IDEMPOTENTWRITE, k.Hostname)] = nil
			case ShepherdOperationType_CONSUMER:
				if k.ResourceType == KafkaResourceType_TOPIC {
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				}
				if k.ResourceType == KafkaResourceType_GROUP {
					temp[constructACLDetailsObject(KafkaResourceType_GROUP, k.ResourceName, KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				}
			// case ShepherdOperationType_CONSUMER_GROUP:
			case ShepherdOperationType_STREAM_READ:
				if k.ResourceType == KafkaResourceType_TOPIC {
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
					if gName := v.(NVPairs)[KafkaResourceType_GROUP.GetACLResourceString()]; gName != "" {
						temp[constructACLDetailsObject(KafkaResourceType_GROUP, gName, KafkaACLPatternType_PREFIXED, k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
					}
				}
				// if k.ResourceType == KafkaResourceType_GROUP {
				// 	temp[constructACLDetailsObject(KafkaResourceType_GROUP, k.ResourceName, KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				// }
			case ShepherdOperationType_STREAM_WRITE:
				// Enable Write to Topic
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				if gName := v.(NVPairs)[KafkaResourceType_GROUP.GetACLResourceString()]; gName != "" {
					temp[constructACLDetailsObject(KafkaResourceType_GROUP, gName, KafkaACLPatternType_PREFIXED, k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				}
			case ShepherdOperationType_SOURCE_CONNECTOR:
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				if cName := v.(NVPairs)[KafkaResourceType_CONNECTOR.GetACLResourceString()]; cName != "" {
					temp[constructACLDetailsObject(KafkaResourceType_GROUP, fmt.Sprintf("connect-%s", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				}
			case ShepherdOperationType_SINK_CONNECTOR:
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName), k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				if cName := v.(NVPairs)[KafkaResourceType_CONNECTOR.GetACLResourceString()]; cName != "" {
					temp[constructACLDetailsObject(KafkaResourceType_GROUP, fmt.Sprintf("connect-%s", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				}
			//  TODO: Implement use case for KSQL
			case ShepherdOperationType_KSQL_READ:
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, k.PatternType, k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, k.PatternType, k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				if cName := v.(NVPairs)[KafkaResourceType_KSQL_CLUSTER.GetACLResourceString()]; cName != "" {
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%s", cName), KafkaACLPatternType_PREFIXED, k.Principal, KafkaACLOperation_ALL, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_GROUP, fmt.Sprintf("_confluent-ksql-%s", cName), KafkaACLPatternType_PREFIXED, k.Principal, KafkaACLOperation_ALL, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("%sksql_processing_log", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_ALL, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%s_command_topic", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%s_command_topic", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, fmt.Sprintf("ksql-%s", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				}
				temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_DESCRIBECONFIGS, k.Hostname)] = nil
			case ShepherdOperationType_KSQL_WRITE:
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, k.PatternType, k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, k.PatternType, k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				if cName := v.(NVPairs)[KafkaResourceType_KSQL_CLUSTER.GetACLResourceString()]; cName != "" {
					// temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("ksql-%s", cName), KafkaACLPatternType_PREFIXED, k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%s", cName), KafkaACLPatternType_PREFIXED, k.Principal, KafkaACLOperation_ALL, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_GROUP, fmt.Sprintf("_confluent-ksql-%s", cName), KafkaACLPatternType_PREFIXED, k.Principal, KafkaACLOperation_ALL, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("%sksql_processing_log", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_ALL, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%s_command_topic", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%s_command_topic", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
					temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, fmt.Sprintf("ksql-%s", cName), KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				}
				temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, k.Principal, KafkaACLOperation_DESCRIBECONFIGS, k.Hostname)] = nil
			default:
				// temp[k] = nil
				logger.Warnw(" Could not generate Kafka ACL Mappings for the provided ACL Map. Marking as failed.",
					"Principal", k.Principal,
					"Resource Type", k.ResourceType.GetACLResourceString(),
					"Resource Value", k.ResourceName,
					"Hostname", k.Hostname,
					"ACL Operation Wanted", k.Operation.String())
			}
		}
	}
	return &temp
}

func determinePatternType(topicName string) KafkaACLPatternType {
	if topicName == "*" {
		return KafkaACLPatternType_LITERAL
		// return KafkaACLPatternType_UNKNOWN
	}
	if strings.HasSuffix(topicName, fmt.Sprintf("%s*", SpdCore.Configs.ConfigRoot.ShepherdCoreConfig.SeperatorToken)) {
		return KafkaACLPatternType_PREFIXED
		// return KafkaACLPatternType_UNKNOWN
	}
	// return KafkaACLPatternType_UNKNOWN
	return KafkaACLPatternType_LITERAL
}
