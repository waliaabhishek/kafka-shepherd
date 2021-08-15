package engine

import (
	"fmt"
	"strings"
)

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

func (c KafkaACLOperation) generateACLMappingStructures(in *ACLMapping) *ACLMapping {
	temp := ACLMapping{}
	for k, v := range *in {
		switch k.Operation.(type) {
		case KafkaACLOperation:
			temp[k] = v
		case ShepherdClientType:
			switch k.Operation {
			case ShepherdClientType_PRODUCER, ShepherdClientType_STREAM_WRITE:
				// Enable Write to Topic
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
				// Enable Describe on Transactional ID
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
			case ShepherdClientType_TRANSACTIONAL_PRODUCER:
				temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, k.ResourceName, KafkaACLPatternType_LITERAL,
					// KafkaACLPatternType_UNKNOWN,
					k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
				temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, k.ResourceName,
					KafkaACLPatternType_LITERAL,
					// KafkaACLPatternType_UNKNOWN,
					k.Principal, KafkaACLOperation_IDEMPOTENTWRITE, k.Hostname)] = nil
				// Enable Idempotent Writes with Transaction ID
			case ShepherdClientType_PRODUCER_IDEMPOTENCE:
				// Repeat of above for the corner case where the customer wants idempotence but not the transactional behavior
				temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, k.ResourceName,
					KafkaACLPatternType_LITERAL,
					// KafkaACLPatternType_UNKNOWN,
					k.Principal, KafkaACLOperation_IDEMPOTENTWRITE, k.Hostname)] = nil
			case ShepherdClientType_CONSUMER, ShepherdClientType_STREAM_READ:
				// Enable Topic Read
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				// Enable topic Describe
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, KafkaACLOperation_DESCRIBE, k.Hostname)] = nil
			case ShepherdClientType_CONSUMER_GROUP:
				// Enable Consumer Group Functionalities
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, k.ResourceName,
					KafkaACLPatternType_LITERAL,
					// KafkaACLPatternType_UNKNOWN,
					k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
			case ShepherdClientType_SOURCE_CONNECTOR:
				// Enable Topic Read
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, KafkaACLOperation_WRITE, k.Hostname)] = nil
				if determinePatternType(k.ResourceName) == KafkaACLPatternType_LITERAL {
					//  TODO : I am still debating if a connector should be allowed to create a topic or not.
					// Personally i don't think topic creation should be allowed/tolerated at all via code bases
					// so that they are centrally managed in a toolkit like this
					// temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, v.(NVPairs)[KafkaResourceType_CLUSTER.String()], KafkaACLPatternType_LITERAL,
					// 	k.Principal, KafkaACLOperation_CREATE, k.Hostname)] = nil
					// sChannel <- temp
				}
			case ShepherdClientType_SINK_CONNECTOR:
				// Enable Topic Read
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				// Enable the connector to use consumer groups if they would want to
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, v.(NVPairs)[KafkaResourceType_GROUP.String()],
					// KafkaACLPatternType_LITERAL,
					KafkaACLPatternType_UNKNOWN,
					k.Principal, KafkaACLOperation_READ, k.Hostname)] = nil
				//  TODO : I am still debating if a Sink connector should be allowed to create a topic or not.
				// Personally I don't think topic creation should be allowed/tolerated at all via code bases
				// so that they are centrally managed in a toolkit like this
				// If topic name is literal then enable create on that topic name as well
				// temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, v.(NVPairs)[KafkaResourceType_CLUSTER.String()], KafkaACLPatternType_LITERAL,
				// 	k.Principal, KafkaACLOperation_CREATE, k.Hostname)] = nil
			//  TODO: Implement use case for KSQL
			// case ShepherdClientType_KSQL.String():
			default:
				// temp[k] = nil
				logger.Warnw(" Could not generate Kafka ACL Mappings for the provided ACL Map. Sending back to the failure channel.",
					"Principal", k.Principal,
					"Resource Type", k.ResourceType.String(),
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
