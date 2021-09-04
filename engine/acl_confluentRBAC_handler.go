package engine

import (
	"fmt"
	"strings"
)

var ConfACLManager ShepherdACLManager = ConfluentRBACACLEngineImpl{}

type ConfluentRBACACLEngineImpl struct {
	ShepherdACLManagerBaseImpl
	ConfRBACType
}

type (
	ConfRBACType string
)

func (in ConfRBACType) String() string {
	return strings.ToUpper(strings.TrimSpace(in.String()))
}

func (c ConfRBACType) GetValue(in string) (ACLOperationsInterface, error) {
	return ConfRBACType(strings.ToUpper(strings.TrimSpace(c.String()))), nil
}

// This function will generate the mappings in the ConfRBACType internal structure type for all the mappings provided
// as the input `in` value. Whatever it is able to properly convert, those mappings will be added to the success
// map and the rest will be added to the failed map.
func (c ConfRBACType) generateACLMappingStructures(clusterName string, in *ACLMapping) *ACLMapping {
	temp := ACLMapping{}
	for k, v := range *in {
		value := make(NVPairs)
		switch k.Operation.(type) {
		case ConfRBACType:
			temp[k] = v
		case ShepherdClientType:
			switch k.Operation {
			case ShepherdClientType_PRODUCER:
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, ConfRBACType("DeveloperWrite"), k.Hostname)] = value
				if determinePatternType(k.ResourceName) == KafkaACLPatternType_LITERAL {
					value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "key"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "value"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
				}
			case ShepherdClientType_PRODUCER_IDEMPOTENCE:
				//  TODO: Not sure what to do for this one
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA-CLUSTER-NAME"
				temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType("DeveloperWrite"), k.Hostname)] = value
			case ShepherdClientType_TRANSACTIONAL_PRODUCER:
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, k.ResourceName, KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
			case ShepherdClientType_CONSUMER:
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, ConfRBACType("DeveloperRead"), k.Hostname)] = value
				if determinePatternType(k.ResourceName) == KafkaACLPatternType_LITERAL {
					value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "key"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType("DeveloperRead"), k.Hostname)] = value
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "value"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType("DeveloperRead"), k.Hostname)] = value
				}
			case ShepherdClientType_CONSUMER_GROUP:
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType("DeveloperRead"), k.Hostname)] = value
			case ShepherdClientType_SOURCE_CONNECTOR, ShepherdClientType_SINK_CONNECTOR:
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
				if determinePatternType(k.ResourceName) == KafkaACLPatternType_LITERAL {
					value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "key"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "value"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
				}
			case ShepherdClientType_STREAM_READ, ShepherdClientType_STREAM_WRITE:
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
				value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
			case ShepherdClientType_KSQL:
				value[KafkaResourceType_KAFKA_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				value[KafkaResourceType_KSQL_CLUSTER.GetACLResourceString()] = k.ResourceName
				temp[constructACLDetailsObject(KafkaResourceType_KSQL_CLUSTER, "ksql-cluster", KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType("DeveloperWrite"), k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, fmt.Sprintf("_confluent-ksql-%s", k.ResourceName), KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType("DeveloperRead"), k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("%sksql_processing_log", k.ResourceName), KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType("DeveloperRead"), k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%stransient", k.ResourceName), KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType("ResourceOwner"), k.Hostname)] = value
			}
		default:
			logger.Warnf("Conversion from %T type to %T type is not supported yet. The ACL mapping will be added to the Failed list.", k.Operation, c)
		}
	}
	return &temp
}
