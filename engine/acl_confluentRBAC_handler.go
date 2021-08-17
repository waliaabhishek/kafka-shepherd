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
	ConfRBACType int
)

const (
	ConfRBACType_UNKNOWN ConfRBACType = iota
	ConfRBACType_DEV_READ
	ConfRBACType_DEV_WRITE
	ConfRBACType_DEV_MANAGE
	ConfRBACType_RES_OWNER
	ConfRBACType_OPERATOR
	ConfRBACType_AUDIT_ADMIN
	ConfRBACType_SECURITY_ADMIN
	ConfRBACType_USER_ADMIN
	ConfRBACType_CLUSTER_ADMIN
	ConfRBACType_SYSTEM_ADMIN
	ConfRBACType_SUPER_USER
)

func (in ConfRBACType) String() string {
	m := map[ConfRBACType]string{
		ConfRBACType_UNKNOWN:        "ConfRBACType_UNKNOWN",
		ConfRBACType_DEV_READ:       "DEVELOPER_READ",
		ConfRBACType_DEV_WRITE:      "DEVELOPER_WRITE",
		ConfRBACType_DEV_MANAGE:     "DEVELOPER_MANAGE",
		ConfRBACType_RES_OWNER:      "RESOURCE_OWNER",
		ConfRBACType_OPERATOR:       "OPERATOR",
		ConfRBACType_AUDIT_ADMIN:    "AUDIT_ADMIN",
		ConfRBACType_SECURITY_ADMIN: "SECURITY_ADMIN",
		ConfRBACType_USER_ADMIN:     "USER_ADMIN",
		ConfRBACType_CLUSTER_ADMIN:  "CLUSTER_ADMIN",
		ConfRBACType_SYSTEM_ADMIN:   "SYSTEM_ADMIN",
		ConfRBACType_SUPER_USER:     "SUPER_USER",
	}
	ret, present := m[in]
	if !present {
		ret = m[ConfRBACType_UNKNOWN]
	}
	return ret
}

func (c ConfRBACType) GetValue(in string) (ACLOperationsInterface, error) {
	m := map[string]ConfRBACType{
		"ConfRBACType_UNKNOWN": ConfRBACType_UNKNOWN,
		"DEVELOPER_READ":       ConfRBACType_DEV_READ,
		"DEVELOPER_WRITE":      ConfRBACType_DEV_WRITE,
		"DEVELOPER_MANAGE":     ConfRBACType_DEV_MANAGE,
		"RESOURCE_OWNER":       ConfRBACType_RES_OWNER,
		"OPERATOR":             ConfRBACType_OPERATOR,
		"AUDIT_ADMIN":          ConfRBACType_AUDIT_ADMIN,
		"SECURITY_ADMIN":       ConfRBACType_SECURITY_ADMIN,
		"USER_ADMIN":           ConfRBACType_USER_ADMIN,
		"CLUSTER_ADMIN":        ConfRBACType_CLUSTER_ADMIN,
		"SYSTEM_ADMIN":         ConfRBACType_SYSTEM_ADMIN,
		"SUPER_USER":           ConfRBACType_SUPER_USER,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["ConfRBACType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
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
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, ConfRBACType_DEV_WRITE, k.Hostname)] = value
				if determinePatternType(k.ResourceName) == KafkaACLPatternType_LITERAL {
					value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "key"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "value"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
				}
			case ShepherdClientType_PRODUCER_IDEMPOTENCE:
				//  TODO: Not sure what to do for this one
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA-CLUSTER-NAME"
				temp[constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType_DEV_WRITE, k.Hostname)] = value
			case ShepherdClientType_TRANSACTIONAL_PRODUCER:
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, k.ResourceName, KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
			case ShepherdClientType_CONSUMER:
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, ConfRBACType_DEV_READ, k.Hostname)] = value
				if determinePatternType(k.ResourceName) == KafkaACLPatternType_LITERAL {
					value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "key"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType_DEV_READ, k.Hostname)] = value
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "value"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType_DEV_READ, k.Hostname)] = value
				}
			case ShepherdClientType_CONSUMER_GROUP:
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType_DEV_READ, k.Hostname)] = value
			case ShepherdClientType_SOURCE_CONNECTOR, ShepherdClientType_SINK_CONNECTOR:
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, determinePatternType(k.ResourceName),
					k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
				if determinePatternType(k.ResourceName) == KafkaACLPatternType_LITERAL {
					value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "key"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
					temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, strings.Join([]string{k.ResourceName, "value"}, "-"), KafkaACLPatternType_LITERAL,
						k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
				}
			case ShepherdClientType_STREAM_READ, ShepherdClientType_STREAM_WRITE:
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
				value[KafkaResourceType_SCHEMA_REGISTRY_CLUSTER.GetACLResourceString()] = "repl::SCHEMA_REGISTRY_CLUSTER_ID"
				temp[constructACLDetailsObject(KafkaResourceType_SUBJECT, k.ResourceName, KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
			case ShepherdClientType_KSQL:
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "repl::KAFKA_CLUSTER_ID"
				value[KafkaResourceType_KSQL_CLUSTER.GetACLResourceString()] = k.ResourceName
				temp[constructACLDetailsObject(KafkaResourceType_KSQL_CLUSTER, "ksql-cluster", KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType_DEV_WRITE, k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_GROUP, fmt.Sprintf("_confluent-ksql-%s", k.ResourceName), KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType_DEV_READ, k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("%sksql_processing_log", k.ResourceName), KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType_DEV_READ, k.Hostname)] = value
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%stransient", k.ResourceName), KafkaACLPatternType_PREFIXED,
					k.Principal, ConfRBACType_RES_OWNER, k.Hostname)] = value
			}
		default:
			logger.Warnf("Conversion from %T type to %T type is not supported yet. The ACL mapping will be added to the Failed list.", k.Operation, c)
		}
	}
	return &temp
}
