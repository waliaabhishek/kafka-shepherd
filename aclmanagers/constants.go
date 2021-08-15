package aclmanagers

import (
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"

	"github.com/Shopify/sarama"
)

type (
	ACLManagementType int
)

const (
	ACLManagementType_UNKNOWN ACLManagementType = iota
	ACLManagementType_CREATE_ACL
	ACLManagementType_DELETE_CONFIG_ACL
	ACLManagementType_DELETE_UNKNOWN_ACL
	ACLManagementType_LIST_CLUSTER_ACL
	ACLManagementType_LIST_CONFIG_ACL
)

func (a *ACLManagementType) String() string {
	mapping := map[ACLManagementType]string{
		ACLManagementType_UNKNOWN:            "UNKNOWN",
		ACLManagementType_CREATE_ACL:         "CREATE_ACL",
		ACLManagementType_DELETE_CONFIG_ACL:  "DELETE_CONFIG_ACL",
		ACLManagementType_DELETE_UNKNOWN_ACL: "DELETE_UNKNOWN_ACL",
		ACLManagementType_LIST_CLUSTER_ACL:   "LIST_CLUSTER_ACL",
		ACLManagementType_LIST_CONFIG_ACL:    "LIST_CONFIG_ACL",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[ACLManagementType_UNKNOWN]
	}
	return s
}

var sarama2KafkaResourceTypeConversion map[sarama.AclResourceType]ksengine.ACLResourceInterface = map[sarama.AclResourceType]ksengine.ACLResourceInterface{
	sarama.AclResourceUnknown:         ksengine.KafkaResourceType_UNKNOWN,
	sarama.AclResourceAny:             ksengine.KafkaResourceType_ANY,
	sarama.AclResourceTopic:           ksengine.KafkaResourceType_TOPIC,
	sarama.AclResourceGroup:           ksengine.KafkaResourceType_GROUP,
	sarama.AclResourceCluster:         ksengine.KafkaResourceType_CLUSTER,
	sarama.AclResourceTransactionalID: ksengine.KafkaResourceType_TRANSACTIONALID,
	sarama.AclResourceDelegationToken: ksengine.KafkaResourceType_RESOURCE_DELEGATION_TOKEN,
}

var kafka2SaramaResourceTypeConversion map[ksengine.ACLResourceInterface]sarama.AclResourceType = map[ksengine.ACLResourceInterface]sarama.AclResourceType{
	ksengine.KafkaResourceType_UNKNOWN:                   sarama.AclResourceUnknown,
	ksengine.KafkaResourceType_ANY:                       sarama.AclResourceAny,
	ksengine.KafkaResourceType_TOPIC:                     sarama.AclResourceTopic,
	ksengine.KafkaResourceType_GROUP:                     sarama.AclResourceGroup,
	ksengine.KafkaResourceType_CLUSTER:                   sarama.AclResourceCluster,
	ksengine.KafkaResourceType_TRANSACTIONALID:           sarama.AclResourceTransactionalID,
	ksengine.KafkaResourceType_RESOURCE_DELEGATION_TOKEN: sarama.AclResourceDelegationToken,
}

var sarama2KafkaPatternTypeConversion map[sarama.AclResourcePatternType]ksengine.KafkaACLPatternType = map[sarama.AclResourcePatternType]ksengine.KafkaACLPatternType{
	sarama.AclPatternUnknown:  ksengine.KafkaACLPatternType_UNKNOWN,
	sarama.AclPatternAny:      ksengine.KafkaACLPatternType_ANY,
	sarama.AclPatternMatch:    ksengine.KafkaACLPatternType_MATCH,
	sarama.AclPatternLiteral:  ksengine.KafkaACLPatternType_LITERAL,
	sarama.AclPatternPrefixed: ksengine.KafkaACLPatternType_PREFIXED,
}

var kafka2SaramaPatternTypeConversion map[ksengine.KafkaACLPatternType]sarama.AclResourcePatternType = map[ksengine.KafkaACLPatternType]sarama.AclResourcePatternType{
	ksengine.KafkaACLPatternType_UNKNOWN:  sarama.AclPatternUnknown,
	ksengine.KafkaACLPatternType_ANY:      sarama.AclPatternAny,
	ksengine.KafkaACLPatternType_MATCH:    sarama.AclPatternMatch,
	ksengine.KafkaACLPatternType_LITERAL:  sarama.AclPatternLiteral,
	ksengine.KafkaACLPatternType_PREFIXED: sarama.AclPatternPrefixed,
}

var sarama2KafkaACLOperationConversion map[sarama.AclOperation]ksengine.KafkaACLOperation = map[sarama.AclOperation]ksengine.KafkaACLOperation{
	sarama.AclOperationUnknown:         ksengine.KafkaACLOperation_UNKNOWN,
	sarama.AclOperationAny:             ksengine.KafkaACLOperation_ANY,
	sarama.AclOperationAll:             ksengine.KafkaACLOperation_ALL,
	sarama.AclOperationRead:            ksengine.KafkaACLOperation_READ,
	sarama.AclOperationWrite:           ksengine.KafkaACLOperation_WRITE,
	sarama.AclOperationCreate:          ksengine.KafkaACLOperation_CREATE,
	sarama.AclOperationDelete:          ksengine.KafkaACLOperation_DELETE,
	sarama.AclOperationAlter:           ksengine.KafkaACLOperation_ALTER,
	sarama.AclOperationDescribe:        ksengine.KafkaACLOperation_DESCRIBE,
	sarama.AclOperationClusterAction:   ksengine.KafkaACLOperation_CLUSTERACTION,
	sarama.AclOperationDescribeConfigs: ksengine.KafkaACLOperation_DESCRIBECONFIGS,
	sarama.AclOperationAlterConfigs:    ksengine.KafkaACLOperation_ALTERCONFIGS,
	sarama.AclOperationIdempotentWrite: ksengine.KafkaACLOperation_IDEMPOTENTWRITE,
}

var kafka2SaramaACLOperationConversion map[ksengine.ACLOperationsInterface]sarama.AclOperation = map[ksengine.ACLOperationsInterface]sarama.AclOperation{
	ksengine.KafkaACLOperation_UNKNOWN:         sarama.AclOperationUnknown,
	ksengine.KafkaACLOperation_ANY:             sarama.AclOperationAny,
	ksengine.KafkaACLOperation_ALL:             sarama.AclOperationAll,
	ksengine.KafkaACLOperation_READ:            sarama.AclOperationRead,
	ksengine.KafkaACLOperation_WRITE:           sarama.AclOperationWrite,
	ksengine.KafkaACLOperation_CREATE:          sarama.AclOperationCreate,
	ksengine.KafkaACLOperation_DELETE:          sarama.AclOperationDelete,
	ksengine.KafkaACLOperation_ALTER:           sarama.AclOperationAlter,
	ksengine.KafkaACLOperation_DESCRIBE:        sarama.AclOperationDescribe,
	ksengine.KafkaACLOperation_CLUSTERACTION:   sarama.AclOperationClusterAction,
	ksengine.KafkaACLOperation_DESCRIBECONFIGS: sarama.AclOperationDescribeConfigs,
	ksengine.KafkaACLOperation_ALTERCONFIGS:    sarama.AclOperationAlterConfigs,
	ksengine.KafkaACLOperation_IDEMPOTENTWRITE: sarama.AclOperationIdempotentWrite,
}

var sarama2KafkaPermissionTypeConversion map[sarama.AclPermissionType]ksengine.KafkaACLPermissionType = map[sarama.AclPermissionType]ksengine.KafkaACLPermissionType{
	sarama.AclPermissionUnknown: ksengine.KafkaACLPermissionType_UNKNOWN,
	sarama.AclPermissionAny:     ksengine.KafkaACLPermissionType_ANY,
	sarama.AclPermissionDeny:    ksengine.KafkaACLPermissionType_DENY,
	sarama.AclPermissionAllow:   ksengine.KafkaACLPermissionType_ALLOW,
}

var kafka2SaramaPermissionTypeConversion map[ksengine.KafkaACLPermissionType]sarama.AclPermissionType = map[ksengine.KafkaACLPermissionType]sarama.AclPermissionType{
	ksengine.KafkaACLPermissionType_UNKNOWN: sarama.AclPermissionUnknown,
	ksengine.KafkaACLPermissionType_ANY:     sarama.AclPermissionAny,
	ksengine.KafkaACLPermissionType_DENY:    sarama.AclPermissionDeny,
	ksengine.KafkaACLPermissionType_ALLOW:   sarama.AclPermissionAllow,
}
