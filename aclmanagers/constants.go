package aclmanagers

import (
	ksinternal "github.com/waliaabhishek/kafka-shepherd/engine"

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

var sarama2KafkaResourceTypeConversion map[sarama.AclResourceType]ksinternal.ACLResourceInterface = map[sarama.AclResourceType]ksinternal.ACLResourceInterface{
	sarama.AclResourceUnknown:         ksinternal.KafkaResourceType_UNKNOWN,
	sarama.AclResourceAny:             ksinternal.KafkaResourceType_ANY,
	sarama.AclResourceTopic:           ksinternal.KafkaResourceType_TOPIC,
	sarama.AclResourceGroup:           ksinternal.KafkaResourceType_GROUP,
	sarama.AclResourceCluster:         ksinternal.KafkaResourceType_CLUSTER,
	sarama.AclResourceTransactionalID: ksinternal.KafkaResourceType_TRANSACTIONALID,
	sarama.AclResourceDelegationToken: ksinternal.KafkaResourceType_RESOURCE_DELEGATION_TOKEN,
}

var kafka2SaramaResourceTypeConversion map[ksinternal.ACLResourceInterface]sarama.AclResourceType = map[ksinternal.ACLResourceInterface]sarama.AclResourceType{
	ksinternal.KafkaResourceType_UNKNOWN:                   sarama.AclResourceUnknown,
	ksinternal.KafkaResourceType_ANY:                       sarama.AclResourceAny,
	ksinternal.KafkaResourceType_TOPIC:                     sarama.AclResourceTopic,
	ksinternal.KafkaResourceType_GROUP:                     sarama.AclResourceGroup,
	ksinternal.KafkaResourceType_CLUSTER:                   sarama.AclResourceCluster,
	ksinternal.KafkaResourceType_TRANSACTIONALID:           sarama.AclResourceTransactionalID,
	ksinternal.KafkaResourceType_RESOURCE_DELEGATION_TOKEN: sarama.AclResourceDelegationToken,
}

var sarama2KafkaPatternTypeConversion map[sarama.AclResourcePatternType]ksinternal.KafkaACLPatternType = map[sarama.AclResourcePatternType]ksinternal.KafkaACLPatternType{
	sarama.AclPatternUnknown:  ksinternal.KafkaACLPatternType_UNKNOWN,
	sarama.AclPatternAny:      ksinternal.KafkaACLPatternType_ANY,
	sarama.AclPatternMatch:    ksinternal.KafkaACLPatternType_MATCH,
	sarama.AclPatternLiteral:  ksinternal.KafkaACLPatternType_LITERAL,
	sarama.AclPatternPrefixed: ksinternal.KafkaACLPatternType_PREFIXED,
}

var kafka2SaramaPatternTypeConversion map[ksinternal.KafkaACLPatternType]sarama.AclResourcePatternType = map[ksinternal.KafkaACLPatternType]sarama.AclResourcePatternType{
	ksinternal.KafkaACLPatternType_UNKNOWN:  sarama.AclPatternUnknown,
	ksinternal.KafkaACLPatternType_ANY:      sarama.AclPatternAny,
	ksinternal.KafkaACLPatternType_MATCH:    sarama.AclPatternMatch,
	ksinternal.KafkaACLPatternType_LITERAL:  sarama.AclPatternLiteral,
	ksinternal.KafkaACLPatternType_PREFIXED: sarama.AclPatternPrefixed,
}

var sarama2KafkaACLOperationConversion map[sarama.AclOperation]ksinternal.KafkaACLOperation = map[sarama.AclOperation]ksinternal.KafkaACLOperation{
	sarama.AclOperationUnknown:         ksinternal.KafkaACLOperation_UNKNOWN,
	sarama.AclOperationAny:             ksinternal.KafkaACLOperation_ANY,
	sarama.AclOperationAll:             ksinternal.KafkaACLOperation_ALL,
	sarama.AclOperationRead:            ksinternal.KafkaACLOperation_READ,
	sarama.AclOperationWrite:           ksinternal.KafkaACLOperation_WRITE,
	sarama.AclOperationCreate:          ksinternal.KafkaACLOperation_CREATE,
	sarama.AclOperationDelete:          ksinternal.KafkaACLOperation_DELETE,
	sarama.AclOperationAlter:           ksinternal.KafkaACLOperation_ALTER,
	sarama.AclOperationDescribe:        ksinternal.KafkaACLOperation_DESCRIBE,
	sarama.AclOperationClusterAction:   ksinternal.KafkaACLOperation_CLUSTERACTION,
	sarama.AclOperationDescribeConfigs: ksinternal.KafkaACLOperation_DESCRIBECONFIGS,
	sarama.AclOperationAlterConfigs:    ksinternal.KafkaACLOperation_ALTERCONFIGS,
	sarama.AclOperationIdempotentWrite: ksinternal.KafkaACLOperation_IDEMPOTENTWRITE,
}

var kafka2SaramaACLOperationConversion map[ksinternal.ACLOperationsInterface]sarama.AclOperation = map[ksinternal.ACLOperationsInterface]sarama.AclOperation{
	ksinternal.KafkaACLOperation_UNKNOWN:         sarama.AclOperationUnknown,
	ksinternal.KafkaACLOperation_ANY:             sarama.AclOperationAny,
	ksinternal.KafkaACLOperation_ALL:             sarama.AclOperationAll,
	ksinternal.KafkaACLOperation_READ:            sarama.AclOperationRead,
	ksinternal.KafkaACLOperation_WRITE:           sarama.AclOperationWrite,
	ksinternal.KafkaACLOperation_CREATE:          sarama.AclOperationCreate,
	ksinternal.KafkaACLOperation_DELETE:          sarama.AclOperationDelete,
	ksinternal.KafkaACLOperation_ALTER:           sarama.AclOperationAlter,
	ksinternal.KafkaACLOperation_DESCRIBE:        sarama.AclOperationDescribe,
	ksinternal.KafkaACLOperation_CLUSTERACTION:   sarama.AclOperationClusterAction,
	ksinternal.KafkaACLOperation_DESCRIBECONFIGS: sarama.AclOperationDescribeConfigs,
	ksinternal.KafkaACLOperation_ALTERCONFIGS:    sarama.AclOperationAlterConfigs,
	ksinternal.KafkaACLOperation_IDEMPOTENTWRITE: sarama.AclOperationIdempotentWrite,
}

var sarama2KafkaPermissionTypeConversion map[sarama.AclPermissionType]ksinternal.KafkaACLPermissionType = map[sarama.AclPermissionType]ksinternal.KafkaACLPermissionType{
	sarama.AclPermissionUnknown: ksinternal.KafkaACLPermissionType_UNKNOWN,
	sarama.AclPermissionAny:     ksinternal.KafkaACLPermissionType_ANY,
	sarama.AclPermissionDeny:    ksinternal.KafkaACLPermissionType_DENY,
	sarama.AclPermissionAllow:   ksinternal.KafkaACLPermissionType_ALLOW,
}

var kafka2SaramaPermissionTypeConversion map[ksinternal.KafkaACLPermissionType]sarama.AclPermissionType = map[ksinternal.KafkaACLPermissionType]sarama.AclPermissionType{
	ksinternal.KafkaACLPermissionType_UNKNOWN: sarama.AclPermissionUnknown,
	ksinternal.KafkaACLPermissionType_ANY:     sarama.AclPermissionAny,
	ksinternal.KafkaACLPermissionType_DENY:    sarama.AclPermissionDeny,
	ksinternal.KafkaACLPermissionType_ALLOW:   sarama.AclPermissionAllow,
}
