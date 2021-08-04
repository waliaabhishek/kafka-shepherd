package aclmanager

import (
	ksinternal "shepherd/internal"

	"github.com/Shopify/sarama"
)

var sarama2ShepherdResourceTypeConversion map[sarama.AclResourceType]ksinternal.ACLResourceInterface = map[sarama.AclResourceType]ksinternal.ACLResourceInterface{
	sarama.AclResourceUnknown:         ksinternal.KafkaResourceType_UNKNOWN,
	sarama.AclResourceAny:             ksinternal.KafkaResourceType_ANY,
	sarama.AclResourceTopic:           ksinternal.KafkaResourceType_TOPIC,
	sarama.AclResourceGroup:           ksinternal.KafkaResourceType_GROUP,
	sarama.AclResourceCluster:         ksinternal.KafkaResourceType_CLUSTER,
	sarama.AclResourceTransactionalID: ksinternal.KafkaResourceType_TRANSACTIONALID,
	sarama.AclResourceDelegationToken: ksinternal.KafkaResourceType_RESOURCE_DELEGATION_TOKEN,
}

var shepherd2SaramaResourceTypeConversion map[ksinternal.ACLResourceInterface]sarama.AclResourceType = map[ksinternal.ACLResourceInterface]sarama.AclResourceType{
	ksinternal.KafkaResourceType_UNKNOWN:                   sarama.AclResourceUnknown,
	ksinternal.KafkaResourceType_ANY:                       sarama.AclResourceAny,
	ksinternal.KafkaResourceType_TOPIC:                     sarama.AclResourceTopic,
	ksinternal.KafkaResourceType_GROUP:                     sarama.AclResourceGroup,
	ksinternal.KafkaResourceType_CLUSTER:                   sarama.AclResourceCluster,
	ksinternal.KafkaResourceType_TRANSACTIONALID:           sarama.AclResourceTransactionalID,
	ksinternal.KafkaResourceType_RESOURCE_DELEGATION_TOKEN: sarama.AclResourceDelegationToken,
}

var sarama2ShepherdPatternTypeConversion map[sarama.AclResourcePatternType]ksinternal.KafkaACLPatternType = map[sarama.AclResourcePatternType]ksinternal.KafkaACLPatternType{
	sarama.AclPatternUnknown:  ksinternal.KafkaACLPatternType_UNKNOWN,
	sarama.AclPatternAny:      ksinternal.KafkaACLPatternType_ANY,
	sarama.AclPatternMatch:    ksinternal.KafkaACLPatternType_MATCH,
	sarama.AclPatternLiteral:  ksinternal.KafkaACLPatternType_LITERAL,
	sarama.AclPatternPrefixed: ksinternal.KafkaACLPatternType_PREFIXED,
}

var shepherd2SaramaPatternTypeConversion map[ksinternal.KafkaACLPatternType]sarama.AclResourcePatternType = map[ksinternal.KafkaACLPatternType]sarama.AclResourcePatternType{
	ksinternal.KafkaACLPatternType_UNKNOWN:  sarama.AclPatternUnknown,
	ksinternal.KafkaACLPatternType_ANY:      sarama.AclPatternAny,
	ksinternal.KafkaACLPatternType_MATCH:    sarama.AclPatternMatch,
	ksinternal.KafkaACLPatternType_LITERAL:  sarama.AclPatternLiteral,
	ksinternal.KafkaACLPatternType_PREFIXED: sarama.AclPatternPrefixed,
}

var sarama2ShepherdACLOperationConversion map[sarama.AclOperation]ksinternal.KafkaACLOperation = map[sarama.AclOperation]ksinternal.KafkaACLOperation{
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

var shepherd2SaramaACLOperationConversion map[ksinternal.KafkaACLOperation]sarama.AclOperation = map[ksinternal.KafkaACLOperation]sarama.AclOperation{
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

var sarama2ShepherdPermissionTypeConversion map[sarama.AclPermissionType]ksinternal.KafkaACLPermissionType = map[sarama.AclPermissionType]ksinternal.KafkaACLPermissionType{
	sarama.AclPermissionUnknown: ksinternal.KafkaACLPermissionType_UNKNOWN,
	sarama.AclPermissionAny:     ksinternal.KafkaACLPermissionType_ANY,
	sarama.AclPermissionDeny:    ksinternal.KafkaACLPermissionType_DENY,
	sarama.AclPermissionAllow:   ksinternal.KafkaACLPermissionType_ALLOW,
}

var shepherd2SaramaPermissionTypeConversion map[ksinternal.KafkaACLPermissionType]sarama.AclPermissionType = map[ksinternal.KafkaACLPermissionType]sarama.AclPermissionType{
	ksinternal.KafkaACLPermissionType_UNKNOWN: sarama.AclPermissionUnknown,
	ksinternal.KafkaACLPermissionType_ANY:     sarama.AclPermissionAny,
	ksinternal.KafkaACLPermissionType_DENY:    sarama.AclPermissionDeny,
	ksinternal.KafkaACLPermissionType_ALLOW:   sarama.AclPermissionAllow,
}
