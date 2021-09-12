package aclmanagers

import (
	"sync"

	"github.com/Shopify/sarama"
	engine "github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

type SaramaACLExecutionManagerImpl struct {
	ACLExecutionManagerBaseImpl
}

var (
	SaramaACLManager  ACLExecutionManager = SaramaACLExecutionManagerImpl{}
	saramaAclMappings *engine.ACLMapping

	sarama2KafkaResourceTypeConversion map[sarama.AclResourceType]engine.ACLResourceInterface = map[sarama.AclResourceType]engine.ACLResourceInterface{
		sarama.AclResourceUnknown:         engine.KafkaResourceType_UNKNOWN,
		sarama.AclResourceAny:             engine.KafkaResourceType_ANY,
		sarama.AclResourceTopic:           engine.KafkaResourceType_TOPIC,
		sarama.AclResourceGroup:           engine.KafkaResourceType_GROUP,
		sarama.AclResourceCluster:         engine.KafkaResourceType_CLUSTER,
		sarama.AclResourceTransactionalID: engine.KafkaResourceType_TRANSACTIONALID,
		sarama.AclResourceDelegationToken: engine.KafkaResourceType_RESOURCE_DELEGATION_TOKEN,
	}

	kafka2SaramaResourceTypeConversion map[engine.ACLResourceInterface]sarama.AclResourceType = map[engine.ACLResourceInterface]sarama.AclResourceType{
		engine.KafkaResourceType_UNKNOWN:                   sarama.AclResourceUnknown,
		engine.KafkaResourceType_ANY:                       sarama.AclResourceAny,
		engine.KafkaResourceType_TOPIC:                     sarama.AclResourceTopic,
		engine.KafkaResourceType_GROUP:                     sarama.AclResourceGroup,
		engine.KafkaResourceType_CLUSTER:                   sarama.AclResourceCluster,
		engine.KafkaResourceType_TRANSACTIONALID:           sarama.AclResourceTransactionalID,
		engine.KafkaResourceType_RESOURCE_DELEGATION_TOKEN: sarama.AclResourceDelegationToken,
	}

	sarama2KafkaPatternTypeConversion map[sarama.AclResourcePatternType]engine.KafkaACLPatternType = map[sarama.AclResourcePatternType]engine.KafkaACLPatternType{
		sarama.AclPatternUnknown:  engine.KafkaACLPatternType_UNKNOWN,
		sarama.AclPatternAny:      engine.KafkaACLPatternType_ANY,
		sarama.AclPatternMatch:    engine.KafkaACLPatternType_MATCH,
		sarama.AclPatternLiteral:  engine.KafkaACLPatternType_LITERAL,
		sarama.AclPatternPrefixed: engine.KafkaACLPatternType_PREFIXED,
	}

	kafka2SaramaPatternTypeConversion map[engine.ACLPatternInterface]sarama.AclResourcePatternType = map[engine.ACLPatternInterface]sarama.AclResourcePatternType{
		engine.KafkaACLPatternType_UNKNOWN:  sarama.AclPatternUnknown,
		engine.KafkaACLPatternType_ANY:      sarama.AclPatternAny,
		engine.KafkaACLPatternType_MATCH:    sarama.AclPatternMatch,
		engine.KafkaACLPatternType_LITERAL:  sarama.AclPatternLiteral,
		engine.KafkaACLPatternType_PREFIXED: sarama.AclPatternPrefixed,
	}

	sarama2KafkaACLOperationConversion map[sarama.AclOperation]engine.KafkaACLOperation = map[sarama.AclOperation]engine.KafkaACLOperation{
		sarama.AclOperationUnknown:         engine.KafkaACLOperation_UNKNOWN,
		sarama.AclOperationAny:             engine.KafkaACLOperation_ANY,
		sarama.AclOperationAll:             engine.KafkaACLOperation_ALL,
		sarama.AclOperationRead:            engine.KafkaACLOperation_READ,
		sarama.AclOperationWrite:           engine.KafkaACLOperation_WRITE,
		sarama.AclOperationCreate:          engine.KafkaACLOperation_CREATE,
		sarama.AclOperationDelete:          engine.KafkaACLOperation_DELETE,
		sarama.AclOperationAlter:           engine.KafkaACLOperation_ALTER,
		sarama.AclOperationDescribe:        engine.KafkaACLOperation_DESCRIBE,
		sarama.AclOperationClusterAction:   engine.KafkaACLOperation_CLUSTERACTION,
		sarama.AclOperationDescribeConfigs: engine.KafkaACLOperation_DESCRIBECONFIGS,
		sarama.AclOperationAlterConfigs:    engine.KafkaACLOperation_ALTERCONFIGS,
		sarama.AclOperationIdempotentWrite: engine.KafkaACLOperation_IDEMPOTENTWRITE,
	}

	kafka2SaramaACLOperationConversion map[engine.ACLOperationsInterface]sarama.AclOperation = map[engine.ACLOperationsInterface]sarama.AclOperation{
		engine.KafkaACLOperation_UNKNOWN:         sarama.AclOperationUnknown,
		engine.KafkaACLOperation_ANY:             sarama.AclOperationAny,
		engine.KafkaACLOperation_ALL:             sarama.AclOperationAll,
		engine.KafkaACLOperation_READ:            sarama.AclOperationRead,
		engine.KafkaACLOperation_WRITE:           sarama.AclOperationWrite,
		engine.KafkaACLOperation_CREATE:          sarama.AclOperationCreate,
		engine.KafkaACLOperation_DELETE:          sarama.AclOperationDelete,
		engine.KafkaACLOperation_ALTER:           sarama.AclOperationAlter,
		engine.KafkaACLOperation_DESCRIBE:        sarama.AclOperationDescribe,
		engine.KafkaACLOperation_CLUSTERACTION:   sarama.AclOperationClusterAction,
		engine.KafkaACLOperation_DESCRIBECONFIGS: sarama.AclOperationDescribeConfigs,
		engine.KafkaACLOperation_ALTERCONFIGS:    sarama.AclOperationAlterConfigs,
		engine.KafkaACLOperation_IDEMPOTENTWRITE: sarama.AclOperationIdempotentWrite,
	}

	sarama2KafkaPermissionTypeConversion map[sarama.AclPermissionType]engine.KafkaACLPermissionType = map[sarama.AclPermissionType]engine.KafkaACLPermissionType{
		sarama.AclPermissionUnknown: engine.KafkaACLPermissionType_UNKNOWN,
		sarama.AclPermissionAny:     engine.KafkaACLPermissionType_ANY,
		sarama.AclPermissionDeny:    engine.KafkaACLPermissionType_DENY,
		sarama.AclPermissionAllow:   engine.KafkaACLPermissionType_ALLOW,
	}

	kafka2SaramaPermissionTypeConversion map[engine.KafkaACLPermissionType]sarama.AclPermissionType = map[engine.KafkaACLPermissionType]sarama.AclPermissionType{
		engine.KafkaACLPermissionType_UNKNOWN: sarama.AclPermissionUnknown,
		engine.KafkaACLPermissionType_ANY:     sarama.AclPermissionAny,
		engine.KafkaACLPermissionType_DENY:    sarama.AclPermissionDeny,
		engine.KafkaACLPermissionType_ALLOW:   sarama.AclPermissionAllow,
	}
)

/*
	The cluster name is the only known entity for the Engine. The Kafka Connection manager
	operates and maintains all the Kafka Connections. This function is a convenience function
	to find the ConnectionObject and type cast it as a Sarama Cluster Admin connection and use
	it to execute any functionality in this module.
*/
func (t SaramaACLExecutionManagerImpl) getConnectionObject(clusterName string) *sarama.ClusterAdmin {
	return kafkamanagers.Connections[kafkamanagers.KafkaConnectionsKey{ClusterName: clusterName}].Connection.(*kafkamanagers.SaramaConnection).SCA
}

func (s SaramaACLExecutionManagerImpl) CreateACL(clusterName string, in *engine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Create Cluster ACLs", "=", 80)
	s.ListClusterACL(clusterName, false)
	createSet := s.FindNonExistentACLsInCluster(clusterName, saramaAclMappings, engine.KafkaACLOperation_ANY)
	s.createACLs(clusterName, createSet, dryRun)
}

func (s SaramaACLExecutionManagerImpl) createACLs(clusterName string, in *engine.ACLMapping, dryRun bool) {
	wg := new(sync.WaitGroup)

	f := func(key engine.ACLDetails, val interface{}) {
		defer wg.Done()
		r := sarama.Resource{
			ResourceType:        kafka2SaramaResourceTypeConversion[key.ResourceType],
			ResourceName:        key.ResourceName,
			ResourcePatternType: kafka2SaramaPatternTypeConversion[key.PatternType],
		}
		a := sarama.Acl{
			Principal:      key.Principal,
			Host:           key.Hostname,
			Operation:      kafka2SaramaACLOperationConversion[key.Operation],
			PermissionType: sarama.AclPermissionAllow,
		}
		if dryRun {
			logger.Infow("CreateACL Request",
				"Resource Type", r.ResourceType.String(),
				"Resource Name", r.ResourceName,
				"Resource Pattern Type", r.ResourcePatternType.String(),
				"Principal Name", a.Principal,
				"Host", a.Host,
				"ACL Operation", a.Operation.String(),
				"Permission Type", a.PermissionType.String(),
			)
		} else {
			err := (*s.getConnectionObject(clusterName)).CreateACL(r, a)
			if err != nil {
				logger.Warnw("Was not able to create the ACL.",
					"Resource Details", r.ResourceName,
					"ACL Type", a.Operation.String(),
					"Error", err)
			} else {
				logger.Infow("Successfully created ACL.",
					"Resource Details", r.ResourceName,
					"ACL Type", a.Operation.String(),
					"Error", err)
			}
		}
	}

	for k, v := range *in {
		wg.Add(1)
		go f(k, v)
	}
	wg.Wait()
}

func (s SaramaACLExecutionManagerImpl) DeleteProvisionedACL(clusterName string, in *engine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Delete Config ACLs", "=", 80)
	s.ListClusterACL(clusterName, false)
	deleteSet := s.FindProvisionedACLsInCluster(clusterName, saramaAclMappings, engine.KafkaACLOperation_ANY)
	s.deleteACLs(clusterName, deleteSet, dryRun)
}

func (s SaramaACLExecutionManagerImpl) DeleteUnknownACL(clusterName string, in *engine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Delete Unknown ACLs", "=", 80)
	s.ListClusterACL(clusterName, false)
	deleteSet := s.FindNonExistentACLsInConfig(clusterName, saramaAclMappings, engine.KafkaACLOperation_ANY)
	s.deleteACLs(clusterName, deleteSet, dryRun)
}

func (s SaramaACLExecutionManagerImpl) deleteACLs(clusterName string, in *engine.ACLMapping, dryRun bool) {
	wg := new(sync.WaitGroup)
	f := func(key engine.ACLDetails, val interface{}) {
		defer wg.Done()
		filter := sarama.AclFilter{
			ResourceType:              kafka2SaramaResourceTypeConversion[key.ResourceType],
			ResourceName:              &key.ResourceName,
			ResourcePatternTypeFilter: kafka2SaramaPatternTypeConversion[key.PatternType],
			PermissionType:            sarama.AclPermissionAllow,
			Principal:                 &key.Principal,
			Host:                      &key.Hostname,
			Operation:                 kafka2SaramaACLOperationConversion[key.Operation],
			Version:                   1,
		}
		if dryRun {
			logger.Infow("Delete ACL Request",
				"Resource Type", filter.ResourceType.String(),
				"Resource Name", filter.ResourceName,
				"Resource Pattern Type", filter.ResourcePatternTypeFilter.String(),
				"Principal Name", filter.Principal,
				"Host", filter.Host,
				"ACL Operation", filter.Operation.String(),
				"Permission Type", filter.PermissionType.String(),
			)
		} else {
			match, err := (*s.getConnectionObject(clusterName)).DeleteACL(filter, false)
			if err != nil {
				logger.Warnw("Was not able to create the ACL.",
					"Resource Details", filter.ResourceName,
					"ACL Operation Type", filter.Operation.String(),
					"Error", err)
			} else {
				logger.Infow("Successfully deleted ACL.",
					"Resource Details", filter.ResourceName,
					"ACL Operation Type", filter.Operation.String(),
					"Matched Object Resource Details", match)
			}
		}
	}
	for k, v := range *in {
		wg.Add(1)
		go f(k, v)
	}
	wg.Wait()
}

func (s SaramaACLExecutionManagerImpl) ListClusterACL(clusterName string, printOutput bool) {
	acls := s.gatherClusterACLs(clusterName)
	wg := new(sync.WaitGroup)
	lock := &sync.Mutex{}
	wg.Add(len(*acls))
	saramaAclMappings = &engine.ACLMapping{}
	for _, v := range *acls {
		go s.mapSaramaToKafkaACL(v, saramaAclMappings, wg, lock)
	}
	wg.Wait()
	if printOutput {
		for _, in := range *acls {
			for _, v := range in.Acls {
				logger.Infow("Sarama ACL Details",
					"Resource Type", in.Resource.ResourceType.String(),
					"Resource Name", in.Resource.ResourceName,
					"Resource Pattern Type", in.Resource.ResourcePatternType.String(),
					"Principal Name", v.Principal,
					"Host", v.Host,
					"ACL Operation", v.Operation.String(),
					"Permission Type", v.PermissionType.String(),
				)
			}
		}
		for k := range *saramaAclMappings {
			perm := engine.KafkaACLPermissionType_ALLOW
			logger.Infow("Mapped Kafka ACL Details (Only Alllow Mappings are filtered)",
				"Resource Type", k.ResourceType.GetACLResourceString(),
				"Resource Name", k.ResourceName,
				"Resource Pattern Type", k.PatternType.GetACLPatternString(),
				"Principal Name", k.Principal,
				"Host", k.Hostname,
				"ACL Operation", k.Operation.String(),
				"Permission Type", perm.String(),
			)
		}
	}
}

func (s SaramaACLExecutionManagerImpl) mapSaramaToKafkaACL(in sarama.ResourceAcls, mapping *engine.ACLMapping, wg *sync.WaitGroup, mtx *sync.Mutex) {
	defer wg.Done()

	for _, v := range in.Acls {
		if v.PermissionType == sarama.AclPermissionAllow {
			mtx.Lock()
			mapping.Append(engine.ACLDetails{
				ResourceType: sarama2KafkaResourceTypeConversion[in.Resource.ResourceType],
				ResourceName: in.Resource.ResourceName,
				PatternType:  sarama2KafkaPatternTypeConversion[in.Resource.ResourcePatternType],
				Principal:    v.Principal,
				Operation:    sarama2KafkaACLOperationConversion[v.Operation],
				Hostname:     v.Host,
			}, nil)
			mtx.Unlock()
		}
	}
}

func (s SaramaACLExecutionManagerImpl) gatherClusterACLs(clusterName string) *[]sarama.ResourceAcls {
	filter := sarama.AclFilter{
		ResourcePatternTypeFilter: sarama.AclPatternAny,
		ResourceType:              sarama.AclResourceAny,
		PermissionType:            sarama.AclPermissionAny,
		Operation:                 sarama.AclOperationAny,
		Version:                   1,
	}
	acls, err := (*s.getConnectionObject(clusterName)).ListAcls(filter)
	if err != nil {
		logger.Fatalw("Failed to list Kafka Cluster ACLs. Cannot proceed without the correct ACLs.")
	}
	return &acls
}

func (s SaramaACLExecutionManagerImpl) mapFromShepherdACL(clusterName string, in *engine.ACLMapping, out *engine.ACLMapping, failed *engine.ACLMapping) {
	panic("implementation not available") // TODO: implement
}

func (s SaramaACLExecutionManagerImpl) mapToShepherdACL(clusterName string, in *engine.ACLMapping, out *engine.ACLMapping, failed *engine.ACLMapping) {
	panic("implementation not available") // TODO: implement
}
