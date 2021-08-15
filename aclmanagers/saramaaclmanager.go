package aclmanagers

import (
	"sync"

	"github.com/Shopify/sarama"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

type SaramaACLManagerImpl struct {
	ACLManagerBaseImpl
}

var (
	SaramaACLManager ACLManager = SaramaACLManagerImpl{}
)

var (
	aclMappings *ksengine.ACLMapping
)

/*
	The cluster name is the only known entity for the Engine. The Kafka Connection manager
	operates and maintains all the Kafka Connections. This function is a convenience function
	to find the ConnectionObject and type cast it as a Sarama Cluster Admin connection and use
	it to execute any functionality in this module.
*/
func (t SaramaACLManagerImpl) getSaramaConnectionObject(clusterName string) *sarama.ClusterAdmin {
	return kafkamanagers.Connections[kafkamanagers.KafkaConnectionsKey{ClusterName: clusterName}].Connection.(*kafkamanagers.SaramaConnection).SCA
}

func (s SaramaACLManagerImpl) CreateACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Create Cluster ACLs", "=", 80)
	createSet := s.FindNonExistentACLsInCluster(aclMappings, ksengine.KafkaACLOperation_ANY)
	s.createACLs(clusterName, createSet, dryRun)
}

func (s SaramaACLManagerImpl) createACLs(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	var wg sync.WaitGroup

	f := func(key ksengine.ACLDetails, val interface{}) {
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
			err := (*s.getSaramaConnectionObject(clusterName)).CreateACL(r, a)
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

func (s SaramaACLManagerImpl) DeleteProvisionedACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Delete Config ACLs", "=", 80)
	deleteSet := s.FindProvisionedACLsInCluster(aclMappings, ksengine.KafkaACLOperation_ANY)
	s.deleteACLs(clusterName, deleteSet, dryRun)
}

func (s SaramaACLManagerImpl) DeleteUnknownACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Delete Unknown ACLs", "=", 80)
	deleteSet := s.FindNonExistentACLsInConfig(aclMappings, ksengine.KafkaACLOperation_ANY)
	s.deleteACLs(clusterName, deleteSet, dryRun)
}

func (s SaramaACLManagerImpl) deleteACLs(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	var wg sync.WaitGroup
	f := func(key ksengine.ACLDetails, val interface{}) {
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
			match, err := (*s.getSaramaConnectionObject(clusterName)).DeleteACL(filter, false)
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

func (s SaramaACLManagerImpl) ListClusterACL(clusterName string) {
	acls := s.gatherClusterACLs(clusterName)
	var wg sync.WaitGroup
	lock := &sync.Mutex{}
	wg.Add(len(*acls))
	aclMappings = &ksengine.ACLMapping{}
	for _, v := range *acls {
		go s.mapSaramaToKafkaACL(v, aclMappings, &wg, lock)
	}
	wg.Wait()
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
	for k := range *aclMappings {
		perm := ksengine.KafkaACLPermissionType_ALLOW
		logger.Infow("Mapped Kafka ACL Details (Only Alllow Mappings are filtered)",
			"Resource Type", k.ResourceType.String(),
			"Resource Name", k.ResourceName,
			"Resource Pattern Type", k.PatternType.String(),
			"Principal Name", k.Principal,
			"Host", k.Hostname,
			"ACL Operation", k.Operation.String(),
			"Permission Type", perm.String(),
		)
	}
}

func (s SaramaACLManagerImpl) mapSaramaToKafkaACL(in sarama.ResourceAcls, mapping *ksengine.ACLMapping, wg *sync.WaitGroup, mtx *sync.Mutex) {
	defer wg.Done()

	for _, v := range in.Acls {
		if v.PermissionType == sarama.AclPermissionAllow {
			mtx.Lock()
			mapping.Append(ksengine.ACLDetails{
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

func (s SaramaACLManagerImpl) gatherClusterACLs(clusterName string) *[]sarama.ResourceAcls {
	filter := sarama.AclFilter{
		ResourcePatternTypeFilter: sarama.AclPatternAny,
		ResourceType:              sarama.AclResourceAny,
		PermissionType:            sarama.AclPermissionAny,
		Operation:                 sarama.AclOperationAny,
		Version:                   1,
	}
	acls, err := (*s.getSaramaConnectionObject(clusterName)).ListAcls(filter)
	if err != nil {
		logger.Fatalw("Failed to list Kafka Cluster ACLs. Cannot proceed without the correct ACLs.")
	}
	return &acls
}
