package aclmanager

import (
	ksinternal "shepherd/internal"
	kafkamanager "shepherd/kafkamanager"
	ksmisc "shepherd/misc"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

var (
	aclMappings    ksinternal.ACLMapping = make(ksinternal.ACLMapping)
	sca            *sarama.ClusterAdmin
	logger         *zap.SugaredLogger
	lastUpdateTime int64
)

func init() {
	temp := kafkamanager.GetAdminConnection().(sarama.ClusterAdmin)
	sca = &temp
	logger = ksinternal.GetLogger()
	lastUpdateTime = time.Now().Unix()
	RefreshClusterACLMetadata(true)
}

func RefreshClusterACLMetadata(forceRefresh bool) {
	if time.Now().Unix()-lastUpdateTime > 30 || forceRefresh {
		aclMappings = getACLListInKafkaACLFormat()
	}
}

func ExecuteRequests(requestType ACLManagementType) {
	RefreshClusterACLMetadata(false)
	switch requestType {
	case ACLManagementType_LIST_CLUSTER_ACL:
		ksmisc.DottedLineOutput("List Cluster ACLs", "=", 80)
		printClusterACLDetails()
		ksmisc.DottedLineOutput("", "=", 80)
		return
	case ACLManagementType_LIST_CONFIG_ACL:
		ksmisc.DottedLineOutput("List Config ACLs", "=", 80)
		printConfigACLDetails()
		ksmisc.DottedLineOutput("", "=", 80)
		return
	case ACLManagementType_CREATE_ACL:
		createStream := ksinternal.FindNonExistentACLsInCluster(&aclMappings, ksinternal.KafkaACLOperation_ANY)
		createACLs(createStream)
	case ACLManagementType_DELETE_UNKNOWN_ACL:
		deleteStream := ksinternal.FindNonExistentACLsInConfig(&aclMappings, ksinternal.KafkaACLOperation_ANY)
		deleteACLs(deleteStream)
	default:
		logger.Errorw("Wrong Execution Mode selected.",
			"Mode Provided", requestType.String())
	}
}

func createACLs(in ksinternal.ACLStreamChannels) {
	runLoop := true
	var wg sync.WaitGroup
	f := func(key ksinternal.ACLDetails, val interface{}) {
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
		if ksinternal.DryRun {
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
			err := (*sca).CreateACL(r, a)
			if err != nil {
				logger.Warnw("Was not able to create the ACL.",
					"Resource Details", r.ResourceName,
					"ACL Type", a.Operation,
					"Error", err)
			} else {
				logger.Infow("Successfully created ACL.",
					"Resource Details", r.ResourceName,
					"ACL Type", a.Operation,
					"Error", err)
			}
		}
	}
	for runLoop {
		select {
		case out := <-in.SChannel:
			for k, v := range out {
				// Do what is need for acl execution
				wg.Add(1)
				go f(k, v)
			}
		case <-in.FChannel:
			// Do Nothing
		case <-in.Finished:
			runLoop = false
			wg.Wait()
		}
	}

}

func deleteACLs(in ksinternal.ACLStreamChannels) {
	runLoop := true
	var wg sync.WaitGroup
	f := func(key ksinternal.ACLDetails, val interface{}) {
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
		if ksinternal.DryRun {
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
			match, err := (*sca).DeleteACL(filter, false)
			if err != nil {
				logger.Warnw("Was not able to create the ACL.",
					"Resource Details", filter.ResourceName,
					"ACL Operation Type", filter.Operation,
					"Error", err)
			} else {
				logger.Infow("Successfully created ACL.",
					"Resource Details", filter.ResourceName,
					"ACL Operation Type", filter.Operation,
					"Matched Object Resource Details", match)
			}
		}
	}
	for runLoop {
		select {
		case out := <-in.SChannel:
			for k, v := range out {
				// Do what is need for acl execution
				wg.Add(1)
				go f(k, v)
			}
		case <-in.FChannel:
			// Do Nothing
		case <-in.Finished:
			runLoop = false
			wg.Wait()
		}
	}

}

func getACLListInKafkaACLFormat() ksinternal.ACLMapping {
	acls := listKafkaACLs()
	var wg sync.WaitGroup
	wg.Add(len(*acls))
	lock := &sync.Mutex{}
	for _, v := range *acls {
		mapACLDetails(v, aclMappings, &wg, lock)
	}
	wg.Wait()
	return aclMappings
}

func mapACLDetails(in sarama.ResourceAcls, mapping ksinternal.ACLMapping, wg *sync.WaitGroup, lock *sync.Mutex) {
	defer wg.Done()

	for _, v := range in.Acls {
		if v.PermissionType == sarama.AclPermissionAllow {
			lock.Lock()
			mapping.Append(ksinternal.ACLDetails{
				ResourceType: sarama2KafkaResourceTypeConversion[in.Resource.ResourceType],
				ResourceName: in.Resource.ResourceName,
				PatternType:  correctResourcePatternType(in.Resource.ResourcePatternType, in.Resource.ResourceName),
				Principal:    v.Principal,
				Operation:    sarama2KafkaACLOperationConversion[v.Operation],
				Hostname:     v.Host,
			}, nil)
			lock.Unlock()
		}
	}
}

func correctResourcePatternType(pat sarama.AclResourcePatternType, resourceName string) ksinternal.KafkaACLPatternType {
	if pat == sarama.AclPatternUnknown {
		if resourceName == "*" {
			return ksinternal.KafkaACLPatternType_LITERAL
		}

		if strings.HasSuffix(resourceName, "*") {
			return ksinternal.KafkaACLPatternType_PREFIXED
		}
	}

	return sarama2KafkaPatternTypeConversion[pat]
}

func listKafkaACLs() *[]sarama.ResourceAcls {
	filter := sarama.AclFilter{
		ResourceType:              sarama.AclResourceAny,
		ResourcePatternTypeFilter: sarama.AclPatternAny,
		PermissionType:            sarama.AclPermissionAny,
		Operation:                 sarama.AclOperationAny,
		Version:                   1,
	}
	acls, err := (*sca).ListAcls(filter)
	if err != nil {
		logger.Fatalw("Failed to list Kafka Cluster ACLs. Cannot proceed without the correct ACLs.")
	}
	return &acls
}

func printClusterACLDetails() {
	acls := listKafkaACLs()
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
	for k := range aclMappings {
		perm := ksinternal.KafkaACLPermissionType_ALLOW
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

func printConfigACLDetails() {
	perm := ksinternal.KafkaACLPermissionType_ALLOW
	for k := range *ksinternal.ACLList {
		logger.Infow("Config ACL Mapping Details",
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

// rsc := sarama.Resource{
// 	ResourceType:        sarama.AclResourceTopic,
// 	ResourceName:        in.TopicName,
// 	ResourcePatternType: sarama.AclPatternAny,
// }
// var acl []sarama.Acl

// switch *in.Operation {
// case ksinternal.ShepherdClientType_PRODUCER:
// 	acl = []sarama.Acl{
// 		{
// 			Principal:      in.Principal,
// 			Operation:      sarama.AclOperationWrite,
// 			PermissionType: sarama.AclPermissionAllow,
// 		},
// 		{
// 			Principal:      in.Principal,
// 			Operation:      sarama.AclOperationDescribe,
// 			PermissionType: sarama.AclPermissionAllow,
// 		},
// 	}
