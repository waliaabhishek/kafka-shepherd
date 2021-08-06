package aclmanager

import (
	ksinternal "shepherd/internal"
	kafkamanager "shepherd/kafkamanager"
	ksmisc "shepherd/misc"
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
	aclMappings = GetACLListInKafkaACLFormat()
	lastUpdateTime = time.Now().Unix()
}

func GetACLListInKafkaACLFormat() ksinternal.ACLMapping {
	acls := listKafkaACLs()
	var wg sync.WaitGroup
	wg.Add(len(*acls))
	for _, v := range *acls {
		mapACLDetails(v, aclMappings, &wg)
	}
	wg.Wait()
	return aclMappings
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

func mapACLDetails(in sarama.ResourceAcls, mapping ksinternal.ACLMapping, wg *sync.WaitGroup) {
	defer wg.Done()

	for _, v := range in.Acls {
		if v.PermissionType == sarama.AclPermissionAllow {
			mapping.Append(ksinternal.ACLDetails{
				ResourceType: sarama2KafkaResourceTypeConversion[in.Resource.ResourceType],
				ResourceName: in.Resource.ResourceName,
				PatternType:  sarama2KafkaPatternTypeConversion[in.Resource.ResourcePatternType],
				Principal:    v.Principal,
				Operation:    sarama2KafkaACLOperationConversion[v.Operation],
				Hostname:     v.Host,
			}, nil)
		}
	}
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
		logger.Infow("Mapped Kafka ACL Details (Only Alllow Mappings are filtered)",
			"Resource Type", k.ResourceType.String(),
			"Resource Name", k.ResourceName,
			"Resource Pattern Type", k.PatternType.String(),
			"Principal Name", k.Principal,
			"Host", k.Hostname,
			"ACL Operation", k.Operation.String(),
			"Permission Type", ksinternal.KafkaACLPermissionType_ALLOW,
		)
	}
}

func printConfigACLDetails() {
	for k := range *ksinternal.ACLList {
		logger.Infow("Mapped Kafka ACL Details",
			"Resource Type", k.ResourceType.String(),
			"Resource Name", k.ResourceName,
			"Resource Pattern Type", k.PatternType.String(),
			"Principal Name", k.Principal,
			"Host", k.Hostname,
			"ACL Operation", k.Operation.String(),
			"Permission Type", ksinternal.KafkaACLPermissionType_ALLOW,
		)
	}
}

func refreshACLMetadata(forceRefresh bool) {
	if time.Now().Unix()-lastUpdateTime > 30 || forceRefresh {
		aclMappings = GetACLListInKafkaACLFormat()
	}
}

func ExecuteRequests(requestType ACLManagementType) {
	refreshACLMetadata(false)
	switch requestType {
	case ACLManagementType_LIST_CLUSTER_ACL:
		ksmisc.DottedLineOutput("List Cluster ACLs", "=", 80)
		printClusterACLDetails()
		ksmisc.DottedLineOutput("", "=", 80)
	case ACLManagementType_LIST_CONFIG_ACL:
		ksmisc.DottedLineOutput("List Config ACLs", "=", 80)
		printConfigACLDetails()
		ksmisc.DottedLineOutput("", "=", 80)
	case ACLManagementType_CREATE_ACL:
		createStream := ksinternal.FindNonExistentACLsInCluster(&aclMappings, ksinternal.KafkaACLOperation_ANY)
		createACLs(createStream)
	case ACLManagementType_DELETE_ACL:
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
			ResourcePatternTypeFilter: kafka2SaramaPatternTypeConversion[key.PatternType],
			PermissionType:            sarama.AclPermissionAllow,
			Operation:                 kafka2SaramaACLOperationConversion[key.Operation],
			Version:                   1,
		}
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
