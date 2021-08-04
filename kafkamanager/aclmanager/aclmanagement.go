package aclmanager

import (
	"fmt"
	ksinternal "shepherd/internal"
	kafkamanager "shepherd/kafkamanager"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

var sca *sarama.ClusterAdmin
var logger *zap.SugaredLogger

func init() {
	temp := kafkamanager.GetAdminConnection().(sarama.ClusterAdmin)
	sca = &temp
	logger = ksinternal.GetLogger()
}

func GetACLListInKafkaACLFormat() ksinternal.ACLMapping {
	filter := sarama.AclFilter{
		ResourceType:              sarama.AclResourceTopic,
		ResourcePatternTypeFilter: sarama.AclPatternAny,
		PermissionType:            sarama.AclPermissionAny,
		Operation:                 sarama.AclOperationAny,
		Version:                   1,
	}
	acls, _ := (*sca).ListAcls(filter)
	aclMapping := ksinternal.ACLMapping{}
	var wg sync.WaitGroup
	wg.Add(len(acls))
	for _, v := range acls {
		go printACLDetails(v, aclMapping, &wg)
	}
	wg.Wait()
	return aclMapping
}

func printACLDetails(in sarama.ResourceAcls, mapping ksinternal.ACLMapping, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, v := range in.Acls {
		if v.PermissionType == sarama.AclPermissionAllow {
			logger.Debugw("ACL Details",
				"Resource Type", in.Resource.ResourceType.String(),
				"Resource Name", in.Resource.ResourceName,
				"Resource Pattern Type", in.Resource.ResourcePatternType.String(),
				"Principal Name", v.Principal,
				"Host", v.Host,
				"ACL Operation", v.Operation.String(),
				"Permission Type", v.PermissionType.String(),
			)
			mapping.Append(ksinternal.ACLDetails{
				ResourceType: sarama2ShepherdResourceTypeConversion[in.Resource.ResourceType],
				ResourceName: in.Resource.ResourceName,
				PatternType:  sarama2ShepherdPatternTypeConversion[in.Resource.ResourcePatternType],
				Principal:    v.Principal,
				Operation:    sarama2ShepherdACLOperationConversion[v.Operation],
				Hostname:     v.Host,
			}, nil)
		}
	}
}

// func (kam *kafkaACLManager) SetupProducerACLs(in *ACLResourceBundleRequest) {

// 	if in.Principal == nil {
// 		logger.Warnw("No Principal value is provided. Skipping ACL set up.",
// 			"ACL Resource Bundle:", in)
// 		return
// 	}

// 	if in.TopicName == nil {
// 		logger.Warnw("No TopicNames provided. Skipping ACL set up.",
// 			"ACL Resource Bundle:", in)
// 		return
// 	}

// 	if in.Operation == nil {
// 		logger.Warnw("No Operation Type provided. Skipping ACL set up.",
// 			"ACL Resource Bundle:", in)
// 		return
// 	}

// 	var wg sync.WaitGroup

// 	for _, p := range *in.Principal {
// 		for _, t := range *in.TopicName {
// 			if in.Hostname != nil {
// 				for _, h := range *in.Hostname {
// 					wg.Add(1)
// 					go kam.executeACLRequest(ACLExecutionRequest{
// 						Principal: p,
// 						TopicName: t,
// 						HostName:  h,
// 						Operation: in.Operation,
// 					}, &wg)
// 				}
// 			} else {
// 				wg.Add(1)
// 				go kam.executeACLRequest(ACLExecutionRequest{
// 					Principal: p,
// 					TopicName: t,
// 					HostName:  "",
// 					Operation: in.Operation,
// 				}, &wg)
// 			}
// 		}
// 	}
// 	wg.Wait()
// }

// func (kam *kafkaACLManager) executeACLRequest(in ACLExecutionRequest, wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	a, r := in.renderACLExecutionObjects()
// 	for _, acl := range a {
// 		err := (*kam.sca).CreateACL(r, acl)
// 		if err != nil {
// 			logger.Warnw("Was not able to create the ACL.",
// 				"Resource Details", r,
// 				"ACL Type", acl,
// 				"Error", err)
// 		} else {
// 			logger.Infow("Successfully created ACL.",
// 				"Resource Details", r,
// 				"ACL Type", acl,
// 				"Error", err)
// 		}
// 	}
// }

func renderACLExecutionObjects() ([]sarama.Acl, sarama.Resource) {

	rsc := sarama.Resource{
		ResourceType:        sarama.AclResourceTopic,
		ResourceName:        in.TopicName,
		ResourcePatternType: sarama.AclPatternAny,
	}
	var acl []sarama.Acl

	switch *in.Operation {
	case ksinternal.ShepherdClientType_PRODUCER:
		acl = []sarama.Acl{
			{
				Principal:      in.Principal,
				Operation:      sarama.AclOperationWrite,
				PermissionType: sarama.AclPermissionAllow,
			},
			{
				Principal:      in.Principal,
				Operation:      sarama.AclOperationDescribe,
				PermissionType: sarama.AclPermissionAllow,
			},
		}
	case ksinternal.ShepherdClientType_CONSUMER:
		acl = []sarama.Acl{
			{
				Principal:      in.Principal,
				Operation:      sarama.AclOperationRead,
				PermissionType: sarama.AclPermissionAllow,
			},
			{
				Principal:      in.Principal,
				Operation:      sarama.AclOperationDescribe,
				PermissionType: sarama.AclPermissionAllow,
			},
		}
	case ksinternal.ShepherdClientType_SOURCE_CONNECTOR:
	case ksinternal.ShepherdClientType_SINK_CONNECTOR:
	case ksinternal.ShepherdClientType_STREAM_READ:
	case ksinternal.ShepherdClientType_STREAM_WRITE:
	case ksinternal.ShepherdClientType_KSQL:
	}
	return acl, rsc
}

func (kam *kafkaACLManager) SetupConsumerACLs(res *ACLResourceBundleRequest) {
	fmt.Println("Setup the permissions for consumption")
}

func (kam *kafkaACLManager) SetupSourceConnectorACLs(res *ACLResourceBundleRequest) {
	fmt.Println("Setup the permissions for Source Connector")
}

func (kam *kafkaACLManager) SetupSinkConnectorACLs(res *ACLResourceBundleRequest) {
	fmt.Println("Setup the permissions for Sink Connector")
}
