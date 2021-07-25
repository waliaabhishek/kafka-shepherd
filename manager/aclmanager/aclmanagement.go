package aclmanager

import (
	"fmt"
	ksinternal "shepherd/internal"
	kafkamanager "shepherd/manager"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

var AclManager ACLManager
var logger *zap.SugaredLogger

type kafkaACLManager struct {
	confMap *ksinternal.ConfigurationMaps
	sca     *sarama.ClusterAdmin
}

func init() {
	AclManager = &kafkaACLManager{
		confMap: ksinternal.GetConfigMaps(),
		sca:     kafkamanager.SetupAdminConnection(),
	}

	logger = ksinternal.GetLogger()
}

func (kam *kafkaACLManager) GetACLListFromKafkaCluster() {
	filter := sarama.AclFilter{
		ResourceType:              sarama.AclResourceTopic,
		ResourcePatternTypeFilter: sarama.AclPatternAny,
		PermissionType:            sarama.AclPermissionAny,
		Operation:                 sarama.AclOperationAny,
		Version:                   1,
	}
	acls, _ := (*kam.sca).ListAcls(filter)
	var wg sync.WaitGroup
	wg.Add(len(acls))
	for _, v := range acls {
		go printACLDetails(v, &wg)
	}

	wg.Wait()
}

func printACLDetails(in sarama.ResourceAcls, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, v := range in.Acls {
		logger.Debugw("ACL Details",
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

func (kam *kafkaACLManager) SetupProducerACLs(in *ACLResourceBundleRequest) {

	if in.Principal == nil {
		logger.Warnw("No Principal value is provided. Skipping ACL set up.",
			"ACL Resource Bundle:", in)
		return
	}

	if in.TopicName == nil {
		logger.Warnw("No TopicNames provided. Skipping ACL set up.",
			"ACL Resource Bundle:", in)
		return
	}

	if in.Operation == nil {
		logger.Warnw("No Operation Type provided. Skipping ACL set up.",
			"ACL Resource Bundle:", in)
		return
	}

	var wg sync.WaitGroup

	for _, p := range *in.Principal {
		for _, t := range *in.TopicName {
			if in.Hostname != nil {
				for _, h := range *in.Hostname {
					wg.Add(1)
					go kam.executeACLRequest(ACLExecutionRequest{
						Principal: p,
						TopicName: t,
						HostName:  h,
						Operation: in.Operation,
					}, &wg)
				}
			} else {
				wg.Add(1)
				go kam.executeACLRequest(ACLExecutionRequest{
					Principal: p,
					TopicName: t,
					HostName:  "",
					Operation: in.Operation,
				}, &wg)
			}
		}
	}
	wg.Wait()
}

func (kam *kafkaACLManager) executeACLRequest(in ACLExecutionRequest, wg *sync.WaitGroup) {
	defer wg.Done()
	a, r := in.renderACLExecutionObjects()
	for _, acl := range a {
		err := (*kam.sca).CreateACL(r, acl)
		if err != nil {
			logger.Warnw("Was not able to create the ACL.",
				"Resource Details", r,
				"ACL Type", acl,
				"Error", err)
		} else {
			logger.Infow("Successfully created ACL.",
				"Resource Details", r,
				"ACL Type", acl,
				"Error", err)
		}
	}
}

func (in *ACLExecutionRequest) renderACLExecutionObjects() ([]sarama.Acl, sarama.Resource) {
	rsc := sarama.Resource{
		ResourceType:        sarama.AclResourceTopic,
		ResourceName:        in.TopicName,
		ResourcePatternType: sarama.AclPatternAny,
	}
	var acl []sarama.Acl

	switch *in.Operation {
	case ksinternal.PRODUCER:
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
	case ksinternal.CONSUMER:
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
	case ksinternal.SOURCE_CONNECTOR:
	case ksinternal.SINK_CONNECTOR:
	case ksinternal.STREAM_READ:
	case ksinternal.STREAM_WRITE:
	case ksinternal.KSQL:
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
