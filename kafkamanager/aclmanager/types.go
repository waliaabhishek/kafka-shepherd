package aclmanager

import (
	"fmt"
	ksinternal "shepherd/internal"
	"strings"
)

// Any ACL Manager will need to implement this interface.
type ACLManager interface {
	GetACLListFromKafkaCluster()
	// This function takes in the input and provides all the necessary permissions
	// to the principal on a specific topic for data production.
	SetupProducerACLs(res *ACLResourceBundleRequest)
	SetupConsumerACLs(res *ACLResourceBundleRequest)
	SetupSourceConnectorACLs(res *ACLResourceBundleRequest)
	SetupSinkConnectorACLs(res *ACLResourceBundleRequest)
	// validateResourceBundle()
}

type ACLResourceBundleRequest struct {
	Principal   *string
	TopicName   *[]string
	Hostname    *[]string
	Operation   *ksinternal.ClientType
	respChannel *chan ACLExecutionResponse
}

func (in ACLResourceBundleRequest) String() string {
	return fmt.Sprintf("Operation Type: [%s],\tPrincipals: [%s],\tTopicNames: [%s],\tHostNames: [%s]",
		in.Operation.String(), strings.Join(*in.Principal, ","), strings.Join(*in.TopicName, ","), strings.Join(*in.Hostname, ","))
}

type ACLExecutionRequest struct {
	Principal string
	TopicName string
	HostName  string
	Operation *ksinternal.ClientType
}

type ACLExecutionResponse struct {
	isSuccess      bool
	requestDetails ACLExecutionRequest
	err            error
}
