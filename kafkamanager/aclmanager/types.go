package aclmanager

// Any ACL Manager will need to implement this interface.
type ACLManager interface {
	// GetACLListFromKafkaCluster()
	// // This function takes in the input and provides all the necessary permissions
	// // to the principal on a specific topic for data production.
	// SetupProducerACLs(res *ACLResourceBundleRequest)
	// SetupConsumerACLs(res *ACLResourceBundleRequest)
	// SetupSourceConnectorACLs(res *ACLResourceBundleRequest)
	// SetupSinkConnectorACLs(res *ACLResourceBundleRequest)
	// // validateResourceBundle()
}
