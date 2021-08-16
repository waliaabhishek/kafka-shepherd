package aclmanagers

import (
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
)

var (
	logger = ksengine.GetLogger()
)

// Any ACL Manager will need to implement this interface.
type ACLExecutionManager interface {
	CreateACL(clusterName string, in *ksengine.ACLMapping, dryRun bool)
	DeleteProvisionedACL(clusterName string, in *ksengine.ACLMapping, dryRun bool)
	DeleteUnknownACL(clusterName string, in *ksengine.ACLMapping, dryRun bool)
	ListClusterACL(clusterName string)
	ListConfigACL()
}

type ACLExecutionManagerBaseImpl struct{}

func (a ACLExecutionManagerBaseImpl) ListConfigACL() {
	perm := ksengine.KafkaACLPermissionType_ALLOW
	for k := range *ksengine.ShepherdACLList {
		logger.Infow("Config ACL Mapping Details",
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

/*
	Returns the Map of ACLMapping by comparing the output of ACL's present in the Kafka Cluster
	to the map of ACLMapping created by parsing the configurations. The response is the mapping
	that the Kafka connection will need to create as a baseline.
*/
func (a ACLExecutionManagerBaseImpl) FindNonExistentACLsInCluster(in *ksengine.ACLMapping, providedAclType ksengine.ACLOperationsInterface) *ksengine.ACLMapping {
	convertedList := ksengine.Shepherd.RenderACLMappings(ksengine.ShepherdACLList, providedAclType)
	return a.conditionalACLMapper(convertedList, in, false)
}

/*
	Returns ACLMapping construct for the ACLs that are provisioned in the Kafka cluster, but are
	not available as part of the configuration files.
*/
func (a ACLExecutionManagerBaseImpl) FindNonExistentACLsInConfig(in *ksengine.ACLMapping, providedAclType ksengine.ACLOperationsInterface) *ksengine.ACLMapping {
	convertedList := ksengine.Shepherd.RenderACLMappings(ksengine.ShepherdACLList, providedAclType)
	return a.conditionalACLMapper(in, convertedList, false)
}

/*
	Compares the list of ACLMappings provided from the Kafka Cluster to the ACLMappings that are
	part of the Configurations. It returns ACL Stream that is a part of the Shepherd Config and
	is already provisioned in the Kafka Cluster
*/
func (a ACLExecutionManagerBaseImpl) FindProvisionedACLsInCluster(in *ksengine.ACLMapping, providedAclType ksengine.ACLOperationsInterface) *ksengine.ACLMapping {
	convertedList := ksengine.Shepherd.RenderACLMappings(ksengine.ShepherdACLList, providedAclType)
	return a.conditionalACLMapper(convertedList, in, true)
}

func (a ACLExecutionManagerBaseImpl) conditionalACLMapper(inputACLs *ksengine.ACLMapping, findIn *ksengine.ACLMapping, presenceCheck bool) *ksengine.ACLMapping {
	ret := make(ksengine.ACLMapping)
	for k, v := range *inputACLs {
		if _, present := (*findIn)[k]; present == presenceCheck {
			if _, found := ret[k]; !found {
				ret[k] = v
			}
		}
	}
	return &ret
}
