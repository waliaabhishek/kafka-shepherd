package aclmanagers

import (
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
)

var (
	logger = ksengine.Shepherd.GetLogger()
)

/*
	The two maps below control which manager will be used for what kind of ACL's. It provides the appropriate
	ACL Manager Object as well as the ACLOperationInterface used for execution.
*/
var (
	aclController map[kafkamanagers.ACLType]ACLExecutionManager = map[kafkamanagers.ACLType]ACLExecutionManager{
		kafkamanagers.ACLType_KAFKA_ACLS:     SaramaACLManager,
		kafkamanagers.ACLType_CONFLUENT_RBAC: SaramaACLManager,
	}

	aclInterface map[kafkamanagers.ACLType]ksengine.ACLOperationsInterface = map[kafkamanagers.ACLType]ksengine.ACLOperationsInterface{
		kafkamanagers.ACLType_KAFKA_ACLS:     ksengine.KafkaACLOperation_UNKNOWN,
		kafkamanagers.ACLType_CONFLUENT_RBAC: ksengine.ConfRBACType_UNKNOWN,
	}
)

/*
	The user can logically derive these values themselves but the convenience method below provides the implemented
	values as an output. If its a forked repo, this is the method and the maps above are the ones to be changed.
*/
func GetACLControllerDetails(clusterName string) (ACLExecutionManager, ksengine.ACLOperationsInterface) {
	aclType := kafkamanagers.Connections[kafkamanagers.KafkaConnectionsKey{ClusterName: clusterName}].ACLType
	execMgr := aclController[aclType]
	execInterface := aclInterface[aclType]
	return execMgr, execInterface
}

// Any ACL Manager will need to implement this interface.
type ACLExecutionManager interface {
	CreateACL(clusterName string, in *ksengine.ACLMapping, dryRun bool)
	DeleteProvisionedACL(clusterName string, in *ksengine.ACLMapping, dryRun bool)
	DeleteUnknownACL(clusterName string, in *ksengine.ACLMapping, dryRun bool)
	ListClusterACL(clusterName string, printOutput bool)
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
func (a ACLExecutionManagerBaseImpl) FindNonExistentACLsInCluster(clusterName string, in *ksengine.ACLMapping, providedAclType ksengine.ACLOperationsInterface) *ksengine.ACLMapping {
	convertedList := ksengine.Shepherd.RenderACLMappings(clusterName, ksengine.ShepherdACLList, providedAclType)
	return a.conditionalACLMapper(convertedList, in, false)
}

/*
	Returns ACLMapping construct for the ACLs that are provisioned in the Kafka cluster, but are
	not available as part of the configuration files.
*/
func (a ACLExecutionManagerBaseImpl) FindNonExistentACLsInConfig(clusterName string, in *ksengine.ACLMapping, providedAclType ksengine.ACLOperationsInterface) *ksengine.ACLMapping {
	convertedList := ksengine.Shepherd.RenderACLMappings(clusterName, ksengine.ShepherdACLList, providedAclType)
	return a.conditionalACLMapper(in, convertedList, false)
}

/*
	Compares the list of ACLMappings provided from the Kafka Cluster to the ACLMappings that are
	part of the Configurations. It returns ACL Stream that is a part of the Shepherd Config and
	is already provisioned in the Kafka Cluster
*/
func (a ACLExecutionManagerBaseImpl) FindProvisionedACLsInCluster(clusterName string, in *ksengine.ACLMapping, providedAclType ksengine.ACLOperationsInterface) *ksengine.ACLMapping {
	convertedList := ksengine.Shepherd.RenderACLMappings(clusterName, ksengine.ShepherdACLList, providedAclType)
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
