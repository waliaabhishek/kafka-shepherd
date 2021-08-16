package engine

import ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"

/*
	This is the Mapping type used for any ACL map to be provided to the Shepherd Core for Translation.
	Due to this centralized mapping structure, it will be easier in the future to cast mappings from one
	type to another. Eg: Convert Kafka ACLs to Confluent RBAC while migrating the cluster objects.
	The Value is marked as an interface but is expected to be of type NVPairs which is of type map[string]string.
	This greatly enhances the interface capability to pass in more arbitrary data as required for supporting
	different types of Security Implementations.
*/
type (
	ACLMapping             map[ACLDetails]interface{}
	KafkaResourceType      int
	KafkaACLPatternType    int
	KafkaACLPermissionType int
	ACLDetails             struct {
		ResourceType ACLResourceInterface
		ResourceName string
		PatternType  ACLPatternInterface
		Principal    string
		Operation    ACLOperationsInterface
		Hostname     string
	}
)

///////////////////////////////////////////////////////////////////////////////
////// ACL Operations Interface for helping with different ACL patterns ///////
///////////////////////////////////////////////////////////////////////////////

type (
	ShepherdACLManager interface {
		ACLResourceInterface
		ACLPatternInterface
		ACLOperationsInterface
	}

	ACLResourceInterface interface {
		GetACLResourceString() string
		GetACLResourceValue(in string) (ACLResourceInterface, error)
	}

	ACLPatternInterface interface {
		GetACLPatternString() string
	}

	ACLOperationsInterface interface {
		String() string
		GetValue(in string) (ACLOperationsInterface, error)
		generateACLMappingStructures(pack *ACLMapping) *ACLMapping
	}

	ShepherdACLManagerBaseImpl struct {
		KafkaResourceType
		KafkaACLPatternType
	}
)

func constructACLDetailsObject(resType ACLResourceInterface, resName string, patType ACLPatternInterface,
	prin string, op ACLOperationsInterface, host string) ACLDetails {
	return ACLDetails{
		ResourceType: resType,
		ResourceName: resName,
		PatternType:  patType,
		Principal:    prin,
		Operation:    op,
		Hostname:     host,
	}
}

func (c *ACLMapping) prettyPrintACLMapping() {
	ksmisc.DottedLineOutput("List ACLMapping", "=", 80)
	for k := range *c {
		logger.Infow("ACL Mapping Details",
			"Principal", k.Principal,
			"Hostname", k.Hostname,
			"Operation", k.Operation.String(),
			"Resource Type", k.ResourceType.GetACLResourceString(),
			"Resource Name", k.ResourceName)
	}
	ksmisc.DottedLineOutput("", "=", 80)
}

func (m *ACLMapping) Append(k ACLDetails, v interface{}) {
	(*m)[k] = v
}
