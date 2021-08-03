package core

/*
	This is the Mapping type used for any ACL map to be provided to the Shepherd Core for Translation.
	Due to this centralized mapping structure, it will be easier in the future to cast mappings from one
	type to another. Eg: Convert Kafka ACLs to Confluent RBAC while migrating the cluster objects.
	The Value is marked as an interface but is expected to be of type NVPairs which is mapped to map[string]string.
	This greatly enhances the interface capability to pass in more arbitrary data as required for supporting
	different types of Security Implementations.
*/
type ACLMapping map[ACLDetails]interface{}

type ACLDetails struct {
	ResourceType ACLResourceInterface
	ResourceName string
	PatternType  KafkaACLPatternType
	Principal    string
	Operation    ACLOperationsInterface
	Hostname     string
}

type ACLStreamChannels struct {
	sChannel chan ACLMapping
	fChannel chan ACLMapping
	finished chan bool
}

func getNewACLChannels() ACLStreamChannels {
	return ACLStreamChannels{sChannel: make(chan ACLMapping), fChannel: make(chan ACLMapping), finished: make(chan bool)}
}

///////////////////////////////////////////////////////////////////////////////
////// ACL Operations Interface for helping with different ACL patterns ///////
///////////////////////////////////////////////////////////////////////////////

type ACLOperationsInterface interface {
	String() string
	GetValue(in string) (ACLOperationsInterface, error)
	generateACLMappingStructures(pack ACLMapping, sChannel chan<- ACLMapping, fChannel chan<- ACLMapping, done chan<- bool)
}

/*
	The function takes in the input as the ACLMapping struct which is then processed and converted to the respective ACLs that you desire
	as per the needType value. The response provided is 2 channels that will produce arbitrary number of ACLs as per your requirements.
	The sChannel caters to the ACLs that were successfully parsed and should be executed.
	The fChannel caters to the ACLs that did not fit any bill as per the core determination functions  and needs further analysis from the caller.
*/
func (u *UserTopicMapping) RenderACLMappings(in ACLMapping, needType ACLOperationsInterface) ACLStreamChannels {
	sChannel, fChannel, done := make(chan ACLMapping, 50), make(chan ACLMapping, 50), make(chan bool)
	go needType.generateACLMappingStructures(in, sChannel, fChannel, done)
	return ACLStreamChannels{sChannel: sChannel, fChannel: fChannel, finished: done}
}

func constructACLDetailsObject(resType ACLResourceInterface, resName string, patType KafkaACLPatternType,
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
