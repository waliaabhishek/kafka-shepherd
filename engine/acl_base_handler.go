package engine

/*
	This is the Mapping type used for any ACL map to be provided to the Shepherd Core for Translation.
	Due to this centralized mapping structure, it will be easier in the future to cast mappings from one
	type to another. Eg: Convert Kafka ACLs to Confluent RBAC while migrating the cluster objects.
	The Value is marked as an interface but is expected to be of type NVPairs which is mapped to map[string]string.
	This greatly enhances the interface capability to pass in more arbitrary data as required for supporting
	different types of Security Implementations.
*/
type ACLMapping map[ACLDetails]interface{}

func (m *ACLMapping) Append(k ACLDetails, v interface{}) {
	(*m)[k] = v
}

type ACLDetails struct {
	ResourceType ACLResourceInterface
	ResourceName string
	PatternType  KafkaACLPatternType
	Principal    string
	Operation    ACLOperationsInterface
	Hostname     string
}

type ACLStreamChannels struct {
	SChannel chan ACLMapping
	FChannel chan ACLMapping
	Finished chan bool
}

func getNewACLChannels() ACLStreamChannels {
	return ACLStreamChannels{SChannel: make(chan ACLMapping), FChannel: make(chan ACLMapping), Finished: make(chan bool)}
}

///////////////////////////////////////////////////////////////////////////////
////// ACL Operations Interface for helping with different ACL patterns ///////
///////////////////////////////////////////////////////////////////////////////

type ACLOperationsInterface interface {
	String() string
	GetValue(in string) (ACLOperationsInterface, error)
	generateACLMappingStructures(pack *ACLMapping) *ACLMapping
}

/*
	The function takes in the input as the ACLMapping struct which is then processed and converted to the respective ACLs that you desire
	as per the needType value. The response provided is 3 channels that will produce arbitrary number of ACLs as per your requirements.
	The sChannel caters to the ACLs that were successfully parsed and should be executed.
	The fChannel caters to the ACLs that did not fit any bill as per the core determination functions  and needs further analysis from the caller.
	The finished Channel only produces a boolean value once (true) for you to understand that the ACL emission is completed and you can logically
	close your select loops (if any).
*/
func (u *UserTopicMapping) RenderACLMappings(in *ACLMapping, needType ACLOperationsInterface) *ACLMapping {
	return needType.generateACLMappingStructures(in)
}

/*
	This Render function renders the full ACl list and returns it as a Map instead of channels.
	The ACL Mappings are already de-duplicated and failed ACLs ignored. This is a convenience method of sorts
	for static render of all the ACs that are gonna be generated by the Core setup for provided ACLType.
	Please know that Channel stream is always more efficient, but there are some edge cases that you need full
	renders and that's where this function helps. It will use additional Memory to render the full ACL Maps.
*/
// func (u *UserTopicMapping) RenderACLMappingMap(in *ACLMapping, providedAclType ACLOperationsInterface) *ACLMapping {
// 	emptyACL := make(ACLMapping)
// 	inStream := ConfMaps.UTM.RenderACLMappingsStream(aclList, providedAclType)
// 	return conditionalACLStreamer(inStream, &emptyACL, false, nil, false)
// }

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