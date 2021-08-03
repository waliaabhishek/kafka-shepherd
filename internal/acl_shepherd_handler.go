package core

import (
	"fmt"
	ksmisc "shepherd/misc"
	"strings"
)

type (
	ShepherdClientType int
)

const (
	ShepherdClientType_UNKNOWN ShepherdClientType = iota
	ShepherdClientType_EMPTY
	ShepherdClientType_PRODUCER
	ShepherdClientType_TRANSACTIONAL_PRODUCER
	ShepherdClientType_PRODUCER_IDEMPOTENCE
	ShepherdClientType_CONSUMER
	ShepherdClientType_CONSUMER_GROUP
	ShepherdClientType_SOURCE_CONNECTOR
	ShepherdClientType_SINK_CONNECTOR
	ShepherdClientType_STREAM_READ
	ShepherdClientType_STREAM_WRITE
	ShepherdClientType_KSQL
)

func (in ShepherdClientType) String() string {
	m := map[ShepherdClientType]string{
		ShepherdClientType_UNKNOWN:                "ShepherdClientType_UNKNOWN",
		ShepherdClientType_EMPTY:                  "EMPTY",
		ShepherdClientType_PRODUCER:               "PRODUCER",
		ShepherdClientType_TRANSACTIONAL_PRODUCER: "TRANSACTIONAL_PRODUCER",
		ShepherdClientType_PRODUCER_IDEMPOTENCE:   "PRODUCER_IDEMPOTENCE",
		ShepherdClientType_CONSUMER:               "CONSUMER",
		ShepherdClientType_CONSUMER_GROUP:         "CONSUMER_GROUP",
		ShepherdClientType_SOURCE_CONNECTOR:       "SOURCE_CONNECTOR",
		ShepherdClientType_SINK_CONNECTOR:         "SINK_CONNECTOR",
		ShepherdClientType_STREAM_READ:            "STREAM_READ",
		ShepherdClientType_STREAM_WRITE:           "STREAM_WRITE",
		ShepherdClientType_KSQL:                   "KSQL",
	}
	ret, present := m[in]
	if !present {
		ret = m[ShepherdClientType_UNKNOWN]
	}
	return ret
}

func (c ShepherdClientType) GetValue(in string) (ACLOperationsInterface, error) {
	m := map[string]ShepherdClientType{
		"ShepherdClientType_UNKNOWN": ShepherdClientType_UNKNOWN,
		"EMPTY":                      ShepherdClientType_EMPTY,
		"PRODUCER":                   ShepherdClientType_PRODUCER,
		"TRANSACTIONAL_PRODUCER":     ShepherdClientType_TRANSACTIONAL_PRODUCER,
		"PRODUCER_IDEMPOTENCE":       ShepherdClientType_PRODUCER_IDEMPOTENCE,
		"CONSUMER":                   ShepherdClientType_CONSUMER,
		"CONSUMER_GROUP":             ShepherdClientType_CONSUMER_GROUP,
		"SOURCE_CONNECTOR":           ShepherdClientType_SOURCE_CONNECTOR,
		"SINK_CONNECTOR":             ShepherdClientType_SINK_CONNECTOR,
		"STREAM_READ":                ShepherdClientType_STREAM_READ,
		"STREAM_WRITE":               ShepherdClientType_STREAM_WRITE,
		"KSQL":                       ShepherdClientType_KSQL,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["ShepherdClientType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
}

// This function will generate the mappings in the ShepherdClientType internal structure type for all the mappings provided
// as the input `in` value. Whatever it is able to properly convert, those mappings will be added to the success
// map and the rest will be added to the failed map.
func (c ShepherdClientType) generateACLMappingStructures(in ACLMapping, sChannel chan<- ACLMapping, fChannel chan<- ACLMapping, done chan<- bool) {
	for k := range in {
		temp := ACLMapping{}
		switch k.Operation.(type) {
		case ShepherdClientType:
			temp[k] = nil
			sChannel <- temp
		default:
			logger.Warnf("Conversion from %T type to %T type is not supported yet. The ACL mapping will be added to the Failed list.", k.Operation, c)
			temp[k] = nil
			fChannel <- temp
		}
	}
	done <- true
	close(sChannel)
	close(fChannel)
	close(done)
}

func (utm *UserTopicMapping) GenerateShepherdClientTypeMappings() ACLMapping {
	ret := make(ACLMapping, 0)
	var temp ShepherdClientType
	for k, v := range *utm {
		pairs := make([][]string, 5)
		pairs = append(pairs, []string{k.Principal}, []string{k.GroupID}, []string{k.ClientType.String()}, v.Hostnames, v.TopicList)
		// TODO: Add logic to convert the higher level constructs (PRODUCER, CONSUMER, etc to the lower level constructs (ClusterAclOperation)
		pairs = ksmisc.GetPermutationsString(pairs)
		for _, i := range pairs {
			varType, _ := temp.GetValue(i[2])
			switch varType {
			case ShepherdClientType_PRODUCER:
				ret[constructACLDetailsObject(KafkaResourceType_TOPIC, i[4], determinePatternType(i[4]),
					i[0], varType, i[3])] = nil
			case ShepherdClientType_TRANSACTIONAL_PRODUCER:
				ret[constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, i[1], determinePatternType(i[4]),
					i[0], varType, i[3])] = nil
			case ShepherdClientType_PRODUCER_IDEMPOTENCE:
				ret[constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", determinePatternType(i[4]),
					i[0], varType, i[3])] = nil
			case ShepherdClientType_CONSUMER:
				ret[constructACLDetailsObject(KafkaResourceType_TOPIC, i[4], determinePatternType(i[4]),
					i[0], varType, i[3])] = nil
			case ShepherdClientType_CONSUMER_GROUP:
				ret[constructACLDetailsObject(KafkaResourceType_GROUP, i[2], determinePatternType(i[4]),
					i[0], varType, i[3])] = nil
			case ShepherdClientType_SOURCE_CONNECTOR:
				value := make(NVPairs)
				value[KafkaResourceType_CLUSTER.String()] = "kafka-cluster"
				ret[constructACLDetailsObject(KafkaResourceType_TOPIC, i[4], determinePatternType(i[4]),
					i[0], varType, i[3])] = value
			case ShepherdClientType_SINK_CONNECTOR:
				value := make(NVPairs)
				value[KafkaResourceType_GROUP.String()] = i[1]
				value[KafkaResourceType_CLUSTER.String()] = "kafka-cluster"
				ret[constructACLDetailsObject(KafkaResourceType_TOPIC, i[4], determinePatternType(i[4]),
					i[0], varType, i[3])] = value
			case ShepherdClientType_STREAM_READ:
				ret[constructACLDetailsObject(KafkaResourceType_TOPIC, i[4], determinePatternType(i[4]),
					i[0], varType, i[3])] = nil
			case ShepherdClientType_STREAM_WRITE:
				ret[constructACLDetailsObject(KafkaResourceType_TOPIC, i[4], determinePatternType(i[4]),
					i[0], varType, i[3])] = nil
			case ShepherdClientType_KSQL:
				// TODO: Implement KSQL Permission sets
				ret[constructACLDetailsObject(KafkaResourceType_KSQL_CLUSTER, i[1], KafkaACLPatternType_PREFIXED,
					i[0], varType, i[3])] = nil
			default:
				// TODO: Error handling if the Client Type provided is unknown
			}
			ret[ACLDetails{
				ResourceType: KafkaResourceType_TOPIC,
				ResourceName: i[4],
				PatternType:  determinePatternType(i[4]),
				Principal:    i[0],
				Operation:    varType,
				Hostname:     i[3],
			}] = nil
		}
	}
	return ret
}
