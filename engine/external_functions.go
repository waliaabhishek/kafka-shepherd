package engine

import (
	"strings"

	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

///////////////////////////////////////////////////////////////////////////////
/////////////////////// User Topic Mapping Managers ///////////////////////////
///////////////////////////////////////////////////////////////////////////////

func (utm *UserTopicMapping) getShepherdACLList() *ACLMapping {
	ret := make(ACLMapping)
	var temp ShepherdClientType
	for k, v := range *utm {
		pairs := make([][]string, 0)
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
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "kafka-cluster"
				ret[constructACLDetailsObject(KafkaResourceType_TOPIC, i[4], determinePatternType(i[4]),
					i[0], varType, i[3])] = value
			case ShepherdClientType_SINK_CONNECTOR:
				value := make(NVPairs)
				value[KafkaResourceType_GROUP.GetACLResourceString()] = i[1]
				value[KafkaResourceType_CLUSTER.GetACLResourceString()] = "kafka-cluster"
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
		}
	}
	return &ret
}

func (utm *UserTopicMapping) PrintUTM() {
	for k1, v1 := range *utm {
		logger.Infow("User Topic Mapping Details",
			"User ID", k1.Principal,
			"Client Type", k1.ClientType,
			"Group ID", k1.GroupID,
			"Topic List", strings.Join(v1.TopicList, ", "))
	}
}

func conditionalACLMapper(inputACLs *ACLMapping, findIn *ACLMapping, presenceCheck bool) *ACLMapping {
	ret := make(ACLMapping)
	for k, v := range *inputACLs {
		if _, present := (*findIn)[k]; present == presenceCheck {
			if _, found := ret[k]; !found {
				ret[k] = v
			}
		}
	}
	return &ret
}

// /*
// 	Returns the Map of ACLMapping by comparing the output of ACL's present in the Kafka Cluster
// 	to the map of ACLMapping created by parsing the configurations. The response is the mapping
// 	that the Kafka connection will need to create as a baseline.
// */
// func FindNonExistentACLsInCluster(in *ACLMapping, providedAclType ACLOperationsInterface) *ACLMapping {
// 	convertedList := ConfMaps.UTM.RenderACLMappings(&shepherdACLList, providedAclType)
// 	return conditionalACLMapper(convertedList, in, false)
// }

// /*
// 	Returns ACLMapping construct for the ACLs that are provisioned in the Kafka cluster, but are
// 	not available as part of the configuration files.
// */
// func FindNonExistentACLsInConfig(in *ACLMapping, providedAclType ACLOperationsInterface) *ACLMapping {
// 	convertedList := ConfMaps.UTM.RenderACLMappings(&shepherdACLList, providedAclType)
// 	return conditionalACLMapper(in, convertedList, false)
// }

// /*
// 	Compares the list of ACLMappings provided from the Kafka Cluster to the ACLMappings that are
// 	part of the Configurations. It returns ACL Stream that is a part of the Shepherd Config and
// 	is already provisioned in the Kafka Cluster
// */
// func FindProvisionedACLsInCluster(in *ACLMapping, providedAclType ACLOperationsInterface) *ACLMapping {
// 	convertedList := ConfMaps.UTM.RenderACLMappings(&shepherdACLList, providedAclType)
// 	return conditionalACLMapper(convertedList, in, true)
// }

///////////////////////////////////////////////////////////////////////////////
/////////////////////// Topic Config Mapping Managers /////////////////////////
///////////////////////////////////////////////////////////////////////////////

func (tcm *TopicConfigMapping) PrintTCM() {
	for k, v := range *tcm {
		logger.Infow("Topic Config Mapping Details",
			"Topic Name", k,
			"Topic Properties", v)
	}
}

func ListTopicsInConfig() []string {
	return ksmisc.GetStringSliceFromMapSet(topicsInConfig)
}

/*
	This function compares the Configurations in the input files with the
	topics existing in the Kafka Cluster. Then it returns only the topics
	that do not exist on the Kafka Cluster.
*/
// func FindNonExistentTopicsInCluster(clusterTopics []string) []string {
// 	return ksmisc.GetStringSliceFromMapSet(FindNonExistentTopicsInClusterMapSet(clusterTopics))
// }

/*
	This function compares the Configurations in the input files with the
	topics existing in the Kafka Cluster. Then it returns only the topics
	that do not exist on the Kafka Cluster. If you want to work with
	map sets instead of the standard string slice, use this function.
*/
// func FindNonExistentTopicsInClusterMapSet(clusterTopics []string) mapset.Set {
// 	return topicsInConfig.Difference(*ksmisc.GetMapSetFromStringSlice(&clusterTopics))
// }

/*
	This function returns the topics that are present on the Kafka Cluster
	but are not present in the Configuration input.
*/
// func FindNonExistentTopicsInConfig(clusterTopics []string) []string {
// 	return ksmisc.GetStringSliceFromMapSet(FindNonExistentTopicsInConfigMapSet(clusterTopics))
// }

/*
	This function returns the topics that are present on the Kafka Cluster
	but are not present in the Configuration input. If you want to work with
	map sets instead of the standard string slice, use this function.
*/
// func FindNonExistentTopicsInConfigMapSet(clusterTopics []string) mapset.Set {
// 	return (*ksmisc.GetMapSetFromStringSlice(&clusterTopics)).Difference(topicsInConfig).Difference(topicsInConfig)
// }

/*
	This function returns the topic list that is part of the config and
	already exists in the cluster.
*/
// func FindProvisionedTopics(clusterTopics []string) []string {
// 	return ksmisc.GetStringSliceFromMapSet(FindProvisionedTopicsMapSet(clusterTopics))
// }

/*
	This function returns the topic list that is part of the config and
	already exists in the cluster. If you want to work with map sets
	instead of the standard string slice, use this function.
*/
// func FindProvisionedTopicsMapSet(clusterTopics []string) mapset.Set {
// 	return topicsInConfig.Intersect(*ksmisc.GetMapSetFromStringSlice(&clusterTopics))
// }

/*
	This function accepts TopicConfigMapping which is a map with the Key as topic name,
	and Value as another map with (key, value) as (property name, property value). Get
	the list of all the configured properties of the topics from the cluster and pass it
	in here for the calculation to run. it will return the list of topics that are
	misconfigured for the properties mentioned in the configuration file. Return value
	is a slice of topics that have configuration differences followed by slice of topics
	that have different partition count configured.

	Eg: It will validate `num.partitions` if it is mentioned in Shepherd configuration
	for the is topic. But if it is only present in the CLuster topic config, the property
	is ignored.
*/
// func FindMisconfiguredTopics(topics TopicConfigMapping) (configDiff []string, partitionDiff []string) {
// 	cd, pd := FindMisconfiguredTopicsMapSet(topics)
// 	return ksmisc.GetStringSliceFromMapSet(cd), ksmisc.GetStringSliceFromMapSet(pd)
// }

// /*
// 	This function accepts TopicConfigMapping which is a map with the Key as topic name,
// 	and Value as another map with (key, value) as (property name, property value). Get
// 	the list of all the configured properties of the topics from the cluster and pass it
// 	in here for the calculation to run. it will return the list of topics that are
// 	misconfigured for the properties mentioned in the configuration file. Return value
// 	is a MapSet of topics that have configuration differences followed by MapSet of topics
// 	that have different partition count configured.

// 	Eg: It will validate `num.partitions` if it is mentioned in Shepherd configuration
// 	for the is topic. But if it is only present in the CLuster topic config, the property
// 	is ignored.
// */
// func FindMisconfiguredTopicsMapSet(topics TopicConfigMapping) (configDiff mapset.Set, partitionDiff mapset.Set) {
// 	configDiff = mapset.NewSet()
// 	partitionDiff = mapset.NewSet()

// 	for k, v := range topics {
// 		if (ConfMaps.TCM)[k] != nil {
// 			_ = compareTopicDetails(k, v, (ConfMaps.TCM)[k], &configDiff, &partitionDiff)
// 		}
// 	}

// 	return configDiff, partitionDiff
// }

// func compareTopicDetails(topicName string, clusterDetails NVPairs, configDetails NVPairs, configDiff *mapset.Set, partitionDiff *mapset.Set) bool {
// 	flag := true
// 	for k, v := range configDetails {
// 		switch k {
// 		case "num.partitions":
// 			if v != clusterDetails[k] {
// 				(*partitionDiff).Add(topicName)
// 				flag = false
// 			}
// 		default:
// 			if v != clusterDetails[k] {
// 				(*configDiff).Add(topicName)
// 				flag = false
// 			}
// 		}
// 	}
// 	return flag
// }

///////////////////////////////////////////////////////////////////////////////
///////////////////// Cluster Config Mapping Managers /////////////////////////
///////////////////////////////////////////////////////////////////////////////
