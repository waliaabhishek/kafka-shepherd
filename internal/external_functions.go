package core

import (
	"strings"

	ksmisc "shepherd/misc"

	mapset "github.com/deckarep/golang-set"
)

///////////////////////////////////////////////////////////////////////////////
/////////////////////// User Topic Mapping Managers ///////////////////////////
///////////////////////////////////////////////////////////////////////////////

/*
	This method generates a Set of Topic Names that does not include the
	(.*) suffixed topics. Technically, this is the unique list of topics
	that the conffiguration is expecting to be created.
*/
func (utm *UserTopicMapping) getTopicListFromUTMList() mapset.Set {
	t := mapset.NewSet()
	for _, v := range *utm {
		for _, topic := range v.TopicList {
			if ksmisc.IsTopicName(topic, spdCore.Configs.ConfigRoot.ShepherdCoreConfig.SeperatorToken) {
				t.Add(topic)
			}
		}
	}
	return t
}

func (utm *UserTopicMapping) PrintUTM() {
	for k1, v1 := range *utm {
		logger.Infow("User Topic Mapping Details",
			"User ID", k1.ID,
			"Client Type", k1.ClientType,
			"Group ID", k1.GroupID,
			"Topic List", strings.Join(v1.TopicList, ", "))
	}
}

/*
	Returns the Map of ACLMapping by comparing the output of ACL's present in the Kafka Cluster
	to the map of ACLMapping created by parsing the configurations. The response is the mapping
	that the Kafka connection will need to create as a baseline.
*/
func FindNonExistentACLsInCluster(in *ACLMapping) ACLMapping {
	ret := make(ACLMapping)
	for key := range aclList {
		val := ACLDetails{ClientID: key.ClientID, GroupID: key.GroupID, Operation: key.Operation, TopicName: key.TopicName, Hostname: key.Hostname}
		if _, present := (*in)[val]; !present {
			ret[val] = nil
		}
	}
	return ret
}

/*
	Returns ACLMapping construct for the ACLs that are provisioned in the Kafka cluster, but are
	not available as part of the configuration files.
*/
func FindNonExistentACLsInConfig(in ACLMapping) ACLMapping {
	ret := make(ACLMapping)
	for key := range in {
		val := ACLDetails{ClientID: key.ClientID, GroupID: key.GroupID, Operation: key.Operation, TopicName: key.TopicName, Hostname: key.Hostname}
		if _, present := aclList[val]; !present {
			ret[val] = nil
		}
	}
	return ret
}

/*
	Compares the list of ACLMappings provided from the Kafka Cluster to the ACLMappings that are
	part of the Configurations. It creates
*/
func FindProvisionedACLsInCluster(in ACLMapping) ACLMapping {
	ret := make(ACLMapping)
	for key := range in {
		val := ACLDetails{ClientID: key.ClientID, GroupID: key.GroupID, Operation: key.Operation, TopicName: key.TopicName, Hostname: key.Hostname}
		if _, present := aclList[val]; present {
			ret[val] = nil
		}
	}
	return ret
}

func (utm *UserTopicMapping) generateSimpleListFromUTM() ACLMapping {
	ret := make(ACLMapping, 0)
	var temp ClusterAclOperation = CAO_UNKNOWN
	for k, v := range *utm {
		pairs := make([][]string, 5)
		// TODO: Add logic to convert the higher level constructs (PRODUCER, CONSUMER, etc to the lower level constructs (ClusterAclOperation)
		pairs = append(pairs, []string{k.ID}, []string{k.GroupID}, []string{k.ClientType.String()}, v.Hostnames, v.TopicList)
		pairs = ksmisc.GetPermutationsString(pairs)
		for _, i := range pairs {
			temp.GetValue(i[2])
			ret[ACLDetails{ClientID: i[0], GroupID: i[1], Operation: temp, Hostname: i[3], TopicName: i[4]}] = nil
		}
	}
	return ret
}

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

/*
	This function compares the Configurations in the input files with the
	topics existing in the Kafka Cluster. Then it returns only the topics
	that do not exist on the Kafka Cluster.
*/
func FindNonExistentTopicsInCluster(clusterTopics []string) []string {
	return GetStringSliceFromMapSet(FindNonExistentTopicsInClusterMapSet(clusterTopics))
}

/*
	This function compares the Configurations in the input files with the
	topics existing in the Kafka Cluster. Then it returns only the topics
	that do not exist on the Kafka Cluster. If you want to work with
	map sets instead of the standard string slice, use this function.
*/
func FindNonExistentTopicsInClusterMapSet(clusterTopics []string) mapset.Set {
	return topicsInConfig.Difference(GetMapSetFromStringSlice(clusterTopics))
}

/*
	This function returns the topics that are present on the Kafka Cluster
	but are not present in the Configuration input.
*/
func FindNonExistentTopicsInConfig(clusterTopics []string) []string {
	return GetStringSliceFromMapSet(FindNonExistentTopicsInConfigMapSet(clusterTopics))
}

/*
	This function returns the topics that are present on the Kafka Cluster
	but are not present in the Configuration input. If you want to work with
	map sets instead of the standard string slice, use this function.
*/
func FindNonExistentTopicsInConfigMapSet(clusterTopics []string) mapset.Set {
	return GetMapSetFromStringSlice(clusterTopics).Difference(topicsInConfig).Difference(topicsInConfig)
}

/*
	This function returns the topic list that is part of the config and
	already exists in the cluster.
*/
func FindProvisionedTopics(clusterTopics []string) []string {
	return GetStringSliceFromMapSet(FindProvisionedTopicsMapSet(clusterTopics))
}

/*
	This function returns the topic list that is part of the config and
	already exists in the cluster. If you want to work with map sets
	instead of the standard string slice, use this function.
*/
func FindProvisionedTopicsMapSet(clusterTopics []string) mapset.Set {
	return topicsInConfig.Intersect(GetMapSetFromStringSlice(clusterTopics))
}

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
func FindMisconfiguredTopics(topics TopicConfigMapping) (configDiff []string, partitionDiff []string) {
	cd, pd := FindMisconfiguredTopicsMapSet(topics)
	return GetStringSliceFromMapSet(cd), GetStringSliceFromMapSet(pd)
}

/*
	This function accepts TopicConfigMapping which is a map with the Key as topic name,
	and Value as another map with (key, value) as (property name, property value). Get
	the list of all the configured properties of the topics from the cluster and pass it
	in here for the calculation to run. it will return the list of topics that are
	misconfigured for the properties mentioned in the configuration file. Return value
	is a MapSet of topics that have configuration differences followed by MapSet of topics
	that have different partition count configured.

	Eg: It will validate `num.partitions` if it is mentioned in Shepherd configuration
	for the is topic. But if it is only present in the CLuster topic config, the property
	is ignored.
*/
func FindMisconfiguredTopicsMapSet(topics TopicConfigMapping) (configDiff mapset.Set, partitionDiff mapset.Set) {
	configDiff = mapset.NewSet()
	partitionDiff = mapset.NewSet()

	for k, v := range topics {
		if (*ConfMaps.TCM)[k] != nil {
			_ = compareTopicDetails(k, v, (*ConfMaps.TCM)[k], &configDiff, &partitionDiff)
		}
	}

	return configDiff, partitionDiff
}

func compareTopicDetails(topicName string, clusterDetails NVPairs, configDetails NVPairs, configDiff *mapset.Set, partitionDiff *mapset.Set) bool {
	flag := true
	for k, v := range configDetails {
		switch k {
		case "num.partitions":
			if v != clusterDetails[k] {
				(*partitionDiff).Add(topicName)
				flag = false
			}
		default:
			if v != clusterDetails[k] {
				(*configDiff).Add(topicName)
				flag = false
			}
		}
	}
	return flag
}

///////////////////////////////////////////////////////////////////////////////
///////////////////// Cluster Config Mapping Managers /////////////////////////
///////////////////////////////////////////////////////////////////////////////
