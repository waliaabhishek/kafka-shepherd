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
			"Group ID", k1.GroupId,
			"Topic List", strings.Join(v1.TopicList, ", "))
	}
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
func FindNonExistentTopicsInKafkaCluster(clusterTopics []string) []string {
	return GetStringSliceFromMapSet(FindNonExistentTopicsInKafkaClusterAsMapSet(clusterTopics))
}

/*
	This function compares the Configurations in the input files with the
	topics existing in the Kafka Cluster. Then it returns only the topics
	that do not exist on the Kafka Cluster. If you want to work with
	map sets instead of the standard string slice, use this function.
*/
func FindNonExistentTopicsInKafkaClusterAsMapSet(clusterTopics []string) mapset.Set {
	return topicsInConfig.Difference(GetMapSetFromStringSlice(clusterTopics))
}

/*
	This function returns the topics that are present on the Kafka Cluster
	but are not present in the Configuration input.
*/
func FindNonExistentTopicsInTopicsConfig(clusterTopics []string) []string {
	return GetStringSliceFromMapSet(FindNonExistentTopicsInTopicsConfigAsMapSet(clusterTopics))
}

/*
	This function returns the topics that are present on the Kafka Cluster
	but are not present in the Configuration input. If you want to work with
	map sets instead of the standard string slice, use this function.
*/
func FindNonExistentTopicsInTopicsConfigAsMapSet(clusterTopics []string) mapset.Set {
	return GetMapSetFromStringSlice(clusterTopics).Difference(topicsInConfig).Difference(topicsInConfig)
}

/*
	This function returns the topic list that is part of the config and
	already exists in the cluster.
*/
func FindProvisionedTopics(clusterTopics []string) []string {
	return GetStringSliceFromMapSet(FindProvisionedTopicsAsMapSet(clusterTopics))
}

/*
	This function returns the topic list that is part of the config and
	already exists in the cluster. If you want to work with map sets
	instead of the standard string slice, use this function.
*/
func FindProvisionedTopicsAsMapSet(clusterTopics []string) mapset.Set {
	return topicsInConfig.Intersect(GetMapSetFromStringSlice(clusterTopics))
}

///////////////////////////////////////////////////////////////////////////////
///////////////////// Cluster Config Mapping Managers /////////////////////////
///////////////////////////////////////////////////////////////////////////////
