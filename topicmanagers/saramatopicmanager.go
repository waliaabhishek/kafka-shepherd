package topicmanagers

import (
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	mapset "github.com/deckarep/golang-set"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
	kafkamanagers "github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

type SaramaTopicExecutionManagerImpl struct {
	TopicExecutionManagerBaseImpl
}

var (
	SaramaTopicManager TopicExecutionManager = SaramaTopicExecutionManagerImpl{}
)

/*
	The cluster name is the only known entity for the Engine. The Kafka Connection manager
	operates and maintains all the Kafka Connections. This function is a convenience function
	to find the ConnectionObject and type cast it as a Sarama Cluster Admin connection and use
	it to execute any functionality in this module.
*/
func (t SaramaTopicExecutionManagerImpl) getSaramaConnectionObject(clusterName string) *sarama.ClusterAdmin {
	return kafkamanagers.Connections[kafkamanagers.KafkaConnectionsKey{ClusterName: clusterName}].Connection.(*kafkamanagers.SaramaConnection).SCA
}

func (t SaramaTopicExecutionManagerImpl) GetTopicsAsSet(clusterName string) *mapset.Set {
	tSet := mapset.NewSet()
	for k := range *t.getTopicListFromKafkaCluster(clusterName) {
		tSet.Add(string(k))
	}
	return &tSet
}

/*
	This function returns the list of topics from Kafka Cluster.
*/
func (t SaramaTopicExecutionManagerImpl) getTopicListFromKafkaCluster(clusterName string) (list *map[string]sarama.TopicDetail) {
	topics, err := (*t.getSaramaConnectionObject(clusterName)).ListTopics()
	if err != nil {
		logger.Fatalw("Something Went Wrong while Listing Topics.",
			"Error Details", err)
	}
	return &topics
}

func (t SaramaTopicExecutionManagerImpl) CreateTopics(clusterName string, topics mapset.Set, dryRun bool) {
	tSet := topics.Difference(*t.GetTopicsAsSet(clusterName))
	logger.Info("Topic List that will be executed")
	t.ListTopics(tSet)
	if !dryRun {
		wg := new(sync.WaitGroup)
		conn := t.getSaramaConnectionObject(clusterName)
		wg.Add(tSet.Cardinality())
		for item := range tSet.Iterator().C {
			go t.createTopic(conn, wg, item.(string))
		}
		wg.Wait()
	}
}

func (t SaramaTopicExecutionManagerImpl) createTopic(conn *sarama.ClusterAdmin, wg *sync.WaitGroup, topicName string) {
	defer wg.Done()
	retry := true
	retryCount := 0
	for retry {
		if retryCount < 5 {
			if err := (*conn).CreateTopic(topicName, getTopicConfigProperties(topicName), false); err != nil {
				retryCount += 1
				dur := ksmisc.GenerateRandomDuration(ksmisc.GenerateRandomNumber(5, 10), "s")
				logger.Errorw("Topic Creation failed. Will try again",
					"Topic Name", topicName,
					"Error", err.Error(),
					"Cooldown before retry", dur.String())
				time.Sleep(dur)
			}
		} else {
			logger.Errorw("Topic Creation request failed consecutively. Will not retry",
				"Topic Name", topicName)
			retry = false
		}
	}
}

func (t SaramaTopicExecutionManagerImpl) DeleteProvisionedTopics(clusterName string, topics mapset.Set, dryRun bool) {
	tSet := topics.Intersect(*t.GetTopicsAsSet(clusterName))
	t.deleteTopics(clusterName, &tSet, dryRun)
}

func (t SaramaTopicExecutionManagerImpl) DeleteUnknownTopics(clusterName string, topics mapset.Set, dryRun bool) {
	tSet := (*t.GetTopicsAsSet(clusterName)).Difference(topics)
	t.deleteTopics(clusterName, &tSet, dryRun)
}

func (t SaramaTopicExecutionManagerImpl) deleteTopics(clusterName string, tSet *mapset.Set, dryRun bool) {
	logger.Info("Topic List eligible for Deletion")
	t.ListTopics(*tSet)
	if !dryRun {
		wg := new(sync.WaitGroup)
		conn := t.getSaramaConnectionObject(clusterName)
		wg.Add((*tSet).Cardinality())
		for item := range (*tSet).Iterator().C {
			go t.deleteTopic(conn, wg, item.(string))
		}
		wg.Wait()
	}
}

func (t SaramaTopicExecutionManagerImpl) deleteTopic(conn *sarama.ClusterAdmin, wg *sync.WaitGroup, topicName string) {
	defer wg.Done()
	retry := true
	retryCount := 0
	for retry {
		if retryCount < 5 {
			if err := (*conn).DeleteTopic(topicName); err != nil {
				retryCount += 1
				dur := ksmisc.GenerateRandomDuration(ksmisc.GenerateRandomNumber(5, 10), "s")
				logger.Errorw("Topic deletion failed. Will try again.",
					"Topic Name", topicName,
					"Error", err.Error(),
					"Cooldown before retry", dur.String())
				time.Sleep(dur)
			}
		} else {
			logger.Errorw("Topic Deletion request failed consecutively. Will not retry",
				"Topic Name", topicName)
			retry = false
		}
	}
}

func (t SaramaTopicExecutionManagerImpl) ModifyTopics(clusterName string, dryRun bool) {
	cDiff, pDiff := t.findMismatchedConfigTopics(clusterName)
	logger.Info("Configurations will be updated for the following topics")
	t.ListTopics(cDiff)
	logger.Info("Partition Count will be updated for the following topics")
	t.ListTopics(pDiff)

	if !dryRun {
		wg := new(sync.WaitGroup)
		conn := t.getSaramaConnectionObject(clusterName)
		wg.Add(pDiff.Cardinality())
		for item := range pDiff.Iterator().C {
			// go t.createTopic(conn, wg, item.(string))
			go modifyTopicPartitions(conn, wg, item.(string))
		}
		wg.Wait()

		wg.Add(cDiff.Cardinality())
		for item := range pDiff.Iterator().C {
			go modifyTopicConfig(conn, wg, item.(string))
		}
		wg.Wait()
	}
}

func modifyTopicConfig(conn *sarama.ClusterAdmin, wg *sync.WaitGroup, topicName string) {
	defer wg.Done()
	retry := true
	retryCount := 0
	for retry {
		if retryCount < 5 {
			// if err := (*conn).CreateTopic(topicName, getTopicConfigProperties(topicName), false); err != nil {
			if err := (*conn).AlterConfig(sarama.TopicResource, topicName, getTopicConfigProperties(topicName).ConfigEntries, false); err != nil {
				retryCount += 1
				dur := ksmisc.GenerateRandomDuration(ksmisc.GenerateRandomNumber(5, 10), "s")
				logger.Errorw("Topic Configuration update failed. Will try again",
					"Topic Name", topicName,
					"Error", err.Error(),
					"Cooldown before retry", dur.String())
				time.Sleep(dur)
			}
		} else {
			logger.Errorw("Topic Configuration update request failed consecutively. Will not retry",
				"Topic Name", topicName)
			retry = false
		}
	}
}

func modifyTopicPartitions(conn *sarama.ClusterAdmin, wg *sync.WaitGroup, topicName string) {
	defer wg.Done()
	retry := true
	retryCount := 0
	for retry {
		if retryCount < 5 {
			if err := (*conn).CreatePartitions(topicName, getTopicConfigProperties(topicName).NumPartitions, nil, false); err != nil {
				retryCount += 1
				dur := ksmisc.GenerateRandomDuration(ksmisc.GenerateRandomNumber(5, 10), "s")
				logger.Errorw("Topic Partition Count failed. Will try again",
					"Topic Name", topicName,
					"Error", err.Error(),
					"Cooldown before retry", dur.String())
				time.Sleep(dur)
			}
		} else {
			logger.Errorw("Topic partition count change request failed consecutively. Will not retry",
				"Topic Name", topicName)
			retry = false
		}
	}
}

func getTopicConfigProperties(topicName string) *sarama.TopicDetail {
	// TODO: Add default Values  in the config file and update it here.
	var td sarama.TopicDetail = sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ReplicaAssignment: nil,
		ConfigEntries:     nil,
	}

	temp := ksengine.ConfMaps.TCM[topicName]
	for k, v := range temp {
		switch k {
		case "num.partitions":
			if v, err := strconv.Atoi(v); err == nil {
				td.NumPartitions = int32(v)
			}
		case "replication.factor", "default.replication.factor":
			if v, err := strconv.Atoi(v); err == nil {
				td.ReplicationFactor = int16(v)
			}
		case "ReplicaAssignment":
			// TODO: Update the correct property for Replica Assignment here.
			break
		default:
			strValue := v
			if td.ConfigEntries != nil {
				td.ConfigEntries[k] = &strValue
			} else {
				td.ConfigEntries = make(map[string]*string)
				td.ConfigEntries[k] = &strValue
			}
		}
	}
	return &td
}

func (t SaramaTopicExecutionManagerImpl) findMismatchedConfigTopics(clusterName string) (configDiff mapset.Set, partitionDiff mapset.Set) {
	clusterTCM := make(ksengine.TopicConfigMapping)
	for tName, configs := range *t.getTopicListFromKafkaCluster(clusterName) {
		t.generateTopicConfigMappings(&clusterTCM, tName, &configs)
	}

	for tName, cPairs := range clusterTCM {
		if (ksengine.ConfMaps.TCM)[tName] != nil {
			for propName, propVal := range cPairs {
				switch propName {
				case "num.partitions":
					if propVal != (ksengine.ConfMaps.TCM)[tName][propName] {
						partitionDiff.Add(tName)
					}
				default:
					if propVal != (ksengine.ConfMaps.TCM)[tName][propName] {
						configDiff.Add(tName)
					}
				}
			}
		}
	}
	return
}

func (t SaramaTopicExecutionManagerImpl) generateTopicConfigMappings(ctcm *ksengine.TopicConfigMapping, topicName string, topicDetails *sarama.TopicDetail) {
	// Anon Function
	assignment := func(v *ksengine.NVPairs) {
		(*v)["num.partitions"] = strconv.FormatInt(int64(topicDetails.NumPartitions), 10)
		(*v)["replication.factor"] = strconv.FormatInt(int64(topicDetails.ReplicationFactor), 10)
		// TODO: Replica Assignment is completely ignored at this time due to format restrictions.
		// Not even sure if that will be something that folks would need in the long run or not.
		for pName, pVal := range topicDetails.ConfigEntries {
			(*v)[pName] = *pVal
		}
	}

	if value, present := (*ctcm)[topicName]; !present {
		v := ksengine.NVPairs{}
		assignment(&v)
		(*ctcm)[topicName] = v
	} else {
		assignment(&value)
		(*ctcm)[topicName] = value
	}
}
