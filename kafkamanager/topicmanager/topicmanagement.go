package topicmanager

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	ksinternal "shepherd/internal"
	kafkamanager "shepherd/kafkamanager"
	ksmisc "shepherd/misc"

	"github.com/Shopify/sarama"
	mapset "github.com/deckarep/golang-set"
	"go.uber.org/zap"
)

var topicsInCluster mapset.Set
var topicsInConfig mapset.Set
var sca *sarama.ClusterAdmin
var logger *zap.SugaredLogger

/* This is the function that Initializes User Topic mapping structure
after parsing the configurations from input files. This also instantiates the configuration structure.
*/
func init() {
	sca = kafkamanager.SetupAdminConnection()
}

/*
	This function returns the list of topics from Kafka Cluster.
*/
func getTopicListFromKafkaCluster(sca *sarama.ClusterAdmin) []string {
	tSet := mapset.NewSet()
	if topics, err := (*sca).ListTopics(); err != nil {
		logger.Fatalw("Something Went Wrong while Listing Topics.",
			"Error Details", err)
	} else {
		for t := range topics {
			tSet.Add(string(t))
		}
	}
	return ksinternal.GetStringSliceFromMapSet(tSet)
}

func refreshTopicList(sca *sarama.ClusterAdmin, discardConnectionCache bool) {
	if topicsInCluster == nil || discardConnectionCache {
		topicsInCluster = ksinternal.GetMapSetFromStringSlice(getTopicListFromKafkaCluster(sca))
	}
}

/*
	Create a Struct for Channel signature
*/
type TopicStatusDetails struct {
	topicName  string
	status     StatusType
	errorStr   string
	retryCount int
}

/*
	This function takes the ksinternal.TopicManagementFunctionType constant and
	executes the CREATE, MODIFY or DELETE request. This function takes care of
	all internals and does not need any other details as it fetches these details
	from configuration files and kafka cluster.
*/
func ExecuteRequests(sca *sarama.ClusterAdmin, threadCount int, requestType TopicManagementFunctionType) {
	refreshTopicList(sca, true)
	c := make(chan TopicStatusDetails, threadCount)
	rand.Seed(time.Now().UnixNano())
	var ts mapset.Set
	var ps mapset.Set
	counter := 0

	switch requestType {
	case CREATE_TOPIC:
		ts = ksinternal.FindNonExistentTopicsInClusterMapSet(getTopicListFromKafkaCluster(sca))
	case MODIFY_TOPIC:
		ts, ps = FindMismatchedConfigTopics(sca)
	case DELETE_TOPIC:
		ts = ksinternal.FindProvisionedTopicsMapSet(getTopicListFromKafkaCluster(sca))
	}

	for item := range ts.Iterator().C {
		tName := item.(string)
		dur := ksmisc.GenerateRandomDuration(ksmisc.GenerateRandomNumber(3, 10), "s")
		go executeTopicRequest(sca, tName, getTopicConfigProperties(tName), dur, 0, requestType, c)
		counter += 1
	}

	if requestType == MODIFY_TOPIC && ps.Cardinality() != 0 {
		for item := range ps.Iterator().C {
			tName := item.(string)
			dur := ksmisc.GenerateRandomDuration(ksmisc.GenerateRandomNumber(3, 10), "s")
			go executeTopicRequest(sca, tName, getTopicConfigProperties(tName), dur, 0, ALTER_PARTITION_REQUEST, c)
			counter += 1
		}
	}
	fmt.Println("Total Requests Triggered: ")
	for i := 0; i < counter; i++ {
		tsd := <-c
		switch tsd.status {
		case CREATED, DELETED, MODIFIED, PARTITION_ALTERED_SUCCESSFULLY:
			tsd.prettyPrint()
		case NOT_CREATED, NOT_DELETED, NOT_MODIFIED, PARTITION_NOT_ALTERED:
			if tsd.retryCount <= 5 {
				dur := ksmisc.GenerateRandomDuration(ksmisc.GenerateRandomNumber(3, 10), "s")
				tsd.prettyPrint()
				go executeTopicRequest(sca, tsd.topicName, getTopicConfigProperties(tsd.topicName), dur, tsd.retryCount, requestType, c)
				i -= 1
			} else {
				fmt.Println("Topic Request failed and is not retriable. Skipping for this topic.")
				tsd.prettyPrint()
			}
		}
	}
	waitForMetadataSync(sca, requestType)
}

func FindMismatchedConfigTopics(sca *sarama.ClusterAdmin) (configDiff mapset.Set, partitionDiff mapset.Set) {
	clusterTCM := ksinternal.TopicConfigMapping{}
	if topics, err := (*sca).ListTopics(); err != nil {
		logger.Fatalw("Something Went Wrong while Listing Topics.",
			"Error Details", err)
	} else {
		for k, v := range topics {
			generateTopicConfigMappings(&clusterTCM, k, &v)
		}
	}
	configDiff, partitionDiff = ksinternal.FindMisconfiguredTopicsMapSet(clusterTCM)
	return
}

func generateTopicConfigMappings(ctcm *ksinternal.TopicConfigMapping, topicName string, topicDetails *sarama.TopicDetail) {

	assignment := func(v *ksinternal.NVPairs) {
		(*v)["num.partitions"] = strconv.FormatInt(int64(topicDetails.NumPartitions), 10)
		(*v)["replication.factor"] = strconv.FormatInt(int64(topicDetails.ReplicationFactor), 10)
		// TODO: Replica Assignment is completely ignored at this time due to format restrictions.
		// Not even sure if that will be something that folks would need in the long run or not.
		for pName, pVal := range topicDetails.ConfigEntries {
			(*v)[pName] = *pVal
		}
	}

	if value, present := (*ctcm)[topicName]; !present {
		v := ksinternal.NVPairs{}
		assignment(&v)
		(*ctcm)[topicName] = v
	} else {
		assignment(&value)
		(*ctcm)[topicName] = value
	}
}

// func compareTopicDetails(topicName string, first *sarama.TopicDetail, second ksinternal.NVPairs, configDiff *mapset.Set, partitionDiff *mapset.Set) bool {
// 	flag := true
// 	for k, v := range second {
// 		switch k {
// 		case "num.partitions":
// 			if v != strconv.FormatInt(int64(first.NumPartitions), 10) {
// 				(*partitionDiff).Add(topicName)
// 				flag = false
// 			}
// 		case "replication.factor", "default.replication.factor":
// 			if v != strconv.FormatInt(int64(first.ReplicationFactor), 10) {
// 				(*configDiff).Add(topicName)
// 				flag = false
// 			}
// 		default:
// 			if v != *first.ConfigEntries[k] {
// 				(*configDiff).Add(topicName)
// 				flag = false
// 			}
// 		}
// 	}
// 	return flag
// }

func executeTopicRequest(sca *sarama.ClusterAdmin, topicName string, topicDetail *sarama.TopicDetail, sleepTime time.Duration, retryCount int, requestType TopicManagementFunctionType, c chan TopicStatusDetails) {
	if retryCount > 0 {
		time.Sleep(sleepTime)
	}
	switch requestType {
	case CREATE_TOPIC:
		if err := (*sca).CreateTopic(topicName, topicDetail, false); err != nil {
			c <- TopicStatusDetails{topicName: topicName, status: NOT_CREATED, errorStr: err.Error(), retryCount: retryCount + 1}
		} else {
			c <- TopicStatusDetails{topicName: topicName, status: CREATED, errorStr: "", retryCount: retryCount}
		}
	case MODIFY_TOPIC:
		if err := (*sca).AlterConfig(sarama.TopicResource, topicName, topicDetail.ConfigEntries, false); err != nil {
			c <- TopicStatusDetails{topicName: topicName, status: NOT_MODIFIED, errorStr: err.Error(), retryCount: retryCount + 1}
			break
		} else {
			c <- TopicStatusDetails{topicName: topicName, status: MODIFIED, errorStr: "", retryCount: retryCount}
		}
	case ALTER_PARTITION_REQUEST:
		numParts := 0
		if value, err := (*sca).DescribeTopics([]string{topicName}); err != nil {
			c <- TopicStatusDetails{topicName: topicName, status: PARTITION_NOT_ALTERED, errorStr: err.Error(), retryCount: retryCount + 1}
		} else {
			for _, v := range value {
				if strings.ToLower(strings.TrimSpace(v.Name)) == topicName {
					numParts = len(v.Partitions)
				}
			}
			if numParts < int(topicDetail.NumPartitions) {
				if err := (*sca).CreatePartitions(topicName, topicDetail.NumPartitions, nil, false); err != nil {
					c <- TopicStatusDetails{topicName: topicName, status: PARTITION_NOT_ALTERED, errorStr: err.Error(), retryCount: retryCount + 1}
					break
				} else {
					c <- TopicStatusDetails{topicName: topicName, status: PARTITION_ALTERED_SUCCESSFULLY, errorStr: "", retryCount: retryCount}
					break
				}
			} else if numParts > int(topicDetail.NumPartitions) {
				// fmt.Println("Cannot decrease Partition count. Incorrect request. Please update the configuration files")
				c <- TopicStatusDetails{topicName: topicName, status: PARTITION_NOT_ALTERED, errorStr: "Cannot decrease Partition count. Please update the configuration files. Error will not be retried.", retryCount: 6}
			}
		}
	case DELETE_TOPIC:
		if err := (*sca).DeleteTopic(topicName); err != nil {
			c <- TopicStatusDetails{topicName: topicName, status: NOT_DELETED, errorStr: err.Error(), retryCount: retryCount + 1}
		} else {
			c <- TopicStatusDetails{topicName: topicName, status: DELETED, errorStr: "", retryCount: retryCount}
		}
	}
}

func waitForMetadataSync(sca *sarama.ClusterAdmin, requestType TopicManagementFunctionType) {
	i := 0
	switch requestType {
	case CREATE_TOPIC:
		for {
			refreshTopicList(sca, true)
			if ksinternal.FindNonExistentTopicsInClusterMapSet(getTopicListFromKafkaCluster(sca)).Cardinality() != 0 && i <= 5 {
				time.Sleep(2 * time.Second)
				fmt.Println("The Topics Have not been created yet. Waiting for Metadata to sync")
				i += 1
			} else {
				if i >= 5 {
					fmt.Println("Retried 5 Times. The sync seems to be failing.")
					fmt.Println("Topics Listed in the config that the tool was not able to create: ")
					ksmisc.PrettyPrintMapSet(ksinternal.FindNonExistentTopicsInClusterMapSet(getTopicListFromKafkaCluster(sca)))
				}
				break
			}
		}
	case MODIFY_TOPIC:
		for {
			refreshTopicList(sca, true)
			ts, ps := FindMismatchedConfigTopics(sca)
			if ts.Cardinality() != 0 && ps.Cardinality() != 0 && i <= 5 {
				time.Sleep(2 * time.Second)
				fmt.Println("The Topics Have not been modified yet. Waiting for Metadata to sync")
				i += 1
			} else {
				if i >= 5 {
					fmt.Println("Retried 5 Times. The sync seems to be failing.")
					fmt.Println("Topics Listed in the config that the tool was not able to modify: ")
					ksmisc.PrettyPrintMapSet(ts)
					fmt.Println("Topics Listed in the config that the tool was not able alter partitions for: ")
					ksmisc.PrettyPrintMapSet(ps)
				}
				break
			}
		}
	case DELETE_TOPIC:
		for {
			refreshTopicList(sca, true)
			if !ksinternal.FindNonExistentTopicsInClusterMapSet(getTopicListFromKafkaCluster(sca)).Equal(ksinternal.GetConfigTopicsAsMapSet()) && i <= 5 {
				time.Sleep(2 * time.Second)
				fmt.Println("The Topics Have not been deleted yet. Waiting for Metadata to sync")
				i += 1
			} else {
				if i >= 5 {
					fmt.Println("Retried 5 Times. The sync seems to be failing.")
					fmt.Println("Topics Listed in the config that the tool was not able to delete: ")
					ksmisc.PrettyPrintMapSet(ksinternal.FindProvisionedTopicsMapSet(getTopicListFromKafkaCluster(sca)))
				}
				break
			}
		}
	}
}

func (t TopicStatusDetails) prettyPrint() {
	fmt.Println("TopicName: ", t.topicName, "Status:", t.status, "Error:", t.errorStr, "\t\tRetry Count:", t.retryCount)
}

func PrettyPrintSaramaTopicDetail(topicName string, td *sarama.TopicDetail) {
	var temp string = ""
	for k, v := range td.ConfigEntries {
		temp += fmt.Sprint(k, "=", *v, " , ")
	}
	logger.Infow("Topic Details",
		"Topic Name", topicName,
		"Replication Factor", td.ReplicationFactor,
		"Partition Count", td.NumPartitions,
		"Other Configurations", temp)
}

func getTopicConfigProperties(topicName string) *sarama.TopicDetail {
	// TODO: Add default Values  in the config file and update it here.
	var td sarama.TopicDetail = sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ReplicaAssignment: nil,
		ConfigEntries:     nil,
	}

	temp := (*ksinternal.ConfMaps.TCM)[topicName]
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