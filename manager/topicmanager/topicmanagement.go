package topicmanager

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	ksinternal "kafkashepherd/internal"
	ksmisc "kafkashepherd/misc"

	"github.com/Shopify/sarama"
	mapset "github.com/deckarep/golang-set"
)

var topicsInCluster ksinternal.TopicNamesSet
var topicsInConfig ksinternal.TopicNamesSet
var utm *ksinternal.UserTopicMapping
var tcm *ksinternal.TopicConfigMapping
var tw *tabwriter.Writer = ksmisc.TW
var sca *sarama.ClusterAdmin

// var rs *ksinternal.RootStruct

/* This is the function that Initializes User Topic mapping structure
after parsing the configurations from input files. This also instantiates the configuration structure.
*/
func init() {
	_, utm, tcm = ksinternal.GetObjects()
	topicsInConfig = getTopicListFromUTMList(utm)
	sca = ksinternal.GetAdminConnection()
}

func GetKafkaClusterConnection() (*sarama.ClusterAdmin) {
	return sca
}

func getTopicListFromUTMList(utm *ksinternal.UserTopicMapping) ksinternal.TopicNamesSet {
	t := mapset.NewSet()
	for _, v := range *utm {
		for _, topic := range v.TopicList {
			if ksmisc.IsTopicName(topic, ".") {
				t.Add(topic)
			}
		}
	}
	return t
}

/* This function returns the list of topics in Kafka Cluster.
 */
func GetTopicListFromKafkaCluster(sca *sarama.ClusterAdmin) ksinternal.TopicNamesSet {
	tSet := mapset.NewSet()
	if topics, err := (*sca).ListTopics(); err != nil {
		//TODO: Change Error Handling
		fmt.Println("Something Went Wrong while Listing Topics. Here are the details: ", err)
	} else {
		for t := range topics {
			tSet.Add(string(t))
		}
	}
	return tSet
}

func refreshTopicList(sca *sarama.ClusterAdmin, discardConnectionCache bool) {
	if topicsInCluster == nil || discardConnectionCache {
		topicsInCluster = GetTopicListFromKafkaCluster(sca)
	}
	if topicsInConfig == nil {
		topicsInConfig = getTopicListFromUTMList(utm)
	}
}

/* Create a Struct for Channel signature
 */
type TopicStatusDetails struct {
	topicName  string
	status     StatusType
	errorStr   string
	retryCount int
}

/*	This function compares the Configurations in the input files with the topics existing in the Kafka Cluster.
Then it returns only the topics that do not exist on the Kafka Cluster.
*/
func FindNonExistentTopicsInKafkaCluster(sca *sarama.ClusterAdmin) ksinternal.TopicNamesSet {
	refreshTopicList(sca, false)
	return topicsInConfig.Difference(topicsInCluster)
}

/* This function returns the topics that are present on the Kafka Cluster but are not present in the Configuration input.
 */
func FindNonExistentTopicsInTopicsConfig(sca *sarama.ClusterAdmin) ksinternal.TopicNamesSet {
	refreshTopicList(sca, false)
	return topicsInCluster.Difference(topicsInConfig).Difference(topicsInConfig)
}

/* This function returns the topic list that is part of the config and already exists in the cluster.
 */
func FindProvisionedTopics(sca *sarama.ClusterAdmin) ksinternal.TopicNamesSet {
	refreshTopicList(sca, false)
	return topicsInConfig.Intersect(topicsInCluster)
}

/* This function takes the ksinternal.TopicManagementFunctionType constant and executes the CREATE, MODIFY or DELETE request.
This function takes care of all internals and does not need any other details as it fetches these details from
configuration files and kafka cluster.
*/
func ExecuteRequests(sca *sarama.ClusterAdmin, threadCount int, requestType TopicManagementFunctionType) {
	refreshTopicList(sca, true)
	c := make(chan TopicStatusDetails, threadCount)
	rand.Seed(time.Now().UnixNano())
	var ts ksinternal.TopicNamesSet
	var ps ksinternal.TopicNamesSet
	counter := 0

	switch requestType {
	case CREATE_TOPIC:
		ts = FindNonExistentTopicsInKafkaCluster(sca)
	case MODIFY_TOPIC:
		ts, ps = FindMismatchedConfigTopics(sca)
		ksmisc.DottedLineOutput("Topics in Modify List", "=", 100)
		ksmisc.DottedLineOutput("Topics in Partition List", "=", 100)
	case DELETE_TOPIC:
		ts = FindProvisionedTopics(sca)
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
			if FindNonExistentTopicsInKafkaCluster(sca).Cardinality() != 0 && i <= 5 {
				time.Sleep(2 * time.Second)
				fmt.Println("The Topics Have not been created yet. Waiting for Metadata to sync")
				i += 1
			} else {
				if i >= 5 {
					fmt.Println("Retried 5 Times. The sync seems to be failing.")
					fmt.Println("Topics Listed in the config that the tool was not able to create: ")
					ksmisc.PrettyPrintMapSet(FindNonExistentTopicsInKafkaCluster(sca))
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
			if !FindNonExistentTopicsInKafkaCluster(sca).Equal(getTopicListFromUTMList(utm)) && i <= 5 {
				time.Sleep(2 * time.Second)
				fmt.Println("The Topics Have not been deleted yet. Waiting for Metadata to sync")
				i += 1
			} else {
				if i >= 5 {
					fmt.Println("Retried 5 Times. The sync seems to be failing.")
					fmt.Println("Topics Listed in the config that the tool was not able to delete: ")
					ksmisc.PrettyPrintMapSet(FindProvisionedTopics(sca))
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
	tw.Flush()
	var temp string = ""
	for k, v := range td.ConfigEntries {
		temp += fmt.Sprint(k, "=", *v, " , ")
	}
	fmt.Fprintf(tw, "\nTopic:%s\tRF:%d\tPartitions:%d\tOther Configs:%s", topicName, td.ReplicationFactor, td.NumPartitions, temp)
}

func getTopicConfigProperties(topicName string) *sarama.TopicDetail {
	// TODO: Add default Values  in the config file and update it here.
	var td sarama.TopicDetail = sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ReplicaAssignment: nil,
		ConfigEntries:     nil,
	}

	temp := (*tcm)[topicName]
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

func FindMismatchedConfigTopics(sca *sarama.ClusterAdmin) (configDiff ksinternal.TopicNamesSet, partitionDiff ksinternal.TopicNamesSet) {
	configDiff = mapset.NewSet()
	partitionDiff = mapset.NewSet()
	if topics, err := (*sca).ListTopics(); err != nil {
		//TODO: Change Error Handling
		fmt.Println("Something Went Wrong while Listing Topics. Here are the details: ", err)
	} else {
		for k, v := range topics {
			if (*tcm)[k] != nil {
				_ = compareTopicDetails(k, &v, (*tcm)[k], &configDiff, &partitionDiff)
			}
		}
	}
	return
}

func compareTopicDetails(topicName string, first *sarama.TopicDetail, second ksinternal.NVPairs, configDiff *ksinternal.TopicNamesSet, partitionDiff *ksinternal.TopicNamesSet) bool {
	flag := true
	for k, v := range second {
		switch k {
		case "num.partitions":
			if v != strconv.FormatInt(int64(first.NumPartitions), 10) {
				(*partitionDiff).Add(topicName)
				flag = false
			}
		case "replication.factor", "default.replication.factor":
			if v != strconv.FormatInt(int64(first.ReplicationFactor), 10) {
				(*configDiff).Add(topicName)
				flag = false
			}
		default:
			if v != *first.ConfigEntries[k] {
				(*configDiff).Add(topicName)
				flag = false
			}
		}
	}
	return flag
}
