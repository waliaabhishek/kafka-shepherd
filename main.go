package main

import (
	kstm "shepherd/manager/topicmanager"
	ksmisc "shepherd/misc"

	"github.com/Shopify/sarama"
)

func main() {
	ksmisc.DottedLineOutput("Starting New Run", "=", 100)
	var sca *sarama.ClusterAdmin = kstm.GetKafkaClusterConnection()
	defer (*sca).Close()

	ksmisc.DottedLineOutput("Create Any Topics as necessary", "=", 100)
	kstm.ExecuteRequests(sca, 10, kstm.CREATE_TOPIC)

	// ksmisc.DottedLineOutput("Modify Any Topics as necessary", "=", 100)
	// kstm.ExecuteRequests(sca, 10, kstm.MODIFY_TOPIC)

	// ksmisc.DottedLineOutput("Delete all topics executed by configs", "=", 100)
	// kstm.ExecuteRequests(sca, 10, kstm.DELETE_TOPIC)

	// kaclm.GetACLListFromKafkaCluster()

	// var value1 string = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkauser\" password=\"kafkapass\""
	// var value3 string = "org.apache.kafka.common.security.plain.PlainLoginModule\n required \nusername='kafkauser' \npassword=\"kafkapass\""
	// // var value2 string = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkauser\" password=\"kafkapass\""

	// fmt.Println(findSASLValues(value1, "username"))
	// fmt.Println(findSASLValues(value1, "password"))

	// fmt.Println(findSASLValues(value3, "username"))
	// fmt.Println(findSASLValues(value3, "password"))

}

// TODO: Get the list of ACL's from UTM List

// TODO: Add Logging. Could possibly look into ZAP for logging.
// https://github.com/uber-go/zap

// TODO: Generate the Baseline YAML files from pre-existing clusters.

// TODO: Full Cluster migration plans.
