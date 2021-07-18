package main

import (
	kstm "kafkashepherd/manager/topicmanager"
	ksmisc "kafkashepherd/misc"
)

func main() {
	ksmisc.DottedLineOutput("Starting New Run", "=", 100)
	sca := kstm.SCA
	defer sca.Close()

	ksmisc.DottedLineOutput("Create Any Topics as necessary", "=", 100)
	// kafkaopsmanager.PrettyPrintMapSet(kafkaopsmanager.FindNonExistentTopicsInKafkaCluster(&sca))
	kstm.ExecuteRequests(&sca, 10, kstm.CREATE_TOPIC)

	ksmisc.DottedLineOutput("Modify Any Topics as necessary", "=", 100)
	kstm.ExecuteRequests(&sca, 10, kstm.MODIFY_TOPIC)

	ksmisc.DottedLineOutput("Delete all topics executed by configs", "=", 100)
	kstm.ExecuteRequests(&sca, 10, kstm.DELETE_TOPIC)
}

// TODO: Get the list of ACL's from UTM List

// TODO: Add Logging. Could possibly look into ZAP for logging.
// https://github.com/uber-go/zap

// TODO: Generate the Baseline YAML files from pre-existing clusters.

// TODO: Full Cluster migration plans.
