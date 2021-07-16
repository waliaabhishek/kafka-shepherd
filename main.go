package main

import (
	shepherd "kafkashepherd/core"
)

func main() {
	shepherd.DottedLineOutput("Starting New Run", "=", 100)
	sca := shepherd.GetAdminConnection()
	defer sca.Close()

	// _, tcm, _ := shepherd.InitObjects()
	shepherd.InitObjects()
	// fmt.Println("Rootstruct", rs.Blueprint)
	// shepherd.DottedLineOutput("Topic Config Mapping Values", "=", 100)
	// tcm.PrettyPrintTCM()

	shepherd.DottedLineOutput("Create Any Topics as necessary", "=", 100)
	// kafkaopsmanager.PrettyPrintMapSet(kafkaopsmanager.FindNonExistentTopicsInKafkaCluster(&sca))
	shepherd.ExecuteRequests(&sca, 10, shepherd.CREATE_TOPIC)

	shepherd.DottedLineOutput("Modify Any Topics as necessary", "=", 100)
	shepherd.ExecuteRequests(&sca, 10, shepherd.MODIFY_TOPIC)

	shepherd.DottedLineOutput("Delete all topics executed by configs", "=", 100)
	shepherd.ExecuteRequests(&sca, 10, shepherd.DELETE_TOPIC)
}

// TODO: Get the list of ACL's from UTM List

// TODO: Add Logging. Could possibly look into ZAP for logging.
// https://github.com/uber-go/zap

// TODO: Generate the Baseline YAML files from pre-existing clusters.

// TODO: Full Cluster migration plans.
