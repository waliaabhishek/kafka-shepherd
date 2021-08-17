package main

import (
	workflow "github.com/waliaabhishek/kafka-shepherd/workflowmanagers"
)

func main() {
	workflow.ExecuteTopicManagementWorkflow(true, true, true)
	workflow.ExecuteACLManagementWorkflow(true, true)
	workflow.DeleteShepherdTopics(true)
	workflow.DeleteShepherdACLs(true)
}

// TODO: Generate the Baseline YAML files from pre-existing clusters.

// TODO: Full Cluster migration plans.

/*
	Current Status:
		PLAINTEXT:					Working
		SASL_PLAINTEXT_PLAIN		Working
		1 Way SSL 					Working
		Cert Veri Enabled 			Not Working
		SASL_SSL					??
		SASL_SCRAM					Working
*/
