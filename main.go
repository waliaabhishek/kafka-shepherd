package main

import (
	"flag"

	kscm "github.com/waliaabhishek/kafka-shepherd/kafkamanager"

	ksam "github.com/waliaabhishek/kafka-shepherd/kafkamanager/aclmanager"

	kstm "github.com/waliaabhishek/kafka-shepherd/kafkamanager/topicmanager"

	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

func main() {
	ksmisc.DottedLineOutput("Starting New Run", "=", 80)
	flag.Parse()
	defer kscm.CloseAdminConnection()

	kstm.ExecuteRequests(1, kstm.TopicManagementType_CREATE_TOPIC)

	kstm.ExecuteRequests(1, kstm.TopicManagementType_MODIFY_TOPIC)

	kstm.ExecuteRequests(1, kstm.TopicManagementType_DELETE_CONFIG_TOPIC)

	kstm.ExecuteRequests(1, kstm.TopicManagementType_DELETE_UNKNOWN_TOPIC)

	ksam.ExecuteRequests(ksam.ACLManagementType_LIST_CLUSTER_ACL)
	ksam.ExecuteRequests(ksam.ACLManagementType_LIST_CONFIG_ACL)
	ksam.ExecuteRequests(ksam.ACLManagementType_CREATE_ACL)
	ksam.ExecuteRequests(ksam.ACLManagementType_DELETE_UNKNOWN_ACL)
	ksam.ExecuteRequests(ksam.ACLManagementType_DELETE_CONFIG_ACL)
}

// TODO: Get the list of ACL's from UTM List

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
