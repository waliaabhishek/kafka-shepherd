package workflowmanagers

import (
	"github.com/waliaabhishek/kafka-shepherd/aclmanagers"
	"github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
	"github.com/waliaabhishek/kafka-shepherd/topicmanagers"
)

var (
	configTopicList     = engine.Shepherd.GetTopicList(true)
	logger              = engine.Shepherd.GetLogger()
	dryRun              = engine.DryRun
	deleteUnknownTopics = engine.SpdCore.Configs.ConfigRoot.ShepherdCoreConfig.DeleteUnknownTopics
	deleteUnknownACLs   = engine.SpdCore.Configs.ConfigRoot.ShepherdCoreConfig.DeleteUnknownACLs
	topicManager        = topicmanagers.SaramaTopicManager
)

func init() {
	kafkamanagers.InitiateAllKafkaConnections(engine.SpdCore.Configs.ConfigRoot)
}

func ExecuteTopicManagementWorkflow(executeCreateFlow bool, executeModifyFlow bool, executeDeleteFlow bool) {
	for k := range engine.ConfMaps.CCM {
		if executeCreateFlow {
			topicManager.CreateTopics(k.Name, configTopicList, dryRun)
		}
		if deleteUnknownTopics && executeDeleteFlow {
			topicManager.DeleteUnknownTopics(k.Name, configTopicList, dryRun)
		}
		if executeModifyFlow {
			topicManager.ModifyTopics(k.Name, dryRun)
		}
	}
}

func ExecuteACLManagementWorkflow(executeCreateFlow bool, executeDeleteFlow bool) {
	for k, v := range engine.ConfMaps.CCM {
		if v.IsACLManagementEnabled {
			aclManager, aclInterface := aclmanagers.GetACLControllerDetails(k.Name, v.ACLManager)
			temp := aclInterface.GenerateACLMappingStructures(k.Name, engine.ShepherdACLList)
			// temp := engine.Shepherd.RenderACLMappings(k.Name, engine.ShepherdACLList, aclInterface)
			if executeCreateFlow {
				aclManager.CreateACL(k.Name, temp, dryRun)
			}
			if deleteUnknownACLs && executeDeleteFlow {
				aclManager.DeleteUnknownACL(k.Name, temp, dryRun)
			}
			continue
		}
		logger.Warnw("ACL management is disabled for the cluster. Skipping ACL Execution.",
			"Cluster Name", k.Name,
			"Cluster Security Protocol", v.ClusterSecurityProtocol.String(),
		)
	}
}

func DeleteShepherdTopics(executeDeleteFlow bool) {
	if engine.IsTest {
		for k := range engine.ConfMaps.CCM {
			if executeDeleteFlow {
				topicManager.DeleteProvisionedTopics(k.Name, configTopicList, dryRun)
			}
		}
	}
}

func DeleteShepherdACLs(executeDeleteFlow bool) {
	if engine.IsTest {
		for k, v := range engine.ConfMaps.CCM {
			if v.IsACLManagementEnabled && executeDeleteFlow {
				aclManager, aclInterface := aclmanagers.GetACLControllerDetails(k.Name, v.ACLManager)
				temp := aclInterface.GenerateACLMappingStructures(k.Name, engine.ShepherdACLList)
				// temp := engine.Shepherd.RenderACLMappings(k.Name, engine.ShepherdACLList, aclInterface)
				aclManager.DeleteProvisionedACL(k.Name, temp, dryRun)
				continue
			}
			logger.Warnw("ACL management is disabled for the cluster. Skipping ACL Execution.",
				"Cluster Name", k.Name,
				"Cluster Security Protocol", v.ClusterSecurityProtocol.String(),
			)
		}
	}
}
