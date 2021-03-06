package engine

import (
	mapset "github.com/deckarep/golang-set"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
	"go.uber.org/zap"
)

const (
	ENVVAR_PREFIX string = "env::"
)

var (
	topicsInConfig mapset.Set      = mapset.NewSet()
	Shepherd       ShepherdManager = ShepherdManagerBaseImpl{}
)

type (
	ShepherdManager interface {
		GetTopicList(forceRefresh bool) mapset.Set
		// GetShepherdACLList() *ACLMapping
		RenderACLMappings(clusterName string, mappings *ACLMapping, inputACLType ACLOperationsInterface) *ACLMapping
		GetLogger() *ShepherdLogger
	}

	ShepherdManagerBaseImpl struct {
		ShepherdTopicManagerImpl
		ShepherdACLConfigManager
	}

	ShepherdTopicManagerImpl struct{}

	ShepherdLogger struct {
		*zap.SugaredLogger
	}
)

/*
	The function takes in the input as the ACLMapping struct which is then processed and converted to the respective ACLs that you desire
	as per the needType value. The response provided is 3 channels that will produce arbitrary number of ACLs as per your requirements.
	The sChannel caters to the ACLs that were successfully parsed and should be executed.
	The fChannel caters to the ACLs that did not fit any bill as per the core determination functions  and needs further analysis from the caller.
	The finished Channel only produces a boolean value once (true) for you to understand that the ACL emission is completed and you can logically
	close your select loops (if any).
*/
func (s ShepherdManagerBaseImpl) RenderACLMappings(clusterName string, in *ACLMapping, needType ACLOperationsInterface) *ACLMapping {
	return needType.GenerateACLMappingStructures(clusterName, in)
}

/*
	This method generates a Set of Topic Names that does not include the
	(.*) suffixed topics. Technically, this is the unique list of topics
	that the configuration is expecting to be created.
*/
func (s ShepherdTopicManagerImpl) GetTopicList(forceRefresh bool) mapset.Set {
	if forceRefresh || topicsInConfig.Cardinality() == 0 {
		topicsInConfig = mapset.NewSet()
		for topicName := range ConfMaps.TCM {
			if ksmisc.IsTopicName(topicName, SpdCore.Configs.ConfigRoot.ShepherdCoreConfig.SeperatorToken) {
				topicsInConfig.Add(topicName)
			}
		}
	}
	return topicsInConfig
}

func (s ShepherdManagerBaseImpl) GetLogger() (lg *ShepherdLogger) {
	return &ShepherdLogger{
		logger,
	}
}
