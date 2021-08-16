package engine

import (
	mapset "github.com/deckarep/golang-set"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
	"go.uber.org/zap"
)

const (
	ENVVAR_PREFIX string = "env::"
)

var ()

var (
	topicsInConfig mapset.Set = mapset.NewSet()
)

type (
	ShepherdManager interface {
		GetTopicList() *mapset.Set
		// GetShepherdACLList() *ACLMapping
		RenderACLMappings(clusterName string, mappings *ACLMapping, inputACLType ACLOperationsInterface) *ACLMapping
		GetLogger() ShepherdLogger
	}

	ShepherdManagerBaseImpl struct {
		ShepherdTopicManagerBaseImpl
		ShepherdACLManagerBaseImpl
	}

	ShepherdTopicManagerBaseImpl struct{}

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
func (s *ShepherdManagerBaseImpl) RenderACLMappings(in *ACLMapping, needType ACLOperationsInterface) *ACLMapping {
	return needType.generateACLMappingStructures(in)
}

/*
	This method generates a Set of Topic Names that does not include the
	(.*) suffixed topics. Technically, this is the unique list of topics
	that the configuration is expecting to be created.
*/
func (s *ShepherdTopicManagerBaseImpl) GetTopicList() mapset.Set {
	if topicsInConfig.Cardinality() == 0 {
		for _, v := range ConfMaps.utm {
			for _, topic := range v.TopicList {
				if ksmisc.IsTopicName(topic, SpdCore.Configs.ConfigRoot.ShepherdCoreConfig.SeperatorToken) {
					topicsInConfig.Add(topic)
				}
			}
		}
	}
	return topicsInConfig
}

func GetLogger() (lg *ShepherdLogger) {
	return &ShepherdLogger{
		logger,
	}
}
