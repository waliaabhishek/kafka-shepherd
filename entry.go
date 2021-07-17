package kafkashepherd

//TODO: Update these values to feed from Config Parser
var bf string = "./configs/001_blueprints.yaml"
var df string = "./configs/102_dev_scoped.yaml"

var rs RootStruct = parseConfigurations(bf, df)
var utm userTopicMapping = userTopicMapping{}
var tcm topicConfigMapping = topicConfigMapping{}
var blueprintMap map[string]NVPairs

func InitObjects() (*userTopicMapping, *topicConfigMapping, *RootStruct) {
	utm := rs.GenerateUserTopicMappings()
	return utm, &tcm, &rs
}
