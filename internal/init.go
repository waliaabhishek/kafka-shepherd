package internal

import (
	ksmisc "shepherd/misc"
	"text/tabwriter"

	"github.com/Shopify/sarama"
)

var bf, df string
var rs RootStruct
var utm UserTopicMapping = UserTopicMapping{}
var tcm TopicConfigMapping = TopicConfigMapping{}
var blueprintMap map[string]NVPairs
var sca *sarama.ClusterAdmin
var TW *tabwriter.Writer = ksmisc.TW

func init() {
	//TODO: Update these values to feed from Config Parser
	bf, df = "./configs/blueprints.yaml", "./configs/definitions_dev.yaml"
	rs = parseConfigurations(bf, df)
	rs.GenerateMappings()
	sca = GetAdminConnection()
}

func GetObjects() (*RootStruct, *UserTopicMapping, *TopicConfigMapping) {
	return &rs, &utm, &tcm
}
