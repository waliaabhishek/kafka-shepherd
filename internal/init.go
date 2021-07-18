package internal

import (
	"github.com/Shopify/sarama"
	ksmisc "kafkashepherd/misc"
	"text/tabwriter"
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
	bf, df = "./configs/001_blueprints.yaml", "./configs/102_dev_scoped.yaml"
	rs = parseConfigurations(bf, df)
	rs.GenerateMappings()
	sca = GetAdminConnection()
}

func GetObjects() (*RootStruct, *UserTopicMapping, *TopicConfigMapping) {
	return &rs, &utm, &tcm
}
