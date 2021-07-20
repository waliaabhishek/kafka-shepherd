package aclmanager

import (
	"fmt"
	ksinternal "shepherd/internal"
	ksmisc "shepherd/misc"
	"text/tabwriter"

	"github.com/Shopify/sarama"
)

var utm *ksinternal.UserTopicMapping
var tcm *ksinternal.TopicConfigMapping
var tw *tabwriter.Writer = ksmisc.TW
var sca *sarama.ClusterAdmin

func init() {
	_, utm, tcm = ksinternal.GetObjects()
	sca = ksinternal.GetAdminConnection()
}

func GetACLListFromKafkaCluster() {
	acl, err := (*sca).ListAcls(sarama.AclFilter{})
	if err != nil {
		fmt.Println("something went wrong. not sure what.")
	}
	fmt.Println(acl)
}
