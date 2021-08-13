package topicmanagers

import (
	"sort"

	mapset "github.com/deckarep/golang-set"
	ksengine "github.com/waliaabhishek/kafka-shepherd/new/engine"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/new/misc"
)

var (
	logger = ksengine.GetLogger()
)

type TopicManager interface {
	GetTopicsAsSet(clusterName string) *mapset.Set
	CreateTopics(clusterName string, topics mapset.Set, dryRun bool)
	ModifyTopics(clusterName string, dryRun bool)
	DeleteProvisionedTopics(clusterName string, topics mapset.Set, dryRun bool)
	DeleteUnknownTopics(clusterName string, topics mapset.Set, dryRun bool)
}

type TopicManagerBaseImpl struct{}

/*
	This is a default implementation dependent on the mapset returns by the
	GetTopicsAsSet implemented by your actual implementation.
*/
func (t TopicManagerBaseImpl) GetTopicsAsSlice(in mapset.Set) []string {
	ret := ksmisc.GetStringSliceFromMapSet(in)
	sort.Strings(ret)
	return ret
}

/*
	List all the topic names provided in the mapset as a formatted output.
*/
func (t TopicManagerBaseImpl) ListTopics(in mapset.Set) {
	ksmisc.DottedLineOutput("Topic List", "=", 80)
	for idx, item := range t.GetTopicsAsSlice(in) {
		logger.Infof("%04d# : %s", idx+1, item)
	}
	ksmisc.DottedLineOutput("", "=", 80)
}
