package aclmanagers

import (
	"github.com/Shopify/sarama"
	ksengine "github.com/waliaabhishek/kafka-shepherd/new/engine"
	"github.com/waliaabhishek/kafka-shepherd/new/kafkamanagers"
)

type SaramaACLManagerImpl struct {
	ACLManagerBaseImpl
}

var (
	SaramaACLManager ACLManager = SaramaACLManagerImpl{}
)

/*
	The cluster name is the only known entity for the Engine. The Kafka Connection manager
	operates and maintains all the Kafka Connections. This function is a convenience function
	to find the ConnectionObject and type cast it as a Sarama Cluster Admin connection and use
	it to execute any functionality in this module.
*/
func (t SaramaACLManagerImpl) getSaramaConnectionObject(clusterName string) *sarama.ClusterAdmin {
	return kafkamanagers.Connections[kafkamanagers.KafkaConnectionsKey{ClusterName: clusterName}].Connection.(*kafkamanagers.SaramaConnection).SCA
}

func (s SaramaACLManagerImpl) ListACL() {
	panic("not implemented") // TODO: Implement
}

func (s SaramaACLManagerImpl) CreateACL(in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}

func (s SaramaACLManagerImpl) DeleteProvisionedACL(in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}

func (s SaramaACLManagerImpl) DeleteUnknownACL(in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}
