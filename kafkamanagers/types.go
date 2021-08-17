package kafkamanagers

import (
	"sync"

	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
)

type KafkaConnections map[KafkaConnectionsKey]KafkaConnectionsValue

type KafkaConnectionsKey struct {
	ClusterName string
}

type KafkaConnectionsValue struct {
	Connection   ConnectionObject
	ACLType      ACLType
	WaitGroupRef *sync.WaitGroup
}

type ConnectionObject interface {
	InitiateAdminConnection(ksengine.ShepherdCluster)
	CloseAdminConnection()
}

var (
	Connections KafkaConnections = make(KafkaConnections)
)

func InitiateAllKafkaConnections(clusters ksengine.ConfigRoot) {
	var temp ACLType
	for _, cluster := range clusters.Clusters {
		v, err := temp.GetValue(cluster.ACLType)
		if err != nil {
			logger.Fatalw("Cannot Proceed with unknown aclType.",
				"Cluster Name", cluster.Name,
				"Is Enabled", cluster.IsEnabled,
				"ACL Type provided", cluster.ACLType,
				"Expected Types", temp.stringJoin())
		}
		if cluster.IsEnabled {
			if v == ConnectionType_KAFKA_ACLS {
				wg := new(sync.WaitGroup)
				k := KafkaConnectionsKey{ClusterName: cluster.Name}
				v := KafkaConnectionsValue{
					Connection:   &SaramaConnection{},
					ACLType:      ConnectionType_KAFKA_ACLS,
					WaitGroupRef: wg,
				}
				v.Connection.InitiateAdminConnection(cluster)
				Connections[k] = v
			}
			if v == ConnectionType_CONFLUENT_RBAC {
				logger.Fatal("Confluent RBAC implementation is not available yet.")
				// TODO : Implement Confluent GO Client Handler
			}
		}
	}
}

func CloseAllKafkaConnections() {
	wg := new(sync.WaitGroup)
	for _, v := range Connections {
		go v.Connection.CloseAdminConnection()
	}
	wg.Wait()
}
