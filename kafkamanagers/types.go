package kafkamanagers

import (
	"sync"

	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
)

type KafkaConnections map[KafkaConnectionsKey]KafkaConnectionsValue

type KafkaConnectionsKey struct {
	ClusterName    string
	ConnectionType ConnectionType
}

type KafkaConnectionsValue struct {
	Connection     ConnectionObject
	ConnectionType ConnectionType
	WaitGroupRef   *sync.WaitGroup
	IsInitiated    bool
}

type ConnectionObject interface {
	InitiateAdminConnection(ksengine.ShepherdCluster)
	validateInputDetails(ksengine.ShepherdCluster)
	CloseAdminConnection()
}

type ConnectionObjectBaseImpl struct{}

var (
	Connections KafkaConnections = make(KafkaConnections)
)

func InitiateAllKafkaConnections(clusters ksengine.ConfigRoot) {
	var temp ConnectionType
	f := func(clusterName string, cType ConnectionType) KafkaConnectionsValue {
		v, found := Connections[KafkaConnectionsKey{ClusterName: clusterName, ConnectionType: cType}]
		if !found {
			wg := new(sync.WaitGroup)
			switch cType {
			case ConnectionType_SARAMA, ConnectionType_KAFKA_ACLS:
				key := KafkaConnectionsKey{ClusterName: clusterName, ConnectionType: ConnectionType_SARAMA}
				val := KafkaConnectionsValue{
					Connection:     &SaramaConnection{},
					ConnectionType: ConnectionType_SARAMA,
					WaitGroupRef:   wg,
					IsInitiated:    false,
				}
				Connections[key] = val
				return val
			case ConnectionType_CONFLUENT_MDS:
				key := KafkaConnectionsKey{ClusterName: clusterName, ConnectionType: ConnectionType_CONFLUENT_MDS}
				val := KafkaConnectionsValue{
					Connection:     &ConfluentMDSConnection{},
					ConnectionType: ConnectionType_CONFLUENT_MDS,
					WaitGroupRef:   wg,
					IsInitiated:    false,
				}
				Connections[key] = val
				return val
			}
		}
		return v
	}

	for _, cluster := range clusters.Clusters {
		if cluster.IsEnabled {
			v, err := temp.GetValue(cluster.ACLManager)
			if err != nil {
				logger.Fatalw("Cannot Proceed with unknown Connection Type.",
					"Cluster Name", cluster.Name,
					"Is Enabled", cluster.IsEnabled,
					"ACL Type provided", cluster.ACLManager,
					"Expected Types", temp.stringJoin())
			}
			val := f(cluster.Name, v)
			val.Connection.InitiateAdminConnection(cluster)

			v, err = temp.GetValue(cluster.TopicManager)
			if err != nil {
				logger.Fatalw("Cannot Proceed with unknown Connection Type.",
					"Cluster Name", cluster.Name,
					"Is Enabled", cluster.IsEnabled,
					"ACL Type provided", cluster.ACLManager,
					"Expected Types", temp.stringJoin())
			}
			val = f(cluster.Name, v)
			val.Connection.InitiateAdminConnection(cluster)
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

func (c *ConnectionObjectBaseImpl) generateCustomError(isFatal bool, attrName string, errMsg string) {
	errVal := "Cannot set up connection without the attribute. Exiting process."
	if errMsg != "" {
		errVal = errMsg
	}
	if isFatal {
		logger.Fatalw("Attribute missing but is required to prepare proper connection",
			"Attribute Name", attrName,
			"Error Details", errVal)
	}
	logger.Errorw("Attribute missing but is required to prepare proper connection",
		"Attribute Name", attrName,
		"Error Details", errVal)
}

func (c *ConnectionObjectBaseImpl) executeBaseValidations(cConfig *ksengine.ShepherdCluster) {
	if cConfig.TLSDetails.Enable2WaySSL {
		if cConfig.TLSDetails.ClientCert == "" {
			c.generateCustomError(true, "cluster.tlsDetails.clientCert", "2 Way SSL is enabled. Need Keystore.")
		}
		if cConfig.TLSDetails.PrivateKey == "" {
			c.generateCustomError(true, "cluster.tlsDetails.privateKey", "2 Way SSL is enabled. Need Keystore Password.")
		}
	}
}
