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
	validateInputDetails(ksengine.ShepherdCluster)
	CloseAdminConnection()
}

type ConnectionObjectBaseImpl struct{}

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
			if v == ACLType_KAFKA_ACLS {
				wg := new(sync.WaitGroup)
				k := KafkaConnectionsKey{ClusterName: cluster.Name}
				v := KafkaConnectionsValue{
					Connection:   &SaramaConnection{},
					ACLType:      ACLType_KAFKA_ACLS,
					WaitGroupRef: wg,
				}
				v.Connection.InitiateAdminConnection(cluster)
				Connections[k] = v
				continue
			}
			if v == ACLType_CONFLUENT_RBAC {
				k := KafkaConnectionsKey{ClusterName: cluster.Name}
				v := KafkaConnectionsValue{
					Connection: &ConfluentMDSConnection{},
					ACLType:    ACLType_CONFLUENT_RBAC,
				}
				v.Connection.InitiateAdminConnection(cluster)
				Connections[k] = v
				continue
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
