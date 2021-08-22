package aclmanagers

import (
	"github.com/go-resty/resty/v2"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

type ConfuentRbacACLExecutionManagerImpl struct {
	ACLExecutionManagerBaseImpl
}

var (
	ConfluentRbacACLManager ACLExecutionManager = ConfuentRbacACLExecutionManagerImpl{}
	confRbacAclMappings     *ksengine.ACLMapping
)

const (
	mds_ListRoles             = "/security/1.0/roles"
	mds_GetPrincipalsForRoles = "/security/1.0/lookup/role/{roleName}"
)

/*
	The cluster name is the only known entity for the Engine. The Kafka Connection manager
	operates and maintains all the Kafka Connections. This function is a convenience function
	to find the ConnectionObject and type cast it as a Sarama Cluster Admin connection and use
	it to execute any functionality in this module.
*/
func (c ConfuentRbacACLExecutionManagerImpl) getConnectionObject(clusterName string) *resty.Client {
	return kafkamanagers.Connections[kafkamanagers.KafkaConnectionsKey{ClusterName: clusterName}].Connection.(*kafkamanagers.ConfluentMDSConnection).MDS
}

func (c ConfuentRbacACLExecutionManagerImpl) findRBACClusterDetails(clusterName string) {

}

func (c ConfuentRbacACLExecutionManagerImpl) CreateACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Create Cluster ACLs", "=", 80)
	c.ListClusterACL(clusterName, false)
	createSet := c.FindNonExistentACLsInCluster(clusterName, confRbacAclMappings, ksengine.ConfRBACType_UNKNOWN)
	c.createACLs(clusterName, createSet, dryRun)
}

func (c ConfuentRbacACLExecutionManagerImpl) createACLs(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}

func (c ConfuentRbacACLExecutionManagerImpl) DeleteProvisionedACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}

func (c ConfuentRbacACLExecutionManagerImpl) DeleteUnknownACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}

func (c ConfuentRbacACLExecutionManagerImpl) ListClusterACL(clusterName string, printOutput bool) {
	conn := c.getConnectionObject(clusterName)
	resp, err := conn.R().Get(mds_ListRoles)
	if err != nil {
		logger.Fatalw("Cannot contact the MDS Server. Will not Retry Listing ACL's. Turn on debug for more details.",
			"Status Code", resp.StatusCode(),
			"Error", err)
	}

	type listRolesResp struct {
		Name string `json:"name"`
	}
	r := []listRolesResp{}
	conn.JSONUnmarshal(resp.Body(), &r)

	var userList map[string]interface{}
	for i, lrr := range r {
		resp, err = conn.R().SetPathParam("roleName", lrr.Name).Get(mds_GetPrincipalsForRoles)
		if err != nil {
			logger.Fatalw("Tried Multiple Times to get Principals for RoleName. Cannot Proceed Without MDS Connectivity.",
				"Principal Name", lrr.Name,
				"Error", err)
		}

	}
	panic("not implemented") // TODO: Implement
}

func (c ConfuentRbacACLExecutionManagerImpl) ListConfigACL() {
	panic("not implemented") // TODO: Implement
}
