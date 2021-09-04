package aclmanagers

import (
	"sync"

	mapset "github.com/deckarep/golang-set"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

type ConfluentRbacACLExecutionManagerImpl struct {
	ACLExecutionManagerBaseImpl
}

var (
	ConfluentRbacACLManager                  ACLExecutionManager                     = ConfluentRbacACLExecutionManagerImpl{}
	confRbacAclMappings                      *ksengine.ACLMapping                    = &ksengine.ACLMapping{}
	confluentRBAC2KafkaPatternTypeConversion map[string]ksengine.KafkaACLPatternType = map[string]ksengine.KafkaACLPatternType{
		"UNKNOWN": ksengine.KafkaACLPatternType_UNKNOWN,
		// "ANY":      ksengine.KafkaACLPatternType_ANY,
		// "MATCH":    ksengine.KafkaACLPatternType_MATCH,
		"LITERAL":  ksengine.KafkaACLPatternType_LITERAL,
		"PREFIXED": ksengine.KafkaACLPatternType_PREFIXED,
	}
	// confluentRBAC2KafkaResourceTypeConversion map[string]ksengine.ACLResourceInterface = map[string]ksengine.ACLResourceInterface{
	// 	"UNKNOWN":                 ksengine.KafkaResourceType_UNKNOWN,
	// 	"ANY":                     ksengine.KafkaResourceType_ANY,
	// 	"TOPIC":                   ksengine.KafkaResourceType_TOPIC,
	// 	"GROUP":                   ksengine.KafkaResourceType_GROUP,
	// 	"CLUSTER":                 ksengine.KafkaResourceType_CLUSTER,
	// 	"TRANSACTIONALID":         ksengine.KafkaResourceType_TRANSACTIONALID,
	// 	"ResourceDelegationToken": ksengine.KafkaResourceType_RESOURCE_DELEGATION_TOKEN,
	// }
)

const (
	kCluster                     = "kafka-cluster"
	cCluster                     = "connect-cluster"
	ksqlCluster                  = "ksql-cluster"
	srCluster                    = "schema-registry-cluster"
	mds_ListRoles                = "/security/1.0/roles"
	mds_GetPrincipalsForRoles    = "/security/1.0/lookup/role/{roleName}"
	mds_GetPrincipalRoleBindings = "/security/1.0/lookup/rolebindings/principal/{pName}"
)

/*
	The cluster name is the only known entity for the Engine. The Kafka Connection manager
	operates and maintains all the Kafka Connections. This function is a convenience function
	to find the ConnectionObject and type cast it as a Sarama Cluster Admin connection and use
	it to execute any functionality in this module.
*/
func (c ConfluentRbacACLExecutionManagerImpl) getConnectionObject(clusterName string) *kafkamanagers.ConfluentMDSConnection {
	return kafkamanagers.Connections[kafkamanagers.KafkaConnectionsKey{ClusterName: clusterName}].Connection.(*kafkamanagers.ConfluentMDSConnection)
}

func (c ConfluentRbacACLExecutionManagerImpl) CreateACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	ksmisc.DottedLineOutput("Create Cluster ACLs", "=", 80)
	c.ListClusterACL(clusterName, false)
	createSet := c.FindNonExistentACLsInCluster(clusterName, confRbacAclMappings, ksengine.ConfRBACType("UNKNOWN"))
	c.createACLs(clusterName, createSet, dryRun)
}

func (c ConfluentRbacACLExecutionManagerImpl) createACLs(clusterName string, in *ksengine.ACLMapping, dryRun bool) {

}

func (c ConfluentRbacACLExecutionManagerImpl) DeleteProvisionedACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}

func (c ConfluentRbacACLExecutionManagerImpl) DeleteUnknownACL(clusterName string, in *ksengine.ACLMapping, dryRun bool) {
	panic("not implemented") // TODO: Implement
}

func (c ConfluentRbacACLExecutionManagerImpl) ListClusterACL(clusterName string, printOutput bool) {
	connObj := c.getConnectionObject(clusterName)
	resp, err := connObj.MDS.R().Get(mds_ListRoles)
	if err != nil {
		logger.Fatalw("Cannot contact the MDS Server. Will not Retry Listing ACL's. Turn on debug for more details.",
			"Status Code", resp.StatusCode(),
			"Error", err)
	}

	type listRolesResp struct {
		Name string `json:"name"`
	}
	r := []listRolesResp{}
	connObj.MDS.JSONUnmarshal(resp.Body(), &r)

	temp := []string{}
	r2 := mapset.NewSet()
	f1 := func(aName string, aVal string) map[string]interface{} {
		if aName != "" {
			return map[string]interface{}{
				"clusters": map[string]interface{}{
					kCluster: connObj.KafkaClusterID,
					aName:    aVal,
				},
			}
		}
		return map[string]interface{}{
			"clusters": map[string]interface{}{
				"kafka-cluster": connObj.KafkaClusterID,
			},
		}
	}
	f2 := func(roleName string, aName string, aVal string) {
		resp, err = connObj.MDS.R().SetBody(f1("", "")).SetPathParam("roleName", roleName).Post(mds_GetPrincipalsForRoles)
		if err != nil {
			logger.Fatalw("Tried Multiple Times to get Principals for RoleName. Cannot Proceed Without MDS Connectivity.",
				"Principal Name", roleName,
				"Error", err)
		}
		connObj.MDS.JSONUnmarshal(resp.Body(), &temp)
		for _, v := range temp {
			r2.Add(v)
		}
	}

	for _, lrr := range r {
		f2(lrr.Name, "", "")
		if connObj.ConnectClusterID != "" {
			f2(lrr.Name, cCluster, connObj.ConnectClusterID)
		}
		if connObj.KSQLClusterID != "" {
			f2(lrr.Name, ksqlCluster, connObj.KSQLClusterID)
		}
		if connObj.ConnectClusterID != "" {
			f2(lrr.Name, srCluster, connObj.SRClusterID)
		}
	}

	type (
		Rb       map[string]interface{}
		Clusters map[string]interface{}
		Scope    struct {
			Clusters Clusters `json:"clusters"`
		}
		rbResp struct {
			Scope Scope `json:"scope"`
			Rb    Rb    `json:"rolebindings"`
		}
	)
	r3 := []rbResp{}
	var wg sync.WaitGroup
	lock := &sync.Mutex{}
	f3 := func(pName string) {
		resp, err := connObj.MDS.R().SetPathParam("pName", pName).Get(mds_GetPrincipalRoleBindings)
		if err != nil {
			logger.Fatalw("Cannot contact the MDS Server. Will not Retry Listing ACL's. Turn on debug for more details.",
				"Status Code", resp.StatusCode(),
				"Error", err)
		}
		connObj.MDS.JSONUnmarshal(resp.Body(), &r3)
		wg.Add(len(r3))
		for _, v := range r3 {
			go c.mapRBACToKafkaACL(v.Scope.Clusters, v.Rb, confRbacAclMappings, &wg, lock)
		}
	}
	for item := range r2.Iter() {
		f3(item.(string))
	}
	wg.Wait()

	panic("not implemented") // TODO: Implement

}
func (c ConfluentRbacACLExecutionManagerImpl) mapRBACToKafkaACL(cluster, rb map[string]interface{}, mapping *ksengine.ACLMapping, wg *sync.WaitGroup, mtx *sync.Mutex) {
	defer wg.Done()
	for user, permMap := range rb {
		perms := permMap.(map[string]interface{})
		for operation, resMap := range perms {
			acl := resMap.(map[string]interface{})
			value := make(map[string]string)
			for k := range acl {
				switch k {
				case kCluster:
					value[kCluster] = cluster[kCluster].(string)
				case cCluster:
					value[cCluster] = cluster[cCluster].(string)
				case ksqlCluster:
					value[ksqlCluster] = cluster[ksqlCluster].(string)
				case srCluster:
					value[srCluster] = cluster[srCluster].(string)
				}
			}
			resType, _ := ksengine.KafkaResourceType_ANY.GetACLResourceValue(acl["resourceType"].(string))
			mtx.Lock()
			mapping.Append(ksengine.ACLDetails{
				ResourceType: resType,
				ResourceName: acl["name"].(string),
				PatternType:  confluentRBAC2KafkaPatternTypeConversion[acl["patternType"].(string)],
				Principal:    user,
				Operation:    ksengine.ConfRBACType(operation),
				Hostname:     "*",
			}, value)
			mtx.Unlock()
		}
	}
}
