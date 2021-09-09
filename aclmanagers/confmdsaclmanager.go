package aclmanagers

import (
	"fmt"
	"strings"
	"sync"

	mapset "github.com/deckarep/golang-set"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/kafkamanagers"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

type (
	ConfluentRBACOperation               string
	ConfluentRbacACLExecutionManagerImpl struct {
		ACLExecutionManagerBaseImpl
		ksengine.ShepherdACLConfigManagerBaseImpl
		ConfluentRBACOperation
	}
)

const (
	kCluster                     = "kafka-cluster"
	cCluster                     = "connect-cluster"
	ksqlCluster                  = "ksql-cluster"
	srCluster                    = "schema-registry-cluster"
	mds_ListRoles                = "/security/1.0/roles"
	mds_GetPrincipalsForRoles    = "/security/1.0/lookup/role/{roleName}"
	mds_GetPrincipalRoleBindings = "/security/1.0/lookup/rolebindings/principal/{pName}"
	mds_CreateRoleBindings       = "/security/1.0/principals/{pName}/roles/{roleName}/bindings"
)

var (
	ConfluentRbacACLManager                  ACLExecutionManager                     = ConfluentRbacACLExecutionManagerImpl{}
	ConfACLManager                           ksengine.ShepherdACLConfigManager       = ConfluentRbacACLExecutionManagerImpl{}
	confRbacAclMappings                      *ksengine.ACLMapping                    = &ksengine.ACLMapping{}
	confluentRBAC2KafkaPatternTypeConversion map[string]ksengine.KafkaACLPatternType = map[string]ksengine.KafkaACLPatternType{
		"UNKNOWN":  ksengine.KafkaACLPatternType_UNKNOWN,
		"LITERAL":  ksengine.KafkaACLPatternType_LITERAL,
		"PREFIXED": ksengine.KafkaACLPatternType_PREFIXED,
	}
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
	createSet := c.FindNonExistentACLsInCluster(clusterName, confRbacAclMappings, ConfluentRBACOperation("Unknown"))
	c.createACLs(clusterName, createSet, dryRun)
}

func (c ConfluentRbacACLExecutionManagerImpl) createACLs(clusterName string, in *ksengine.ACLMapping, dryRun bool) {

	type (
		mappingKey struct {
			principal         string
			role              ksengine.ACLOperationsInterface
			kafkaCluster      string
			otherClusterName  string
			otherClusterValue string
		}
		resources struct {
			resourceType string
			name         string
			patternType  string
		}
		mappingTable map[mappingKey][]resources

		Clusters map[string]interface{}
		Scope    struct {
			Clusters Clusters `json:"clusters"`
		}
		rbReq struct {
			scope Scope       `json:"scope"`
			Rb    []resources `json:"resourcePatterns"`
		}
	)

	mappingCache := make(mappingTable)

	for k, v := range *in {
		var key mappingKey
		key.principal = k.Principal
		key.role = k.Operation
		for cName, cVal := range v.(map[string]string) {
			switch cName {
			case kCluster:
				key.kafkaCluster = cVal
			case cCluster:
				key.otherClusterName = cCluster
				key.otherClusterValue = cVal
			case srCluster:
				key.otherClusterName = srCluster
				key.otherClusterValue = cVal
			case ksqlCluster:
				key.otherClusterName = ksqlCluster
				key.otherClusterValue = cVal
			}
		}

		if mapRes, found := mappingCache[key]; found {
			mappingCache[key] = append(mapRes, resources{resourceType: k.ResourceType.GetACLResourceString(), name: k.ResourceName, patternType: k.PatternType.GetACLPatternString()})
		} else {
			mappingCache[key] = []resources{resources{resourceType: k.ResourceType.GetACLResourceString(), name: k.ResourceName, patternType: k.PatternType.GetACLPatternString()}}
		}
	}

	execRB := func(mapKey mappingKey, mapVal []resources, wg *sync.WaitGroup) {
		defer wg.Done()

		var cluster Clusters = c.createClustersObject(clusterName, mapKey.otherClusterName, mapKey.otherClusterValue)
		req := &rbReq{
			scope: Scope{cluster},
			Rb:    mapVal,
		}
		connObj := c.getConnectionObject(clusterName)

		resp, err := connObj.MDS.R().SetBody(req).SetPathParams(map[string]string{"pName": mapKey.principal, "roleName": mapKey.role.String()}).Post(mds_CreateRoleBindings)
		if err != nil {
			logger.Fatalw("Cannot contact the MDS Server. Will not Retry Creating ACL's. Turn on debug for more details.",
				"Status Code", resp.StatusCode(),
				"Error", err)
		}

		if resp.StatusCode() == 204 {
			logger.Debugw("Role Binding has been created.",
				"URI", resp.Request.URL,
				"Request Body", resp.Request.Body)
		}

	}

	wg_int := new(sync.WaitGroup)
	wg_int.Add(len(mappingCache))
	for k, v := range mappingCache {
		go execRB(k, v, wg_int)
	}

	wg_int.Wait()
	logger.Infof("All Rolebindings have been created. Total RoleBinding Creation Requests Executed: %d", len(mappingCache))
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
	f2 := func(roleName string, aName string, aVal string) {
		resp, err = connObj.MDS.R().SetBody(c.createClustersObject(clusterName, "", "")).SetPathParam("roleName", roleName).Post(mds_GetPrincipalsForRoles)
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
	wg := new(sync.WaitGroup)
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
			go c.mapRBACToACLMapping(v.Scope.Clusters, v.Rb, confRbacAclMappings, wg, lock)
		}
	}
	for item := range r2.Iter() {
		f3(item.(string))
	}
	wg.Wait()
}

func (c ConfluentRbacACLExecutionManagerImpl) mapRBACToACLMapping(cluster, rb map[string]interface{}, mapping *ksengine.ACLMapping, wg *sync.WaitGroup, mtx *sync.Mutex) {
	defer wg.Done()
	value := make(map[string]string)
	for k, v := range cluster {
		v := v.(string)
		switch k {
		case kCluster:
			value[kCluster] = v
		case cCluster:
			value[cCluster] = v
		case ksqlCluster:
			value[ksqlCluster] = v
		case srCluster:
			value[srCluster] = v
		}
	}
	for user, permMap := range rb {
		for perm, resMap := range permMap.(map[string]interface{}) {
			for _, acl := range resMap.([]map[string]string) {
				resType, _ := ksengine.KafkaResourceType_ANY.GetACLResourceValue(acl["resourceType"])
				mtx.Lock()
				mapping.Append(ksengine.ACLDetails{
					ResourceType: resType,
					ResourceName: acl["name"],
					PatternType:  confluentRBAC2KafkaPatternTypeConversion[acl["patternType"]],
					Principal:    user,
					Operation:    ConfluentRBACOperation(perm),
					Hostname:     "*",
				}, value)
				mtx.Unlock()
			}
		}
	}
}

func (c ConfluentRbacACLExecutionManagerImpl) mapFromShepherdACL(clusterName string, in *ksengine.ACLMapping, out *ksengine.ACLMapping, failed *ksengine.ACLMapping) {
	connObj := c.getConnectionObject(clusterName)
	for k, v := range *in {
		value := make(ksengine.NVPairs)
		value[kCluster] = connObj.KafkaClusterID
		switch k.Operation {
		case ksengine.ShepherdClientType_PRODUCER:
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_TOPIC, k.ResourceName, c.determinePatternType(k.ResourceName),
				k.Principal, ConfluentRBACOperation("DeveloperWrite"), k.Hostname), value)
			if c.determinePatternType(k.ResourceName) == ksengine.KafkaACLPatternType_LITERAL && connObj.SRClusterID != "" {
				value[srCluster] = connObj.SRClusterID
				out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_SUBJECT, fmt.Sprintf("%s-key", k.ResourceName), ksengine.KafkaACLPatternType_LITERAL,
					k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
				out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_SUBJECT, fmt.Sprintf("%s-value", k.ResourceName), ksengine.KafkaACLPatternType_LITERAL,
					k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
			}
		case ksengine.ShepherdClientType_PRODUCER_IDEMPOTENCE:
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_CLUSTER, "kafka-cluster", ksengine.KafkaACLPatternType_LITERAL,
				k.Principal, ConfluentRBACOperation("DeveloperWrite"), k.Hostname), value)
		case ksengine.ShepherdClientType_TRANSACTIONAL_PRODUCER:
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_TRANSACTIONALID, k.ResourceName, ksengine.KafkaACLPatternType_LITERAL,
				k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
		case ksengine.ShepherdClientType_CONSUMER:
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_TOPIC, k.ResourceName, c.determinePatternType(k.ResourceName),
				k.Principal, ConfluentRBACOperation("DeveloperRead"), k.Hostname), value)
			if c.determinePatternType(k.ResourceName) == ksengine.KafkaACLPatternType_LITERAL && connObj.SRClusterID != "" {
				value[srCluster] = connObj.SRClusterID
				out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_SUBJECT, fmt.Sprintf("%s-key", k.ResourceName), ksengine.KafkaACLPatternType_LITERAL,
					k.Principal, ConfluentRBACOperation("DeveloperRead"), k.Hostname), value)
				out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_SUBJECT, fmt.Sprintf("%s-value", k.ResourceName), ksengine.KafkaACLPatternType_LITERAL,
					k.Principal, ConfluentRBACOperation("DeveloperRead"), k.Hostname), value)
			}
		case ksengine.ShepherdClientType_CONSUMER_GROUP:
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_GROUP, k.ResourceName, ksengine.KafkaACLPatternType_PREFIXED,
				k.Principal, ConfluentRBACOperation("DeveloperRead"), k.Hostname), value)
		case ksengine.ShepherdClientType_SOURCE_CONNECTOR, ksengine.ShepherdClientType_SINK_CONNECTOR:
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_TOPIC, k.ResourceName, c.determinePatternType(k.ResourceName),
				k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
			if c.determinePatternType(k.ResourceName) == ksengine.KafkaACLPatternType_LITERAL && connObj.SRClusterID != "" {
				value[srCluster] = connObj.SRClusterID
				out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_SUBJECT, fmt.Sprintf("%s-key", k.ResourceName), ksengine.KafkaACLPatternType_LITERAL,
					k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
				out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_SUBJECT, fmt.Sprintf("%s-value", k.ResourceName), ksengine.KafkaACLPatternType_LITERAL,
					k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
			}
		case ksengine.ShepherdClientType_STREAM_READ, ksengine.ShepherdClientType_STREAM_WRITE:
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_TOPIC, k.ResourceName, ksengine.KafkaACLPatternType_PREFIXED,
				k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_GROUP, k.ResourceName, ksengine.KafkaACLPatternType_PREFIXED,
				k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
			if connObj.SRClusterID != "" {
				value[srCluster] = connObj.SRClusterID
				out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_SUBJECT, k.ResourceName, ksengine.KafkaACLPatternType_PREFIXED,
					k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
			}
		case ksengine.ShepherdClientType_KSQL:
			value[ksqlCluster] = connObj.KSQLClusterID
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_KSQL_CLUSTER, "ksql-cluster", ksengine.KafkaACLPatternType_LITERAL,
				k.Principal, ConfluentRBACOperation("DeveloperWrite"), k.Hostname), value)
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_GROUP, fmt.Sprintf("_confluent-ksql-%s", k.ResourceName), ksengine.KafkaACLPatternType_PREFIXED,
				k.Principal, ConfluentRBACOperation("DeveloperRead"), k.Hostname), value)
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_TOPIC, fmt.Sprintf("%sksql_processing_log", k.ResourceName), ksengine.KafkaACLPatternType_LITERAL,
				k.Principal, ConfluentRBACOperation("DeveloperRead"), k.Hostname), value)
			out.Append(c.constructACLDetailsObject(ksengine.KafkaResourceType_TOPIC, fmt.Sprintf("_confluent-ksql-%stransient", k.ResourceName), ksengine.KafkaACLPatternType_PREFIXED,
				k.Principal, ConfluentRBACOperation("ResourceOwner"), k.Hostname), value)
		default:
			logger.Warnf("Conversion from %T type to %T type is not supported yet. The ACL mapping will be added to the Failed list.", k.Operation, c)
			failed.Append(k, v)
		}
	}
}

func (c ConfluentRbacACLExecutionManagerImpl) mapToShepherdACL(clusterName string, in *ksengine.ACLMapping, out *ksengine.ACLMapping, failed *ksengine.ACLMapping) {
	// TODO: Convert Confluent ACL's back to the Shepherd ACL format for interconversion support
	for k, v := range *in {
		if k.ResourceType != ksengine.KafkaResourceType_TOPIC || k.ResourceType != ksengine.KafkaResourceType_CLUSTER || k.ResourceType != ksengine.KafkaResourceType_GROUP || k.ResourceType != ksengine.KafkaResourceType_TRANSACTIONALID {
			logger.Warnf("Resource Type %s is not supported as they may not have a logical conversion to the Shepherd ACLs. Adding to the list of Failed ACLs.", k.ResourceType.GetACLResourceString())
			failed.Append(k, v)
			continue
		}
		switch k.Operation {
		case ConfluentRBACOperation("DeveloperRead"):
			if k.ResourceType == ksengine.KafkaResourceType_TOPIC {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_CONSUMER, k.Hostname), nil)
			}
			if k.ResourceType == ksengine.KafkaResourceType_GROUP {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_CONSUMER_GROUP, k.Hostname), nil)
			}
		case ConfluentRBACOperation("DeveloperWrite"):
			if k.ResourceType == ksengine.KafkaResourceType_TOPIC {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_PRODUCER, k.Hostname), nil)
			}
			if k.ResourceType == ksengine.KafkaResourceType_CLUSTER {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_PRODUCER_IDEMPOTENCE, k.Hostname), nil)
			}
			if k.ResourceType == ksengine.KafkaResourceType_TRANSACTIONALID {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_TRANSACTIONAL_PRODUCER, k.Hostname), nil)
			}
		case ConfluentRBACOperation("ResourceOwner"):
			if k.ResourceType == ksengine.KafkaResourceType_TOPIC {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_CONSUMER, k.Hostname), nil)
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_PRODUCER, k.Hostname), nil)
			}
			if k.ResourceType == ksengine.KafkaResourceType_GROUP {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_CONSUMER_GROUP, k.Hostname), nil)
			}
			if k.ResourceType == ksengine.KafkaResourceType_TRANSACTIONALID {
				out.Append(c.constructACLDetailsObject(k.ResourceType, k.ResourceName, k.PatternType, k.Principal, ksengine.ShepherdClientType_TRANSACTIONAL_PRODUCER, k.Hostname), nil)
			}
		}
	}
}

func (c ConfluentRBACOperation) String() string {
	return strings.ToUpper(strings.TrimSpace(c.String()))
}

func (c ConfluentRBACOperation) GetValue(in string) (ksengine.ACLOperationsInterface, error) {
	return ConfluentRBACOperation(strings.ToUpper(strings.TrimSpace(in))), nil
}

func (c ConfluentRBACOperation) GenerateACLMappingStructures(clusterName string, in *ksengine.ACLMapping) *ksengine.ACLMapping {
	out, temp, failed := ksengine.ACLMapping{}, ksengine.ACLMapping{}, ksengine.ACLMapping{}
	for k, v := range *in {
		switch k.Operation.(type) {
		case ConfluentRBACOperation:
			out.Append(k, v)
		case ksengine.ShepherdClientType:
			temp.Append(k, v)
		default:
			logger.Warnf("Conversion is only supported Between Shepherd Config Type and %T. The ACL mapping will be added to the Failed list", c)
			failed.Append(k, v)
		}
	}
	if len(temp) > 0 {
		ConfluentRbacACLManager.mapFromShepherdACL(clusterName, &temp, &out, &failed)
	}
	ksmisc.DottedLineOutput("Failed ACLs", "=", 80)
	ConfluentRbacACLManager.ListConfigACL(true, &failed)
	return &out
}

func (c ConfluentRbacACLExecutionManagerImpl) createClustersObject(clusterName string, aName string, aVal string) map[string]interface{} {
	connObj := c.getConnectionObject(clusterName)
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
