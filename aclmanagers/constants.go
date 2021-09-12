package aclmanagers

type (
	ACLManagementType int
)

const (
	ACLManagementType_UNKNOWN ACLManagementType = iota
	ACLManagementType_CREATE_ACL
	ACLManagementType_DELETE_CONFIG_ACL
	ACLManagementType_DELETE_UNKNOWN_ACL
	ACLManagementType_LIST_CLUSTER_ACL
	ACLManagementType_LIST_CONFIG_ACL
)

func (a *ACLManagementType) String() string {
	mapping := map[ACLManagementType]string{
		ACLManagementType_UNKNOWN:            "UNKNOWN",
		ACLManagementType_CREATE_ACL:         "CREATE_ACL",
		ACLManagementType_DELETE_CONFIG_ACL:  "DELETE_CONFIG_ACL",
		ACLManagementType_DELETE_UNKNOWN_ACL: "DELETE_UNKNOWN_ACL",
		ACLManagementType_LIST_CLUSTER_ACL:   "LIST_CLUSTER_ACL",
		ACLManagementType_LIST_CONFIG_ACL:    "LIST_CONFIG_ACL",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[ACLManagementType_UNKNOWN]
	}
	return s
}
