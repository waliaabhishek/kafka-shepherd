package core

import (
	"fmt"
	"strings"
)

type (
	ConfRBACType        int
	ConfRBACClusterType int
)

const (
	ConfRBACType_UNKNOWN ConfRBACType = iota
	ConfRBACType_DEV_READ
	ConfRBACType_DEV_WRITE
	ConfRBACType_DEV_MANAGE
	ConfRBACType_RES_OWNER
	ConfRBACType_OPERATOR
	ConfRBACType_AUDIT_ADMIN
	ConfRBACType_SECURITY_ADMIN
	ConfRBACType_USER_ADMIN
	ConfRBACType_CLUSTER_ADMIN
	ConfRBACType_SYSTEM_ADMIN
	ConfRBACType_SUPER_USER
)

func (in ConfRBACType) String() string {
	m := map[ConfRBACType]string{
		ConfRBACType_UNKNOWN:        "ConfRBACType_UNKNOWN",
		ConfRBACType_DEV_READ:       "DEVELOPER_READ",
		ConfRBACType_DEV_WRITE:      "DEVELOPER_WRITE",
		ConfRBACType_DEV_MANAGE:     "DEVELOPER_MANAGE",
		ConfRBACType_RES_OWNER:      "RESOURCE_OWNER",
		ConfRBACType_OPERATOR:       "OPERATOR",
		ConfRBACType_AUDIT_ADMIN:    "AUDIT_ADMIN",
		ConfRBACType_SECURITY_ADMIN: "SECURITY_ADMIN",
		ConfRBACType_USER_ADMIN:     "USER_ADMIN",
		ConfRBACType_CLUSTER_ADMIN:  "CLUSTER_ADMIN",
		ConfRBACType_SYSTEM_ADMIN:   "SYSTEM_ADMIN",
		ConfRBACType_SUPER_USER:     "SUPER_USER",
	}
	ret, present := m[in]
	if !present {
		ret = m[ConfRBACType_UNKNOWN]
	}
	return ret
}

func (c ConfRBACType) GetValue(in string) (ACLOperationsInterface, error) {
	m := map[string]ConfRBACType{
		"ConfRBACType_UNKNOWN": ConfRBACType_UNKNOWN,
		"DEVELOPER_READ":       ConfRBACType_DEV_READ,
		"DEVELOPER_WRITE":      ConfRBACType_DEV_WRITE,
		"DEVELOPER_MANAGE":     ConfRBACType_DEV_MANAGE,
		"RESOURCE_OWNER":       ConfRBACType_RES_OWNER,
		"OPERATOR":             ConfRBACType_OPERATOR,
		"AUDIT_ADMIN":          ConfRBACType_AUDIT_ADMIN,
		"SECURITY_ADMIN":       ConfRBACType_SECURITY_ADMIN,
		"USER_ADMIN":           ConfRBACType_USER_ADMIN,
		"CLUSTER_ADMIN":        ConfRBACType_CLUSTER_ADMIN,
		"SYSTEM_ADMIN":         ConfRBACType_SYSTEM_ADMIN,
		"SUPER_USER":           ConfRBACType_SUPER_USER,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		s = m["ConfRBACType_UNKNOWN"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return s, fmt.Errorf("illegal cluster acl string provided. input string: %s", in)
	}
	return s, nil
}

// This function will generate the mappings in the ConfRBACType internal structure type for all the mappings provided
// as the input `in` value. Whatever it is able to properly convert, those mappings will be added to the success
// map and the rest will be added to the failed map.
func (c ConfRBACType) generateACLMappingStructures(in ACLMapping, sChannel chan<- ACLMapping, fChannel chan<- ACLMapping) {
	for k, v := range in {
		temp := ACLMapping{}
		value := make(NVPairs)
		switch k.Operation.(type) {
		case ConfRBACType:
			temp[k] = v
			sChannel <- temp
		case ShepherdClientType:
			switch k.Operation {
			case ShepherdClientType_PRODUCER:
				temp[constructACLDetailsObject(KafkaResourceType_TOPIC, k.ResourceName, KafkaACLPatternType_LITERAL,
					k.Principal, ConfRBACType_DEV_WRITE, k.Hostname)] = value
				value["KAFKA_CLUSTER_NAME"] = "repl::KAFKA-CLUSTER-NAME"
				sChannel <- temp
			case ShepherdClientType_PRODUCER_IDEMPOTENCE:

			}
		default:
			temp[k] = v
			fChannel <- temp
			logger.Warnf("Conversion from %T type to %T type is not supported yet. The ACL mapping will be added to the Failed list.", k.Operation, c)
		}
	}
	close(sChannel)
	close(fChannel)
}
