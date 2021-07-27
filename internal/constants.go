package core

import (
	"fmt"
	"strings"
)

type (
	ClientType              int
	RunMode                 int
	ClusterSecurityProtocol int
	ClusterSASLMechanism    int
	ClusterAclOperation     int
)

const (
	CT_UNKNOWN ClientType = iota
	CT_PRODUCER
	CT_CONSUMER
	CT_SOURCE_CONNECTOR
	CT_SINK_CONNECTOR
	CT_STREAM_READ
	CT_STREAM_WRITE
	CT_KSQL
)

func (in ClientType) String() string {
	m := map[ClientType]string{
		CT_UNKNOWN:          "ClientType_UNKNOWN",
		CT_PRODUCER:         "PRODUCER",
		CT_CONSUMER:         "CONSUMER",
		CT_SOURCE_CONNECTOR: "SOURCE_CONNECTOR",
		CT_SINK_CONNECTOR:   "SINK_CONNECTOR",
		CT_STREAM_READ:      "STREAM_READ",
		CT_STREAM_WRITE:     "STREAM_WRITE",
		CT_KSQL:             "KSQL",
	}
	ret, present := m[in]
	if !present {
		ret = m[CT_UNKNOWN]
	}
	return ret
}

const (
	RM_UNKNOWN RunMode = iota
	RM_SINGLE_CLUSTER
	RM_MULTI_CLUSTER
	RM_MIGRATION
	RM_CREATE_CONFIGS
)

func (in RunMode) String() string {
	m := map[RunMode]string{
		RM_UNKNOWN:        "RunMode_UNKNOWN",
		RM_SINGLE_CLUSTER: "SINGLE_CLUSTER",
		RM_MULTI_CLUSTER:  "MULTI_CLUSTER",
		RM_MIGRATION:      "MIGRATION",
		RM_CREATE_CONFIGS: "CREATE_CONFIGS",
	}
	ret, present := m[in]
	if !present {
		ret = m[RM_UNKNOWN]
	}
	return ret
}

const (
	SP_UNKNOWN ClusterSecurityProtocol = iota
	SP_PLAINTEXT
	SP_SSL
	SP_SASL_SSL
	SP_SASL_PLAINTEXT
)

func (in ClusterSecurityProtocol) String() string {
	m := map[ClusterSecurityProtocol]string{
		SP_UNKNOWN:        "ClusterSecurityProtocol_Unknown",
		SP_PLAINTEXT:      "PLAINTEXT",
		SP_SSL:            "SSL",
		SP_SASL_SSL:       "SASL_SSL",
		SP_SASL_PLAINTEXT: "SASL_PLAINTEXT",
	}
	ret, present := m[in]
	if !present {
		ret = m[SP_UNKNOWN]
	}
	return ret
}

const (
	SM_UNKNOWN ClusterSASLMechanism = iota
	SM_PLAIN
	SM_SCRAM_SHA_256
	SM_SCRAM_SHA_512
	SM_OAUTHBEARER
	SM_SASL_MECH_NULL
)

func (in ClusterSASLMechanism) String() string {
	m := map[ClusterSASLMechanism]string{
		SM_UNKNOWN:       "ClusterSASLMechanism_Unknown",
		SM_PLAIN:         "PLAIN",
		SM_SCRAM_SHA_256: "SCRAM-SHA-256",
		SM_SCRAM_SHA_512: "SCRAM-SHA-512",
		SM_OAUTHBEARER:   "OAUTHBEARER",
	}
	ret, present := m[in]
	if !present {
		ret = m[SM_UNKNOWN]
	}
	return ret
}

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java
// This Constant list is a set of all the values listed in Apache Kafka. This helps to ensure that the complete and
// only fail in case of an outlier.
const (
	CAO_UNKNOWN ClusterAclOperation = iota
	CAO_ANY
	CAO_ALL
	CAO_READ
	CAO_WRITE
	CAO_CREATE
	CAO_DELETE
	CAO_ALTER
	CAO_DESCRIBE
	CAO_CLUSTERACTION
	CAO_DESCRIBECONFIGS
	CAO_ALTERCONFIGS
	CAO_IDEMPOTENTWRITE
)

func (in ClusterAclOperation) String() string {
	m := map[ClusterAclOperation]string{
		CAO_UNKNOWN:         "CLUSTERACLOPERATION_UNKNOWN",
		CAO_ANY:             "ANY",
		CAO_ALL:             "ALL",
		CAO_READ:            "READ",
		CAO_WRITE:           "WRITE",
		CAO_CREATE:          "CREATE",
		CAO_DELETE:          "DELETE",
		CAO_ALTER:           "ALTER",
		CAO_DESCRIBE:        "DESCRIBE",
		CAO_CLUSTERACTION:   "CLUSTER_ACTION",
		CAO_DESCRIBECONFIGS: "DESCRIBE_CONFIGS",
		CAO_ALTERCONFIGS:    "ALTER_CONFIGS",
		CAO_IDEMPOTENTWRITE: "IDEMPOTENT_WRITE",
	}
	s, ok := m[in]
	if !ok {
		s = m[CAO_UNKNOWN]
	}
	return s
}

func (c *ClusterAclOperation) GetValue(in string) error {
	m := map[string]ClusterAclOperation{
		"CLUSTERACLOPERATION_UNKNOWN": CAO_UNKNOWN,
		"ANY":                         CAO_ANY,
		"ALL":                         CAO_ALL,
		"READ":                        CAO_READ,
		"WRITE":                       CAO_WRITE,
		"CREATE":                      CAO_CREATE,
		"DELETE":                      CAO_DELETE,
		"ALTER":                       CAO_ALTER,
		"DESCRIBE":                    CAO_DESCRIBE,
		"CLUSTER_ACTION":              CAO_CLUSTERACTION,
		"DESCRIBE_CONFIGS":            CAO_DESCRIBECONFIGS,
		"ALTER_CONFIGS":               CAO_ALTERCONFIGS,
		"IDEMPOTENT_WRITE":            CAO_IDEMPOTENTWRITE,
	}
	s, ok := m[strings.ToUpper(strings.TrimSpace(in))]
	if !ok {
		*c = m["ClusterAclOperation_Unknown"]
		logger.Errorw("Illegal Cluster ACL Operation String provided.",
			"Input String", in)
		return fmt.Errorf("illegal cluster acl pperation string provided. input string: %s", in)
	}
	*c = s
	return nil
}
