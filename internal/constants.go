package core

type (
	RunMode                 int
	ClusterSecurityProtocol int
	ClusterSASLMechanism    int
	KafkaACLOperation       int
	KafkaACLPermissionType  int
	KafkaACLPatternType     int
)

const (
	RunMode_UNKNOWN RunMode = iota
	RunMode_SINGLE_CLUSTER
	RunMode_MULTI_CLUSTER
	RunMode_MIGRATION
	RunMode_CREATE_CONFIGS
)

func (in RunMode) String() string {
	m := map[RunMode]string{
		RunMode_UNKNOWN:        "RunMode_UNKNOWN",
		RunMode_SINGLE_CLUSTER: "SINGLE_CLUSTER",
		RunMode_MULTI_CLUSTER:  "MULTI_CLUSTER",
		RunMode_MIGRATION:      "MIGRATION",
		RunMode_CREATE_CONFIGS: "CREATE_CONFIGS",
	}
	ret, present := m[in]
	if !present {
		ret = m[RunMode_UNKNOWN]
	}
	return ret
}

const (
	ClusterSecurityProtocol_UNKNOWN ClusterSecurityProtocol = iota
	ClusterSecurityProtocol_PLAINTEXT
	ClusterSecurityProtocol_SSL
	ClusterSecurityProtocol_SASL_SSL
	ClusterSecurityProtocol_SASL_PLAINTEXT
)

func (in ClusterSecurityProtocol) String() string {
	m := map[ClusterSecurityProtocol]string{
		ClusterSecurityProtocol_UNKNOWN:        "ClusterSecurityProtocol_Unknown",
		ClusterSecurityProtocol_PLAINTEXT:      "PLAINTEXT",
		ClusterSecurityProtocol_SSL:            "SSL",
		ClusterSecurityProtocol_SASL_SSL:       "SASL_SSL",
		ClusterSecurityProtocol_SASL_PLAINTEXT: "SASL_PLAINTEXT",
	}
	ret, present := m[in]
	if !present {
		ret = m[ClusterSecurityProtocol_UNKNOWN]
	}
	return ret
}

const (
	ClusterSASLMechanism_UNKNOWN ClusterSASLMechanism = iota
	ClusterSASLMechanism_PLAIN
	ClusterSASLMechanism_SCRAM_SHA_256
	ClusterSASLMechanism_SCRAM_SHA_512
	ClusterSASLMechanism_OAUTHBEARER
	ClusterSASLMechanism_SASL_MECH_NULL
)

func (in ClusterSASLMechanism) String() string {
	m := map[ClusterSASLMechanism]string{
		ClusterSASLMechanism_UNKNOWN:       "ClusterSASLMechanism_UNKNOWN",
		ClusterSASLMechanism_PLAIN:         "PLAIN",
		ClusterSASLMechanism_SCRAM_SHA_256: "SCRAM-SHA-256",
		ClusterSASLMechanism_SCRAM_SHA_512: "SCRAM-SHA-512",
		ClusterSASLMechanism_OAUTHBEARER:   "OAUTHBEARER",
	}
	ret, present := m[in]
	if !present {
		ret = m[ClusterSASLMechanism_UNKNOWN]
	}
	return ret
}

const (
	KafkaACLPermissionType_UNKNOWN KafkaACLPermissionType = iota
	KafkaACLPermissionType_ANY
	KafkaACLPermissionType_DENY
	KafkaACLPermissionType_ALLOW
)

func (in *KafkaACLPermissionType) String() string {
	m := map[KafkaACLPermissionType]string{
		KafkaACLPermissionType_UNKNOWN: "KafkaACLPermissionType_UNKNOWN",
		KafkaACLPermissionType_ANY:     "Any",
		KafkaACLPermissionType_DENY:    "Deny",
		KafkaACLPermissionType_ALLOW:   "Allow",
	}
	s, ok := m[*in]
	if !ok {
		s = m[KafkaACLPermissionType_UNKNOWN]
	}
	return s
}

const (
	KafkaACLPatternType_UNKNOWN KafkaACLPatternType = iota
	KafkaACLPatternType_ANY
	KafkaACLPatternType_MATCH
	KafkaACLPatternType_LITERAL
	KafkaACLPatternType_PREFIXED
)

func (a *KafkaACLPatternType) String() string {
	mapping := map[KafkaACLPatternType]string{
		KafkaACLPatternType_UNKNOWN:  "KafkaACLPatternType_UNKNOWN",
		KafkaACLPatternType_ANY:      "Any",
		KafkaACLPatternType_MATCH:    "Match",
		KafkaACLPatternType_LITERAL:  "Literal",
		KafkaACLPatternType_PREFIXED: "Prefixed",
	}
	s, ok := mapping[*a]
	if !ok {
		s = mapping[KafkaACLPatternType_UNKNOWN]
	}
	return s
}
