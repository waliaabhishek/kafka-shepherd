package core

type ClientType int

const (
	PRODUCER ClientType = iota
	CONSUMER
	SOURCE_CONNECTOR
	SINK_CONNECTOR
	STREAM_READ
	STREAM_WRITE
	KSQL
)

func (r ClientType) String() string {
	return [...]string{"PRODUCER", "CONSUMER", "SOURCE_CONNECTOR", "SINK_CONNECTOR", "STREAM_READ", "STREAM_WRITE", "KSQL"}[r]
}

type RunMode int

const (
	SINGLE_CLUSTER RunMode = iota
	MULTI_CLUSTER
	MIGRATION
	CREATE_CONFIGS_FROM_EXISTING_CLUSTER
)

func (r RunMode) String() string {
	return [...]string{"SINGLE_CLUSTER", "MULTI_CLUSTER", "MIGRATION", "CREATE_CONFIGS_FROM_EXISTING_CLUSTER"}[r]
}

type ClusterSecurityProtocol int

const (
	PLAINTEXT ClusterSecurityProtocol = iota
	SSL
	SASL_SSL
	SASL_PLAINTEXT
	UNIDENTIFIED_SECURITY_PROTOCOL
)

func (r ClusterSecurityProtocol) String() string {
	return [...]string{"PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT", "UNIDENTIFIED_SECURITY_PROTOCOL"}[r]
}

type ClusterSASLMechanism int

const (
	PLAIN ClusterSASLMechanism = iota
	SCRAM_SHA_256
	SCRAM_SHA_512
	OAUTHBEARER
	SASL_MECH_NULL
	UNIDENTIFIED_SASL_MECHANISM
)

func (r ClusterSASLMechanism) String() string {
	return [...]string{"PLAIN", "SCRAM_SHA_256", "SCRAM_SHA_512", "OAUTHBEARER", "SASL_MECH_NULL", "UNIDENTIFIED_SASL_MECHANISM"}[r]
}
