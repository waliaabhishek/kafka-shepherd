package engine

import (
	"fmt"
	"os"
	"reflect"
)

func (s *StackSuite) SetupTest() {
	os.Setenv("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./testdata/blueprints_0.yaml")
	SpdCore.Blueprints.ParseShepherBlueprints(getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", ""))
}

func (s *StackSuite) TestStackSuite_ExternalFunctions_ProducerDefinitionsToUTMMapping() {
	cases := []struct {
		inDefFileName string
		out           UserTopicMapping
		err           string
	}{
		{"./testdata/utm_mapping/producers/definitions_1.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "p1", ClientType: ShepherdOperationType_PRODUCER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)}},
			"Only Producer ID present"},
		{"./testdata/utm_mapping/producers/definitions_2.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "p1", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pgroup"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)}},
			"Producer ID & Group present"},
		// {"./testdata/utm_mapping/producers/definitions_3.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pgroup"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
		// 	"Only Producer Group present"},
		// {"./testdata/utm_mapping/producers/definitions_4.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
		// 	"Only 1 Hostname present"},
		// {"./testdata/utm_mapping/producers/definitions_5.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host", "def.host"}}},
		// 	"Only Multiple Hostnames present"},
		// {"./testdata/utm_mapping/producers/definitions_6.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg1"}:             UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
		// 	"Only group name & Idempotence flag present"},
		// {"./testdata/utm_mapping/producers/definitions_7.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
		// 	"Only group name & Transaction flag is present"},
		// {"./testdata/utm_mapping/producers/definitions_8.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
		// 	"Only group name , Idempotence flag & transation flag present"},
		{"./testdata/utm_mapping/producers/definitions_9.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)}},
			"Everything Present"},
		{"./testdata/utm_mapping/producers/definitions_10.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "pi2", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg2"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "pi2", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg2"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "pi2", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)}},
			"Everything Present"},
	}

	for _, c := range cases {
		s.testUTMMapping(c.inDefFileName, c.out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_ExternalFunctions_ConsumerDefinitionsToUTMMapping() {
	cases := []struct {
		inDefFileName string
		out           UserTopicMapping
		err           string
	}{
		{"./testdata/utm_mapping/consumers/definitions_1.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: make(NVPairs)}},
			"Only ConsumerID present"},
		// {"./testdata/utm_mapping/consumers/definitions_2.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
		// 	"Only Group present"},
		// {"./testdata/utm_mapping/consumers/definitions_3.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
		// 	"Only 1 Hostname present"},
		// {"./testdata/utm_mapping/consumers/definitions_4.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host", "def.host"}}},
		// 	"Only Multiple Hostnames present"},
		{"./testdata/utm_mapping/consumers/definitions_5.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: make(NVPairs)}},
			"Only ID & hostnames present"},
		// {"./testdata/utm_mapping/consumers/definitions_6.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
		// 	"Only group name & hostnames present"},
		{"./testdata/utm_mapping/consumers/definitions_7.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: make(NVPairs)}},
			"All 3 attributes present"},
		{"./testdata/utm_mapping/consumers/definitions_8.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "ci2", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg2"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}, AddlData: make(NVPairs)},
			UserTopicMappingKey{Principal: "ci2", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}, AddlData: make(NVPairs)}},
			"Multiple consumers present"},
	}

	for _, c := range cases {
		s.testUTMMapping(c.inDefFileName, c.out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_ExternalFunctions_ConnectorDefinitionsToUTMMapping() {
	cases := []struct {
		inDefFileName string
		out           UserTopicMapping
		err           string
	}{
		// {"./testdata/utm_mapping/connectors/definitions_1.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// },
		// 	"Only one attribute is present"},
		{"./testdata/utm_mapping/connectors/definitions_2.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "con1", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "con2", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect2"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}:         UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host", "def.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}},
		},
			"Only two attributes are present"},
		{"./testdata/utm_mapping/connectors/definitions_3.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "coni1", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect1", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni2", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect2"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect2", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni3", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}:         UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni4", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni5", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}:         UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni6", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
		},
			"Three attributes are present"},
		{"./testdata/utm_mapping/connectors/definitions_4.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "coni1", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect1", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni2", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host", "ghi.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect2", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni3", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect3"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"jkl.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect3", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
			UserTopicMappingKey{Principal: "coni4", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect4"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}, AddlData: NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect4", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""}},
		},
			"All attributes are present"},
	}

	for _, c := range cases {
		s.testUTMMapping(c.inDefFileName, c.out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_ExternalFunctions_StreamsDefinitionsToUTMMapping() {
	cases := []struct {
		inDefFileName string
		out           UserTopicMapping
		err           string
	}{
		// {"./testdata/utm_mapping/streams/definitions_1.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str1"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// },
		// 	"Only two attributes are present"},
		{"./testdata/utm_mapping/streams/definitions_2.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "stri1", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str1"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "str1"}},
			UserTopicMappingKey{Principal: "stri2", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "str2"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str3"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str4"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host", "ghi.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str5"}:      UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"jkl.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str6"}:      UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
		},
			"Three attributes are present"},
		{"./testdata/utm_mapping/streams/definitions_3.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "stri1", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str1"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "str1"}},
			UserTopicMappingKey{Principal: "stri2", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}, AddlData: NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "str2"}},
			UserTopicMappingKey{Principal: "stri3", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str3"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}, AddlData: NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "str3"}},
			UserTopicMappingKey{Principal: "stri4", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str4"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}, AddlData: NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "str4"}},
		},
			"All attributes are present"},
	}

	for _, c := range cases {
		s.testUTMMapping(c.inDefFileName, c.out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_ExternalFunctions_KSQLDefinitionsToUTMMapping() {
	cases := []struct {
		inDefFileName string
		out           UserTopicMapping
		err           string
	}{
		// {"./testdata/utm_mapping/ksql/definitions_2.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_KSQL, GroupID: "ksql1"}:     UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_KSQL, GroupID: "ksql2"}:     UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
		// },
		// 	"Only two attributes are present"},
		{"./testdata/utm_mapping/ksql/definitions_3.yaml", UserTopicMapping{
			// UserTopicMappingKey{Principal: "ksqli1", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "ksqli1", ClientType: ShepherdOperationType_KSQL_READ, GroupID: "ksql1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "ksql1"}},
			// UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_KSQL_WRITE, GroupID: "ksql2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}, AddlData: NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "ksql2"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql3"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"stu.host", "vwx.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_KSQL, GroupID: "ksql3"}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"stu.host", "vwx.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql4"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"yza.host", "bcd.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_KSQL, GroupID: "ksql4"}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"yza.host", "bcd.host"}},
		},
			"Three attributes are present"},
		{"./testdata/utm_mapping/ksql/definitions_4.yaml", UserTopicMapping{
			// UserTopicMappingKey{Principal: "ksqli1", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql1"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ksqli1", ClientType: ShepherdOperationType_KSQL_READ, GroupID: "ksql1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}, AddlData: NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "ksql1"}},
			// UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql2"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}},
			UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_KSQL_WRITE, GroupID: "ksql2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}, AddlData: NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "ksql2"}},
			// UserTopicMappingKey{Principal: "ksqli3", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql3"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}},
			UserTopicMappingKey{Principal: "ksqli3", ClientType: ShepherdOperationType_KSQL_READ, GroupID: "ksql3"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}, AddlData: NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "ksql3"}},
			// UserTopicMappingKey{Principal: "ksqli4", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql4"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
			UserTopicMappingKey{Principal: "ksqli4", ClientType: ShepherdOperationType_KSQL_WRITE, GroupID: "ksql4"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}, AddlData: NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "ksql4"}},
		},
			"All attributes are present"},
	}

	for _, c := range cases {
		s.testUTMMapping(c.inDefFileName, c.out, c.err)
	}
}

func (s *StackSuite) testUTMMapping(in string, expected UserTopicMapping, err string) {
	os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", in)
	SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", ""), true)
	ConfMaps.utm = UserTopicMapping{}
	GenerateMappings()
	// s.EqualValues(expected, ConfMaps.utm, fmt.Sprintf("Input File Name: %v\n\nError: %v", in, err))
	s.True(reflect.DeepEqual(expected, ConfMaps.utm), fmt.Sprintf("File Name Reference: %v\n\nExpected Value: %v\n\nActual Value:   %v\n\nError: %v", in, expected, ConfMaps.utm, err))
}

func (s *StackSuite) TestStackSuite_Init_CCMMappingTests() {
	cases := []struct {
		inFileName string
		out        ClusterConfigMapping
		err        string
	}{
		{"./testdata/cluster_config/shepherd_0.yaml",
			ClusterConfigMapping{ClusterConfigMappingKey{Name: "dev_plaintext", IsEnabled: true}: ClusterConfigMappingValue{
				IsActive: false, ClientID: "test1", IsACLManagementEnabled: false, TopicManager: "sarama", ACLManager: "kafka_acl",
				BootstrapServers: []string{"localhost:9093"}, ClusterSecurityProtocol: ClusterSecurityProtocol_PLAINTEXT, ClusterDetails: NVPairs{},
				ClusterSASLMechanism: ClusterSASLMechanism_UNKNOWN, Configs: NVPairs{"security.protocol": "PLAINTEXT"},
			}},
			"PLAINTEXT Cluster Connectivity Failed"},
		// {"./testdata/cluster_config/shepherd_1.yaml",
		// 	ClusterConfigMapping{ClusterConfigMappingKey{Name: "test_ssl_1WaySSL", IsEnabled: true}: ClusterConfigMappingValue{
		// 		IsActive: false, ClientID: "test2", IsACLManagementEnabled: true, TopicManager: "sarama", ACLManager: "kafka_acl",
		// 		BootstrapServers: []string{"localhost:9093"}, ClusterSecurityProtocol: ClusterSecurityProtocol_SSL, ClusterDetails: NVPairs{},
		// 		ClusterSASLMechanism: ClusterSASLMechanism_UNKNOWN, Configs: NVPairs{"security.protocol": "SSL"},
		// 	}},
		// "SSL 1 Way Cluster Connectivity Failed"},
		{"./testdata/cluster_config/shepherd_2.yaml",
			ClusterConfigMapping{ClusterConfigMappingKey{Name: "test2_sasl_plaintext", IsEnabled: true}: ClusterConfigMappingValue{
				IsActive: false, ClientID: "test3", IsACLManagementEnabled: true, TopicManager: "sarama", ACLManager: "kafka_acl",
				BootstrapServers: []string{"localhost:9093"}, ClusterSecurityProtocol: ClusterSecurityProtocol_SASL_PLAINTEXT, ClusterDetails: NVPairs{},
				ClusterSASLMechanism: ClusterSASLMechanism_PLAIN, Configs: NVPairs{"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN", "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\""},
			}},
			"SASL_PLAINTEXT Cluster Connectivity Failed"},
		{"./testdata/cluster_config/shepherd_3.yaml",
			ClusterConfigMapping{ClusterConfigMappingKey{Name: "test3_sasl_plaintext_scram", IsEnabled: true}: ClusterConfigMappingValue{
				IsActive: false, ClientID: "test4", IsACLManagementEnabled: true, TopicManager: "sarama", ACLManager: "kafka_acl",
				BootstrapServers: []string{"localhost:9093", "localhost:9094"}, ClusterSecurityProtocol: ClusterSecurityProtocol_SASL_PLAINTEXT, ClusterDetails: NVPairs{},
				ClusterSASLMechanism: ClusterSASLMechanism_SCRAM_SHA_256, Configs: NVPairs{"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-256", "sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka-pass\""},
			}},
			"SASL_SCRAM-256 Cluster Connectivity Failed"},
		{"./testdata/cluster_config/shepherd_4.yaml",
			ClusterConfigMapping{ClusterConfigMappingKey{Name: "test3_sasl_plaintext_scram_512", IsEnabled: true}: ClusterConfigMappingValue{
				IsActive: false, ClientID: "test4", IsACLManagementEnabled: true, TopicManager: "sarama", ACLManager: "kafka_acl",
				BootstrapServers: []string{"localhost:9093", "localhost:9094"}, ClusterSecurityProtocol: ClusterSecurityProtocol_SASL_PLAINTEXT, ClusterDetails: NVPairs{},
				ClusterSASLMechanism: ClusterSASLMechanism_SCRAM_SHA_512, Configs: NVPairs{"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-512", "sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka-pass\""},
			}},
			"SASL_SCRAM-512 Cluster Connectivity Failed"},
		{"./testdata/cluster_config/shepherd_5.yaml",
			ClusterConfigMapping{ClusterConfigMappingKey{Name: "test4_confluent_rbac", IsEnabled: true}: ClusterConfigMappingValue{
				IsActive: false, ClientID: "test5", IsACLManagementEnabled: true, TopicManager: "sarama", ACLManager: "confluent_mds",
				BootstrapServers: []string{"localhost:9093", "localhost:9094"}, ClusterSecurityProtocol: ClusterSecurityProtocol_SASL_PLAINTEXT, ClusterDetails: NVPairs{},
				ClusterSASLMechanism: ClusterSASLMechanism_PLAIN, Configs: NVPairs{"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN", "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"alice-secret\"",
					"erp.url": "http://0.0.0.0:8090", "mds.url": "http://0.0.0.0:8090", "ksql.url": "http://0.0.0.0:8083", "mds.username": "alice", "mds.password": "alice-secret", "connect-cluster": "connect-cluster", "schema-registry-cluster": "schema-registry"},
			}},
			"Confluent RBAC Cluster Connectivity Failed"},
	}

	for _, c := range cases {
		os.Setenv("SHEPHERD_CONFIG_FILE_LOCATION", c.inFileName)
		SpdCore.Configs.ParseShepherdConfig(getEnvVarsWithDefaults("SHEPHERD_CONFIG_FILE_LOCATION", ""), true)
		ConfMaps.CCM = ClusterConfigMapping{}
		GenerateMappings()
		// s.EqualValues(expected, ConfMaps.utm, fmt.Sprintf("Input File Name: %v\n\nError: %v", in, err))
		s.True(reflect.DeepEqual(c.out, ConfMaps.CCM), fmt.Sprintf("File Name Reference: %v\n\nExpected Value: %v\n\nActual Value:   %v\n\nError: %v", c.inFileName, c.out, ConfMaps.CCM, c.err))
	}
}
