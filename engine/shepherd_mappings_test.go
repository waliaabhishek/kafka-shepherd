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
			UserTopicMappingKey{Principal: "p1", ClientType: ShepherdOperationType_PRODUCER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only Producer ID present"},
		{"./testdata/utm_mapping/producers/definitions_2.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "p1", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pgroup"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
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
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Everything Present"},
		{"./testdata/utm_mapping/producers/definitions_10.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi2", ClientType: ShepherdOperationType_PRODUCER, GroupID: "pg2"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdOperationType_PRODUCER_IDEMPOTENCE, GroupID: "pg2"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi2", ClientType: ShepherdOperationType_TRANSACTIONAL_PRODUCER, GroupID: "pg2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
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
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
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
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
			"Only ID & hostnames present"},
		// {"./testdata/utm_mapping/consumers/definitions_6.yaml", UserTopicMapping{
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
		// 	UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
		// 	"Only group name & hostnames present"},
		{"./testdata/utm_mapping/consumers/definitions_7.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
			"All 3 attributes present"},
		{"./testdata/utm_mapping/consumers/definitions_8.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ci2", ClientType: ShepherdOperationType_CONSUMER, GroupID: "cg2"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}},
			UserTopicMappingKey{Principal: "ci2", ClientType: ShepherdOperationType_CONSUMER_GROUP, GroupID: "cg2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}}},
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
			UserTopicMappingKey{Principal: "con1", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "con2", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect2"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}:         UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host", "def.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}},
		},
			"Only two attributes are present"},
		{"./testdata/utm_mapping/connectors/definitions_3.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "coni1", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "coni2", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect2"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "coni3", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}:         UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "coni4", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}},
			UserTopicMappingKey{Principal: "coni5", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: ""}:         UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}},
			UserTopicMappingKey{Principal: "coni6", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: ""}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
		},
			"Three attributes are present"},
		{"./testdata/utm_mapping/connectors/definitions_4.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "coni1", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "coni2", ClientType: ShepherdOperationType_SOURCE_CONNECTOR, GroupID: "connect2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host", "ghi.host"}},
			UserTopicMappingKey{Principal: "coni3", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect3"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"jkl.host"}},
			UserTopicMappingKey{Principal: "coni4", ClientType: ShepherdOperationType_SINK_CONNECTOR, GroupID: "connect4"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
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
			UserTopicMappingKey{Principal: "stri1", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str1"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "stri2", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str3"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str4"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host", "ghi.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str5"}:      UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"jkl.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str6"}:      UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
		},
			"Three attributes are present"},
		{"./testdata/utm_mapping/streams/definitions_3.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "stri1", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str1"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "stri2", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}},
			UserTopicMappingKey{Principal: "stri3", ClientType: ShepherdOperationType_STREAM_READ, GroupID: "str3"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}},
			UserTopicMappingKey{Principal: "stri4", ClientType: ShepherdOperationType_STREAM_WRITE, GroupID: "str4"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
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
			UserTopicMappingKey{Principal: "ksqli1", ClientType: ShepherdOperationType_KSQL_READ, GroupID: "ksql1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_KSQL_WRITE, GroupID: "ksql2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql3"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"stu.host", "vwx.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_KSQL, GroupID: "ksql3"}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"stu.host", "vwx.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql4"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"yza.host", "bcd.host"}},
			// UserTopicMappingKey{Principal: "", ClientType: ShepherdOperationType_KSQL, GroupID: "ksql4"}:           UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"yza.host", "bcd.host"}},
		},
			"Three attributes are present"},
		{"./testdata/utm_mapping/ksql/definitions_4.yaml", UserTopicMapping{
			// UserTopicMappingKey{Principal: "ksqli1", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql1"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ksqli1", ClientType: ShepherdOperationType_KSQL_READ, GroupID: "ksql1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			// UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql2"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}},
			UserTopicMappingKey{Principal: "ksqli2", ClientType: ShepherdOperationType_KSQL_WRITE, GroupID: "ksql2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}},
			// UserTopicMappingKey{Principal: "ksqli3", ClientType: ShepherdOperationType_CONSUMER, GroupID: "ksql3"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}},
			UserTopicMappingKey{Principal: "ksqli3", ClientType: ShepherdOperationType_KSQL_READ, GroupID: "ksql3"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"ghi.host", "jkl.host"}},
			// UserTopicMappingKey{Principal: "ksqli4", ClientType: ShepherdOperationType_PRODUCER, GroupID: "ksql4"}:  UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
			UserTopicMappingKey{Principal: "ksqli4", ClientType: ShepherdOperationType_KSQL_WRITE, GroupID: "ksql4"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"mno.host", "pqr.host"}},
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
