package engine

import (
	"fmt"
	"os"
	"reflect"

	mapset "github.com/deckarep/golang-set"
)

func (s *StackSuite) TestStackSuite_ExternalFunctions_ListTopicsInConfig() {
	cases := []struct {
		inDefFileName string
		out           []string
		err           string
	}{
		{"./testdata/topics/definitions_1.yaml", []string{"test.1", "test.2"}, "Multiple adhoc topics present"},
		{"./testdata/topics/definitions_2.yaml", []string{"test.1"}, "only single adhoc topic present"},
		{"./testdata/topics/definitions_3.yaml", []string{}, "adhoc defined but no topics present"},
		{"./testdata/topics/definitions_4.yaml", []string{"test.1", "test.2"}, "Multiple duplicate adhoc topics with config overrides present"},
		{"./testdata/topics/definitions_5.yaml", []string{}, "Only top level scope added to the topic name"},
		{"./testdata/topics/definitions_6.yaml", []string{"int.test", "bss.test", "oss.test"}, "Top level scope and topic Name present"},
		{"./testdata/topics/definitions_7.yaml", []string{"int.test", "bss.test", "oss.test"}, "Top level scope, second level scope and topic Name present"},
		{"./testdata/topics/definitions_8.yaml", []string{"int.test", "bss.test", "oss.test", "int.landing.test2", "int.staging.test2", "int.ready.test2", "bss.landing.test2", "bss.staging.test2", "bss.ready.test2", "oss.landing.test2", "oss.staging.test2", "oss.ready.test2"}, "Top level scope and topic Name at both nodes present"},
		{"./testdata/topics/definitions_9.yaml", []string{"int.test", "oss.test"}, "Top level scope, ignore scope and topic Name at first level present"},
		{"./testdata/topics/definitions_10.yaml", []string{"int.test", "oss.test", "int.landing.test2", "int.staging.test2", "int.ready.test2", "bss.landing.test2", "bss.staging.test2", "bss.ready.test2", "oss.landing.test2", "oss.staging.test2", "oss.ready.test2"}, "Top level scope, ignore scope (top level) and topic Name at both levels present"},
		{"./testdata/topics/definitions_10.yaml", []string{"int.test", "oss.test", "int.landing.test2", "int.staging.test2", "int.ready.test2", "bss.landing.test2", "bss.staging.test2", "bss.ready.test2", "oss.landing.test2", "oss.staging.test2", "oss.ready.test2"}, "Top level scope, ignore scope (top level) and topic Name at both levels present"},
		{"./testdata/topics/definitions_11.yaml", []string{"int.landing.test2", "int.staging.test2", "int.ready.test2", "bss.landing.test2", "bss.staging.test2", "bss.ready.test2", "oss.landing.test2", "oss.staging.test2", "oss.ready.test2"}, "Top level scope, ignore scope (top level) and topic Name at both levels present"},
		{"./testdata/topics/definitions_12.yaml", []string{"int.staging.test2", "int.ready.test2", "bss.staging.test2", "bss.ready.test2", "oss.staging.test2", "oss.ready.test2"}, "Top level scope, ignore scope (second level) and topic Name at second level present"},
		{"./testdata/topics/definitions_13.yaml", []string{"int.landing.test3", "int.staging.test3", "int.ready.test3", "bss.landing.test3", "bss.staging.test3", "bss.ready.test3", "oss.landing.test3", "oss.staging.test3", "oss.ready.test3"}, "Top level, second level, third level scope & topic name at the third level present."},
		{"./testdata/topics/definitions_14.yaml", []string{"landing.test3", "staging.test3", "ready.test3"}, "Top level (skipped), second level, third level scope & topic name at the third level present."},
	}

	for _, c := range cases {
		os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", c.inDefFileName)
		SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", ""), true)
		ConfMaps.TCM = TopicConfigMapping{}
		topicsInConfig = mapset.NewSet()
		GenerateMappings()
		out := ListTopicsInConfig(true)
		s.ElementsMatch(c.out, out, c.err)
	}
}

func (s *StackSuite) TestStackSuite_ExternalFunctions_ListACLsInConfig() {
	cases := []struct {
		inDefFileName string
		out           *ACLMapping
		err           string
	}{
		{"./testdata/utm_mapping/acl/shepherd/definitions_1.yaml",
			&ACLMapping{
				// Producers
				// User:1101
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1101", ShepherdOperationType_PRODUCER, "*"): nil,
				// User:1102
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1102", ShepherdOperationType_PRODUCER, "*"): nil,
				// User:1103
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1103", ShepherdOperationType_PRODUCER, "*"):                      nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1103", ShepherdOperationType_PRODUCER_IDEMPOTENCE, "*"): nil,
				// User:1104
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1104", ShepherdOperationType_PRODUCER, "*"):                       nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1104", ShepherdOperationType_PRODUCER_IDEMPOTENCE, "*"):  nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1104", KafkaACLPatternType_LITERAL, "User:1104", ShepherdOperationType_TRANSACTIONAL_PRODUCER, "*"): nil,
				// User:1105
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1105", ShepherdOperationType_PRODUCER, "*"):                       nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1105", ShepherdOperationType_PRODUCER_IDEMPOTENCE, "*"):  nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1105", KafkaACLPatternType_LITERAL, "User:1105", ShepherdOperationType_TRANSACTIONAL_PRODUCER, "*"): nil,
				//User:1106
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1106", ShepherdOperationType_PRODUCER, "abc.host"):                       nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1106", ShepherdOperationType_PRODUCER, "def.host"):                       nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1106", ShepherdOperationType_PRODUCER_IDEMPOTENCE, "abc.host"):  nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1106", ShepherdOperationType_PRODUCER_IDEMPOTENCE, "def.host"):  nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1106", KafkaACLPatternType_LITERAL, "User:1106", ShepherdOperationType_TRANSACTIONAL_PRODUCER, "abc.host"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1106", KafkaACLPatternType_LITERAL, "User:1106", ShepherdOperationType_TRANSACTIONAL_PRODUCER, "def.host"): nil,
				// // Consumers
				// // User:1111
				// constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1111", ShepherdOperationType_CONSUMER, "*"): nil,
				// // User:1112
				// constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1112", ShepherdOperationType_CONSUMER, "*"): nil,
				// constructACLDetailsObject(KafkaResourceType_GROUP, "1112", KafkaACLPatternType_LITERAL, "User:1112", ShepherdOperationType_CONSUMER, "*"):   nil,
			},
			"Producer ACL Mismatch"},
		{"./testdata/utm_mapping/acl/shepherd/definitions_2.yaml",
			&ACLMapping{
				// Consumers
				// User:1111
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1111", ShepherdOperationType_CONSUMER, "*"): nil,
				// User:1112
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1112", ShepherdOperationType_CONSUMER, "*"):     nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1112", KafkaACLPatternType_LITERAL, "User:1112", ShepherdOperationType_CONSUMER_GROUP, "*"): nil,
				// User:1113
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1113", ShepherdOperationType_CONSUMER, "ghi.host"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1113", ShepherdOperationType_CONSUMER, "jkl.host"):     nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1113", KafkaACLPatternType_LITERAL, "User:1113", ShepherdOperationType_CONSUMER_GROUP, "ghi.host"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1113", KafkaACLPatternType_LITERAL, "User:1113", ShepherdOperationType_CONSUMER_GROUP, "jkl.host"): nil,
			},
			"Consumer ACL Mismatch"},
		{"./testdata/utm_mapping/acl/shepherd/definitions_3.yaml",
			&ACLMapping{
				// Connectors
				// User:1121
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1121", ShepherdOperationType_SOURCE_CONNECTOR, "*"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""},
				// User:1122
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1122", ShepherdOperationType_SINK_CONNECTOR, "*"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""},
				// User:1123
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1123", ShepherdOperationType_SOURCE_CONNECTOR, "*"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect-cluster", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""},
				// User:1124
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1124", ShepherdOperationType_SINK_CONNECTOR, "*"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect-cluster", KafkaResourceType_CONNECTOR.GetACLResourceString(): ""},
				// User:1125
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1125", ShepherdOperationType_SOURCE_CONNECTOR, "*"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect-cluster", KafkaResourceType_CONNECTOR.GetACLResourceString(): "1125"},
				// User:1126
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1126", ShepherdOperationType_SINK_CONNECTOR, "*"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect-cluster", KafkaResourceType_CONNECTOR.GetACLResourceString(): "1126"},
				// User:1127
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1127", ShepherdOperationType_SOURCE_CONNECTOR, "mno.host"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect-cluster", KafkaResourceType_CONNECTOR.GetACLResourceString(): "1127"},
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1127", ShepherdOperationType_SOURCE_CONNECTOR, "pqr.host"): NVPairs{KafkaResourceType_CLUSTER.GetACLResourceString(): "kafka-cluster", KafkaResourceType_CONNECT_CLUSTER.GetACLResourceString(): "connect-cluster", KafkaResourceType_CONNECTOR.GetACLResourceString(): "1127"},
			},
			"Connector ACL Mismatch"},
		{"./testdata/utm_mapping/acl/shepherd/definitions_4.yaml",
			&ACLMapping{
				// Streams
				// User:1131
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1131", ShepherdOperationType_STREAM_READ, "*"): NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "1131"},
				// User:1132
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1132", ShepherdOperationType_STREAM_WRITE, "*"): NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "1132"},
				// User:1133
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1133", ShepherdOperationType_STREAM_READ, "stu.host"): NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "1133"},
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1133", ShepherdOperationType_STREAM_READ, "vwx.host"): NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "1133"},
				// User:1134
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1134", ShepherdOperationType_STREAM_WRITE, "yza.host"): NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "1134"},
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1134", ShepherdOperationType_STREAM_WRITE, "bcd.host"): NVPairs{KafkaResourceType_GROUP.GetACLResourceString(): "1134"},
			},
			"Streams ACL Mismatch"},
		{"./testdata/utm_mapping/acl/shepherd/definitions_5.yaml",
			&ACLMapping{
				// ksql
				// User:1141
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1141", ShepherdOperationType_KSQL_READ, "*"): NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "1141"},
				// User:1142
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1142", ShepherdOperationType_KSQL_WRITE, "*"): NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "1142"},
				// User:1143
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1143", ShepherdOperationType_KSQL_READ, "efg.host"): NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "1143"},
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1143", ShepherdOperationType_KSQL_READ, "hij.host"): NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "1143"},
				// User:1144
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1144", ShepherdOperationType_KSQL_WRITE, "klm.host"): NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "1144"},
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1144", ShepherdOperationType_KSQL_WRITE, "nop.host"): NVPairs{KafkaResourceType_KSQL_CLUSTER.GetACLResourceString(): "1144"},
			},
			"Streams ACL Mismatch"},
	}

	for _, c := range cases {
		os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", c.inDefFileName)
		SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", ""), true)
		ConfMaps.utm = UserTopicMapping{}
		GenerateMappings()
		result := ConfMaps.utm.getShepherdACLList()
		// s.EqualValues(c.out, out, c.err)
		s.True(reflect.DeepEqual(c.out, result), fmt.Sprintf("Expected Value: %v \n\n  Actual Value: %v \n\nFilename: %v\n\nError: %v", c.out, result, c.inDefFileName, c.err))
	}
}
