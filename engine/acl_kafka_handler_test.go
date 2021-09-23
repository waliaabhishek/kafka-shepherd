package engine

import (
	"fmt"
	"os"
	"reflect"
)

var (
	kafkaAclMappingCases = []struct {
		inDefFileName string
		out           *ACLMapping
		err           string
	}{
		{"./testdata/utm_mapping/acl/shepherd/definitions_1.yaml",
			&ACLMapping{
				// Producers
				// User:1101
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1101", KafkaACLOperation_WRITE, "*"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1101", KafkaACLOperation_DESCRIBE, "*"): nil,
				// User:1102
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1102", KafkaACLOperation_WRITE, "*"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1102", KafkaACLOperation_DESCRIBE, "*"): nil,
				// User:1103
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1103", KafkaACLOperation_WRITE, "*"):                    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1103", KafkaACLOperation_DESCRIBE, "*"):                 nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1103", KafkaACLOperation_IDEMPOTENTWRITE, "*"): nil,
				// User:1104
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1104", KafkaACLOperation_WRITE, "*"):                    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1104", KafkaACLOperation_DESCRIBE, "*"):                 nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1104", KafkaACLOperation_IDEMPOTENTWRITE, "*"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1104", KafkaACLPatternType_LITERAL, "User:1104", KafkaACLOperation_DESCRIBE, "*"):         nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1104", KafkaACLPatternType_LITERAL, "User:1104", KafkaACLOperation_WRITE, "*"):            nil,
				// User:1105
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1105", KafkaACLOperation_WRITE, "*"):                    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1105", KafkaACLOperation_DESCRIBE, "*"):                 nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1105", KafkaACLOperation_IDEMPOTENTWRITE, "*"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1105", KafkaACLPatternType_LITERAL, "User:1105", KafkaACLOperation_DESCRIBE, "*"):         nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1105", KafkaACLPatternType_LITERAL, "User:1105", KafkaACLOperation_WRITE, "*"):            nil,
				//User:1106
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_WRITE, "abc.host"):                    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_DESCRIBE, "abc.host"):                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_WRITE, "def.host"):                    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_DESCRIBE, "def.host"):                 nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_IDEMPOTENTWRITE, "abc.host"): nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_IDEMPOTENTWRITE, "def.host"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1106", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_DESCRIBE, "abc.host"):         nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1106", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_WRITE, "abc.host"):            nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1106", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_DESCRIBE, "def.host"):         nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "1106", KafkaACLPatternType_LITERAL, "User:1106", KafkaACLOperation_WRITE, "def.host"):            nil,
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
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1111", KafkaACLOperation_READ, "*"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1111", KafkaACLOperation_DESCRIBE, "*"): nil,
				// User:1112
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1112", KafkaACLOperation_READ, "*"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1112", KafkaACLOperation_DESCRIBE, "*"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1112", KafkaACLPatternType_LITERAL, "User:1112", KafkaACLOperation_READ, "*"):       nil,
				// User:1113
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1113", KafkaACLOperation_READ, "ghi.host"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1113", KafkaACLOperation_DESCRIBE, "ghi.host"): nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1113", KafkaACLOperation_READ, "jkl.host"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1113", KafkaACLOperation_DESCRIBE, "jkl.host"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1113", KafkaACLPatternType_LITERAL, "User:1113", KafkaACLOperation_READ, "ghi.host"):       nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1113", KafkaACLPatternType_LITERAL, "User:1113", KafkaACLOperation_READ, "jkl.host"):       nil,
			},
			"Consumer ACL Mismatch"},
		{"./testdata/utm_mapping/acl/shepherd/definitions_3.yaml",
			&ACLMapping{
				// Connectors
				// User:1121
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1121", KafkaACLOperation_WRITE, "*"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1121", KafkaACLOperation_DESCRIBE, "*"): nil,
				// User:1122
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1122", KafkaACLOperation_READ, "*"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1122", KafkaACLOperation_DESCRIBE, "*"): nil,
				// User:1123
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1123", KafkaACLOperation_WRITE, "*"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1123", KafkaACLOperation_DESCRIBE, "*"): nil,
				// User:1124
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1124", KafkaACLOperation_READ, "*"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1124", KafkaACLOperation_DESCRIBE, "*"): nil,
				// User:1125
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1125", KafkaACLOperation_WRITE, "*"):      nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1125", KafkaACLOperation_DESCRIBE, "*"):   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "connect-1125", KafkaACLPatternType_LITERAL, "User:1125", KafkaACLOperation_READ, "*"): nil,
				// User:1126
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1126", KafkaACLOperation_READ, "*"):       nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1126", KafkaACLOperation_DESCRIBE, "*"):   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "connect-1126", KafkaACLPatternType_LITERAL, "User:1126", KafkaACLOperation_READ, "*"): nil,
				// User:1127
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1127", KafkaACLOperation_WRITE, "mno.host"):      nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1127", KafkaACLOperation_WRITE, "pqr.host"):      nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1127", KafkaACLOperation_DESCRIBE, "mno.host"):   nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1127", KafkaACLOperation_DESCRIBE, "pqr.host"):   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "connect-1127", KafkaACLPatternType_LITERAL, "User:1127", KafkaACLOperation_READ, "mno.host"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "connect-1127", KafkaACLPatternType_LITERAL, "User:1127", KafkaACLOperation_READ, "pqr.host"): nil,
			},
			"Connector ACL Mismatch"},
		{"./testdata/utm_mapping/acl/shepherd/definitions_4.yaml",
			&ACLMapping{
				// Streams
				// User:1131
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1131", KafkaACLOperation_READ, "*"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1131", KafkaACLOperation_DESCRIBE, "*"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1131", KafkaACLPatternType_PREFIXED, "User:1131", KafkaACLOperation_READ, "*"):      nil,
				// User:1132
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1132", KafkaACLOperation_WRITE, "*"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1132", KafkaACLOperation_DESCRIBE, "*"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1132", KafkaACLPatternType_PREFIXED, "User:1132", KafkaACLOperation_READ, "*"):      nil,
				// User:1133
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1133", KafkaACLOperation_READ, "stu.host"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1133", KafkaACLOperation_DESCRIBE, "stu.host"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1133", KafkaACLPatternType_PREFIXED, "User:1133", KafkaACLOperation_READ, "stu.host"):      nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1133", KafkaACLOperation_READ, "vwx.host"):     nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1133", KafkaACLOperation_DESCRIBE, "vwx.host"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1133", KafkaACLPatternType_PREFIXED, "User:1133", KafkaACLOperation_READ, "vwx.host"):      nil,
				// User:1134
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1134", KafkaACLOperation_WRITE, "yza.host"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1134", KafkaACLOperation_DESCRIBE, "yza.host"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1134", KafkaACLPatternType_PREFIXED, "User:1134", KafkaACLOperation_READ, "yza.host"):      nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1134", KafkaACLOperation_WRITE, "bcd.host"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1134", KafkaACLOperation_DESCRIBE, "bcd.host"): nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "1134", KafkaACLPatternType_PREFIXED, "User:1134", KafkaACLOperation_READ, "bcd.host"):      nil,
			},
			"Streams ACL Mismatch"},
		{"./testdata/utm_mapping/acl/shepherd/definitions_5.yaml",
			&ACLMapping{
				// ksql
				// User:1141
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1141", KafkaACLOperation_READ, "*"):                                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1141", KafkaACLOperation_DESCRIBE, "*"):                             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1141", KafkaACLPatternType_PREFIXED, "User:1141", KafkaACLOperation_ALL, "*"):                   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "_confluent-ksql-1141", KafkaACLPatternType_PREFIXED, "User:1141", KafkaACLOperation_ALL, "*"):                   nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "1141ksql_processing_log", KafkaACLPatternType_LITERAL, "User:1141", KafkaACLOperation_ALL, "*"):                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1141_command_topic", KafkaACLPatternType_LITERAL, "User:1141", KafkaACLOperation_WRITE, "*"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1141_command_topic", KafkaACLPatternType_LITERAL, "User:1141", KafkaACLOperation_DESCRIBE, "*"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "ksql-1141", KafkaACLPatternType_LITERAL, "User:1141", KafkaACLOperation_DESCRIBE, "*"):                nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1141", KafkaACLOperation_DESCRIBECONFIGS, "*"):             nil,
				// User:1142
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1142", KafkaACLOperation_WRITE, "*"):                                nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1142", KafkaACLOperation_DESCRIBE, "*"):                             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1142", KafkaACLPatternType_PREFIXED, "User:1142", KafkaACLOperation_ALL, "*"):                   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "_confluent-ksql-1142", KafkaACLPatternType_PREFIXED, "User:1142", KafkaACLOperation_ALL, "*"):                   nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "1142ksql_processing_log", KafkaACLPatternType_LITERAL, "User:1142", KafkaACLOperation_ALL, "*"):                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1142_command_topic", KafkaACLPatternType_LITERAL, "User:1142", KafkaACLOperation_WRITE, "*"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1142_command_topic", KafkaACLPatternType_LITERAL, "User:1142", KafkaACLOperation_DESCRIBE, "*"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "ksql-1142", KafkaACLPatternType_LITERAL, "User:1142", KafkaACLOperation_DESCRIBE, "*"):                nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1142", KafkaACLOperation_DESCRIBECONFIGS, "*"):             nil,
				// User:1143
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_READ, "efg.host"):                                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBE, "efg.host"):                             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1143", KafkaACLPatternType_PREFIXED, "User:1143", KafkaACLOperation_ALL, "efg.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "_confluent-ksql-1143", KafkaACLPatternType_PREFIXED, "User:1143", KafkaACLOperation_ALL, "efg.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "1143ksql_processing_log", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_ALL, "efg.host"):                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1143_command_topic", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_WRITE, "efg.host"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1143_command_topic", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBE, "efg.host"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "ksql-1143", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBE, "efg.host"):                nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBECONFIGS, "efg.host"):             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_READ, "hij.host"):                                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBE, "hij.host"):                             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1143", KafkaACLPatternType_PREFIXED, "User:1143", KafkaACLOperation_ALL, "hij.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "_confluent-ksql-1143", KafkaACLPatternType_PREFIXED, "User:1143", KafkaACLOperation_ALL, "hij.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "1143ksql_processing_log", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_ALL, "hij.host"):                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1143_command_topic", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_WRITE, "hij.host"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1143_command_topic", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBE, "hij.host"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "ksql-1143", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBE, "hij.host"):                nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1143", KafkaACLOperation_DESCRIBECONFIGS, "hij.host"):             nil,
				// User:1144
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_WRITE, "klm.host"):                                nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBE, "klm.host"):                             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1144", KafkaACLPatternType_PREFIXED, "User:1144", KafkaACLOperation_ALL, "klm.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "_confluent-ksql-1144", KafkaACLPatternType_PREFIXED, "User:1144", KafkaACLOperation_ALL, "klm.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "1144ksql_processing_log", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_ALL, "klm.host"):                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1144_command_topic", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_WRITE, "klm.host"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1144_command_topic", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBE, "klm.host"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "ksql-1144", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBE, "klm.host"):                nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBECONFIGS, "klm.host"):             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_WRITE, "nop.host"):                                nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "test.1", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBE, "nop.host"):                             nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1144", KafkaACLPatternType_PREFIXED, "User:1144", KafkaACLOperation_ALL, "nop.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_GROUP, "_confluent-ksql-1144", KafkaACLPatternType_PREFIXED, "User:1144", KafkaACLOperation_ALL, "nop.host"):                   nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "1144ksql_processing_log", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_ALL, "nop.host"):                 nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1144_command_topic", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_WRITE, "nop.host"):    nil,
				constructACLDetailsObject(KafkaResourceType_TOPIC, "_confluent-ksql-1144_command_topic", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBE, "nop.host"): nil,
				constructACLDetailsObject(KafkaResourceType_TRANSACTIONALID, "ksql-1144", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBE, "nop.host"):                nil,
				constructACLDetailsObject(KafkaResourceType_CLUSTER, "kafka-cluster", KafkaACLPatternType_LITERAL, "User:1144", KafkaACLOperation_DESCRIBECONFIGS, "nop.host"):             nil,
			},
			"KSQL ACL Mismatch"},
	}
)

func (s *StackSuite) TestStackSuite_ExternalFunctions_KafkaACLMapping() {
	// aclMappingCases is defined in external_functions_test.go
	for _, c := range kafkaAclMappingCases {
		os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", c.inDefFileName)
		SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", ""), true)
		ConfMaps.utm = UserTopicMapping{}
		GenerateMappings()
		result := KafkaACLOperation_ANY.GenerateACLMappingStructures("", ConfMaps.utm.getShepherdACLList())
		s.True(reflect.DeepEqual(c.out, result), fmt.Sprintf("Expected Value: %v \n\n  Actual Value: %v \n\nFilename: %v\n\nError: Failed while invoking GenerateACLMappingStructures with error %v", c.out, result, c.inDefFileName, c.err))
	}
}
