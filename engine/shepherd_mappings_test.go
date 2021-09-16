package engine

import (
	"os"
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
			UserTopicMappingKey{Principal: "p1", ClientType: ShepherdClientType_PRODUCER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only Producer ID present"},
		{"./testdata/utm_mapping/producers/definitions_2.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "p1", ClientType: ShepherdClientType_PRODUCER, GroupID: "pgroup"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Producer ID & Group present"},
		{"./testdata/utm_mapping/producers/definitions_3.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER, GroupID: "pgroup"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only Producer Group present"},
		{"./testdata/utm_mapping/producers/definitions_4.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
			"Only 1 Hostname present"},
		{"./testdata/utm_mapping/producers/definitions_5.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host", "def.host"}}},
			"Only Multiple Hostnames present"},
		{"./testdata/utm_mapping/producers/definitions_6.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER, GroupID: "pg1"}:             UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only group name & Idempotence flag present"},
		{"./testdata/utm_mapping/producers/definitions_7.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only group name & Transaction flag is present"},
		{"./testdata/utm_mapping/producers/definitions_8.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only group name , Idempotence flag & transation flag present"},
		{"./testdata/utm_mapping/producers/definitions_9.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdClientType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdClientType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdClientType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Everything Present"},
		{"./testdata/utm_mapping/producers/definitions_10.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdClientType_PRODUCER, GroupID: "pg1"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdClientType_PRODUCER_IDEMPOTENCE, GroupID: "pg1"}:   UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi1", ClientType: ShepherdClientType_TRANSACTIONAL_PRODUCER, GroupID: "pg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi2", ClientType: ShepherdClientType_PRODUCER, GroupID: "pg2"}:               UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "pi2", ClientType: ShepherdClientType_TRANSACTIONAL_PRODUCER, GroupID: "pg2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Everything Present"},
	}

	for _, c := range cases {
		os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", c.inDefFileName)
		SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", ""), true)
		ConfMaps.utm = UserTopicMapping{}
		GenerateMappings()
		s.EqualValues(c.out, ConfMaps.utm, c.err)
	}
}

func (s *StackSuite) TestStackSuite_ExternalFunctions_ConsumerDefinitionsToUTMMapping() {
	cases := []struct {
		inDefFileName string
		out           UserTopicMapping
		err           string
	}{
		{"./testdata/utm_mapping/consumers/definitions_1.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdClientType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only ConsumerID present"},
		{"./testdata/utm_mapping/consumers/definitions_2.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}},
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"*"}}},
			"Only Group present"},
		{"./testdata/utm_mapping/consumers/definitions_3.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
			"Only 1 Hostname present"},
		{"./testdata/utm_mapping/consumers/definitions_4.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host", "def.host"}}},
			"Only Multiple Hostnames present"},
		{"./testdata/utm_mapping/consumers/definitions_5.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdClientType_CONSUMER, GroupID: ""}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
			"Only ID & hostnames present"},
		{"./testdata/utm_mapping/consumers/definitions_6.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "", ClientType: ShepherdClientType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
			"Only group name & hostnames present"},
		{"./testdata/utm_mapping/consumers/definitions_7.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdClientType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdClientType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}}},
			"All 3 attributes present"},
		{"./testdata/utm_mapping/consumers/definitions_8.yaml", UserTopicMapping{
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdClientType_CONSUMER, GroupID: "cg1"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ci1", ClientType: ShepherdClientType_CONSUMER_GROUP, GroupID: "cg1"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"abc.host"}},
			UserTopicMappingKey{Principal: "ci2", ClientType: ShepherdClientType_CONSUMER, GroupID: "cg2"}:       UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}},
			UserTopicMappingKey{Principal: "ci2", ClientType: ShepherdClientType_CONSUMER_GROUP, GroupID: "cg2"}: UserTopicMappingValue{TopicList: []string{"test.1", "test.2"}, Hostnames: []string{"def.host"}}},
			"Multiple consumers present"},
	}

	for _, c := range cases {
		os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", c.inDefFileName)
		SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", ""), true)
		ConfMaps.utm = UserTopicMapping{}
		GenerateMappings()
		s.EqualValues(c.out, ConfMaps.utm, c.err)
	}
}
