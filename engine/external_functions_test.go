package engine

import (
	"os"

	mapset "github.com/deckarep/golang-set"
)

func (s *StackSuite) TestStackSuite_ExternalFunctions_ListTopicsInConfig() {
	os.Setenv("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./testdata/blueprints_0.yaml")
	SpdCore.Blueprints.ParseShepherBlueprints(getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", ""))

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
