package internal

import (
	"fmt"
	"strings"

	mapset "github.com/deckarep/golang-set"
)

type ShepherdConfig struct {
	Config Config `yaml:"config"`
}

type Config struct {
	Clusters []Cluster `yaml:"clusters,flow"`
}

type Cluster struct {
	Name                 string    `yaml:"name"`
	BootstrapServer      string    `yaml:"bootstrap.server"`
	SecurityType         string    `yaml:"security.type"`
	Configs              []NVPairs `yaml:"config,flow"`
	EnvironmentOverrides []NVPairs `yaml:"envOverrides,flow"`
}

type RootStruct struct {
	Blueprint  Blueprints
	Definition Definitions
}

type Blueprints struct {
	Blueprints Blueprint `yaml:"blueprints"`
}

type Definitions struct {
	Definitions Definition `yaml:"definitions"`
}

type Blueprint struct {
	Topic  TopicBlueprints   `yaml:"topic,omitempty"`
	Policy PolicyBlueprints  `yaml:"policy,omitempty"`
	Custom []CustomBlueprint `yaml:"customEnums,flow,omitempty"`
}

type Definition struct {
	AdhocTopicsDefinitions TopicDefinitions   `yaml:"adhoc,omitempty"`
	ScopeFlow              []ScopeDefinitions `yaml:"scopeFlow,flow,omitempty"`
}

type PolicyBlueprints struct {
	TopicPolicy TopicPolicyConfigs `yaml:"topicPolicy,omitempty"`
	ACLPolicy   ACLPolicyConfigs   `yaml:"aclPolicy,omitempty"`
}

type TopicPolicyConfigs struct {
	Defaults  []NVPairs            `yaml:"defaults,flow,omitempty"`
	Overrides TopicPolicyOverrides `yaml:"overrides,omitempty"`
}

type ACLPolicyConfigs struct {
	SetupACLs    bool   `yaml:"setupACLs,omitempty"`
	ACLType      string `yaml:"aclType,omitempty"`
	OptimizeACLs bool   `yaml:"optimizeACLs,omitempty"`
}

type TopicPolicyOverrides struct {
	Whitelist []string `yaml:"whitelist,flow,omitempty"`
	Blacklist []string `yaml:"blacklist,flow,omitempty"`
}

type TopicBlueprints struct {
	TopicConfigs []TopicBlueprintConfigs `yaml:"topicConfigs,flow,omitempty"`
}

type TopicBlueprintConfigs struct {
	Name      string    `yaml:"name,omitempty"`
	Overrides []NVPairs `yaml:"configOverrides,omitempty,flow"`
}

type CustomBlueprint struct {
	Name               string   `yaml:"name,omitempty"`
	Values             []string `yaml:"values,flow,omitempty"`
	IncludeInTopicName bool     `yaml:"mandatoryInTopicName,omitempty"`
}

type NVPairs map[string]string

type TopicDefinitions struct {
	TopicDefs []TopicDefinition `yaml:"topics,flow,omitempty"`
}

type TopicDefinition struct {
	Name                  []string         `yaml:"name,flow,omitempty"`
	Clients               ClientDefinition `yaml:"clients,omitempty"`
	FilterScope           []string         `yaml:"filterScope,flow,omitempty"`
	TopicBlueprintEnumRef string           `yaml:"blueprintEnum,omitempty"`
	ConfigOverrides       []NVPairs        `yaml:"configOverrides,flow,omitempty"`
}

type ClientDefinition struct {
	Consumers  []ConsumerDefinition  `yaml:"consumers,flow,omitempty"`
	Producers  []ProducerDefinition  `yaml:"producers,flow,omitempty"`
	Connectors []ConnectorDefinition `yaml:"connectors,flow,omitempty"`
}

type ConsumerDefinition struct {
	ID    string `yaml:"id,omitempty"`
	Group string `yaml:"group,omitempty"`
}

type ProducerDefinition struct {
	ID    string `yaml:"id,omitempty"`
	Group string `yaml:"group,omitempty"`
}

type ConnectorDefinition struct {
	ID   string `yaml:"id,omitempty"`
	Type string `yaml:"type,omitempty"`
}

type ScopeDefinitions struct {
	Scope ScopeNodeDefinition `yaml:"scope,omitempty"`
}

type ScopeNodeDefinition struct {
	ShortName          string               `yaml:"shortName,omitempty"`
	Values             []string             `yaml:"values,flow,omitempty"`
	IncludeInTopicName bool                 `yaml:"addToTopicName,omitempty"`
	CustomEnumRef      string               `yaml:"customEnum,omitempty"`
	Topics             TopicDefinition      `yaml:"topics,omitempty"`
	Clients            ClientDefinition     `yaml:"clients,omitempty"`
	Child              *ScopeNodeDefinition `yaml:"child,omitempty"`
}

func (sc Definitions) PrettyPrintScope() {
	for _, v1 := range sc.Definitions.ScopeFlow {
		v1.Scope.prettyPrintSND(0)
	}
}

func (snd ScopeNodeDefinition) prettyPrintSND(tabCounter int) {
	fmt.Println(strings.Repeat("  ", tabCounter), "Short Name:", snd.ShortName)
	fmt.Println(strings.Repeat("  ", tabCounter), "Custom Enum Ref:", snd.CustomEnumRef)
	fmt.Println(strings.Repeat("  ", tabCounter), "Values:", snd.Values)
	fmt.Println(strings.Repeat("  ", tabCounter), "Include in Topic Name Flag:", snd.IncludeInTopicName)
	fmt.Println(strings.Repeat("  ", tabCounter), "Topics:", snd.Topics)
	fmt.Println(strings.Repeat("  ", tabCounter), "Clients:", snd.Clients)
	if snd.Child != nil {
		fmt.Println(strings.Repeat("  ", tabCounter), "Child Node:")
		snd.Child.prettyPrintSND(tabCounter + 1)
	}
}

type TopicNameString string
type TopicNamesSet mapset.Set

type TopicConfigSet mapset.Set
type TopicConfigMapping map[string]NVPairs

type UserTopicMapping map[UserTopicMappingKey]UserTopicMappingValue

type UserTopicMappingKey struct {
	ID         string
	ClientType ClientType
}

type UserTopicMappingValue struct {
	TopicList []string
}

func (utm *UserTopicMapping) PrettyPrintUTM() {
	defer TW.Flush()

	fmt.Fprintf(TW, "\n %s\t%s\t%s\t", "User ID", "Client Type", "Topics Used")
	fmt.Fprintf(TW, "\n %s\t%s\t%s\t", "-------", "-----------", "-----------")

	for k1, v1 := range *utm {
		fmt.Fprintf(TW, "\n %s\t%s\t%s\t", k1.ID, k1.ClientType.String(), strings.Join(v1.TopicList, ", "))
	}
	fmt.Fprintln(TW, " ")
}

func (tcm *TopicConfigMapping) PrettyPrintTCM() {
	defer TW.Flush()

	fmt.Fprintf(TW, "\n %s\t%s\t", "Key", "NVPairs")
	fmt.Fprintf(TW, "\n %s\t%s\t", "---", "-------")

	for k1, v1 := range *tcm {
		fmt.Fprintf(TW, "\n %s\t%s\t", k1, v1)
	}
	fmt.Fprintln(TW, " ")
}
