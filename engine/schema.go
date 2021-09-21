package engine

type CustomParser interface {
	readValuesFromENV()
}

type NVPairs map[string]string

func (nv *NVPairs) readValuesFromENV() {
	for k, v := range *nv {
		(*nv)[k] = envVarCheckNReplace(v, "")
	}
}

/*
	This struct holds all the available configuration maps that are calculated
	and arranged by the core code. These can be used by the implementation layers
	to calculate some custom behaviors. There will be additional convenience
	functions added as and when needed on top of this struct so that you have
	one layer to access everything.
*/
type ConfigurationMaps struct {
	TCM TopicConfigMapping
	utm UserTopicMapping
	CCM ClusterConfigMapping
}

/*
	This is core structure of the configuration which holds pointers to all the
	available configuration fields in a structured way. This will be used gather
	and setup all and every Configuration mapper set up by the core platform.
*/
type ShepherdCore struct {
	Configs     ShepherdConfig
	Blueprints  ShepherdBlueprint
	Definitions ShepherdDefinition
}

func (c *ShepherdCore) readValuesFromENV() {
	c.Configs.readValuesFromENV()
	c.Blueprints.readValuesFromENV()
	c.Definitions.readValuesFromENV()
}

type ShepherdConfig struct {
	ConfigRoot ConfigRoot `yaml:"configs"`
}

func (c *ShepherdConfig) readValuesFromENV() {
	c.ConfigRoot.readValuesFromENV()
}

type ConfigRoot struct {
	ShepherdCoreConfig ShepherdCoreConfig `yaml:"core"`
	Clusters           []ShepherdCluster  `yaml:"clusters,flow"`
}

func (c *ConfigRoot) readValuesFromENV() {
	c.ShepherdCoreConfig.readValuesFromENV()
	for idx := 0; idx < len(c.Clusters); idx++ {
		c.Clusters[idx].readValuesFromENV()
	}
}

type ShepherdCoreConfig struct {
	SeperatorToken      string `yaml:"separatorToken"`
	DeleteUnknownTopics bool   `yaml:"deleteUnknownTopics"`
	DeleteUnknownACLs   bool   `yaml:"deleteUnknownACLs"`
}

func (c *ShepherdCoreConfig) readValuesFromENV() {
	c.SeperatorToken = envVarCheckNReplace(c.SeperatorToken, ".")
}

type ShepherdCluster struct {
	Name             string        `yaml:"name"`
	IsEnabled        bool          `yaml:"isEnabled"`
	BootstrapServers []string      `yaml:"bootstrapServers,flow"`
	ACLManager       string        `yaml:"aclManager"`
	TopicManager     string        `yaml:"topicManager" default:"sarama"`
	ClientID         string        `yaml:"clientId"`
	TLSDetails       ShepherdCerts `yaml:"tlsDetails,omitempty"`
	Configs          []NVPairs     `yaml:"configOverrides,flow"`
	ClusterDetails   []NVPairs     `yaml:"clusterDetails,flow"`
}

func (c *ShepherdCluster) readValuesFromENV() {
	c.Name = envVarCheckNReplace(c.Name, "")
	for i, v := range c.BootstrapServers {
		c.BootstrapServers[i] = envVarCheckNReplace(v, "")
	}
	c.ACLManager = envVarCheckNReplace(c.ACLManager, "kafka_acl")
	c.TopicManager = envVarCheckNReplace(c.TopicManager, "sarama")
	c.ClientID = envVarCheckNReplace(c.ClientID, "")
	c.TLSDetails.readValuesFromENV()
	c.Configs = streamlineNVPairs(c.Configs)
	for idx := 0; idx < len(c.Configs); idx++ {
		c.Configs[idx].readValuesFromENV()
	}
	c.ClusterDetails = streamlineNVPairs(c.ClusterDetails)
	for idx := 0; idx < len(c.ClusterDetails); idx++ {
		c.ClusterDetails[idx].readValuesFromENV()
	}
}

type ShepherdCerts struct {
	Enable2WaySSL      bool     `yaml:"enable2WaySSL,flow"`
	TrustedCerts       []string `yaml:"trustedCerts,flow"`
	ClientCert         string   `yaml:"clientCert"`
	PrivateKey         string   `yaml:"privateKey"`
	PrivateKeyPassword string   `yaml:"privateKeyPass"`
}

func (c *ShepherdCerts) readValuesFromENV() {
	for i, v := range c.TrustedCerts {
		c.TrustedCerts[i] = envVarCheckNReplace(v, "")
	}
	c.ClientCert = envVarCheckNReplace(c.ClientCert, "")
	c.PrivateKey = envVarCheckNReplace(c.PrivateKey, "")
	c.PrivateKeyPassword = envVarCheckNReplace(c.PrivateKeyPassword, "")
}

type ShepherdBlueprint struct {
	Blueprint BlueprintRoot `yaml:"blueprints"`
}

func (c *ShepherdBlueprint) readValuesFromENV() {
	c.Blueprint.readValuesFromENV()
}

type BlueprintRoot struct {
	Topic       TopicBlueprints  `yaml:"topic,omitempty"`
	Policy      PolicyBlueprints `yaml:"policy,omitempty"`
	CustomEnums []CustomEnums    `yaml:"customEnums,flow,omitempty"`
}

func (c *BlueprintRoot) readValuesFromENV() {
	c.Topic.readValuesFromENV()
	c.Policy.readValuesFromENV()
	for i := 0; i < len(c.CustomEnums); i++ {
		c.CustomEnums[i].readValuesFromENV()
	}
}

type TopicBlueprints struct {
	TopicConfigs []TopicBlueprintConfigs `yaml:"topicConfigs,flow,omitempty"`
}

func (c *TopicBlueprints) readValuesFromENV() {
	for i := 0; i < len(c.TopicConfigs); i++ {
		c.TopicConfigs[i].readValuesFromENV()
	}
}

type TopicBlueprintConfigs struct {
	Name      string    `yaml:"name"`
	Overrides []NVPairs `yaml:"configOverrides,omitempty,flow"`
}

func (c *TopicBlueprintConfigs) readValuesFromENV() {
	c.Name = envVarCheckNReplace(c.Name, "")
	c.Overrides = streamlineNVPairs(c.Overrides)
	for i := 0; i < len(c.Overrides); i++ {
		c.Overrides[i].readValuesFromENV()
	}
}

type PolicyBlueprints struct {
	TopicPolicy *TopicPolicyConfigs `yaml:"topicPolicy,omitempty"`
	ACLPolicy   *ACLPolicyConfigs   `yaml:"aclPolicy,omitempty"`
}

func (c *PolicyBlueprints) readValuesFromENV() {
	if c.TopicPolicy != nil {
		c.TopicPolicy.readValuesFromENV()
	}
	if c.ACLPolicy != nil {
		c.ACLPolicy.readValuesFromENV()
	}
}

type TopicPolicyConfigs struct {
	Defaults  []NVPairs            `yaml:"defaults,flow,omitempty"`
	Overrides TopicPolicyOverrides `yaml:"overrides,omitempty"`
}

func (c *TopicPolicyConfigs) readValuesFromENV() {
	c.Defaults = streamlineNVPairs(c.Defaults)
	for i := 0; i < len(c.Defaults); i++ {
		c.Defaults[i].readValuesFromENV()
	}
	c.Overrides.readValuesFromENV()
}

type TopicPolicyOverrides struct {
	Whitelist []string `yaml:"whitelist,flow,omitempty"`
	Blacklist []string `yaml:"blacklist,flow,omitempty"`
}

func (c *TopicPolicyOverrides) readValuesFromENV() {
	for i, v := range c.Whitelist {
		c.Whitelist[i] = envVarCheckNReplace(v, "")
	}
	for i, v := range c.Blacklist {
		c.Blacklist[i] = envVarCheckNReplace(v, "")
	}
}

type ACLPolicyConfigs struct {
	SetupACLs    bool   `yaml:"setupACLs,omitempty"`
	ACLType      string `yaml:"aclType,omitempty"`
	OptimizeACLs bool   `yaml:"optimizeACLs,omitempty"`
}

func (c *ACLPolicyConfigs) readValuesFromENV() {
	c.ACLType = envVarCheckNReplace(c.ACLType, "")
}

type CustomEnums struct {
	Name               string   `yaml:"name,omitempty"`
	Values             []string `yaml:"values,flow,omitempty"`
	IncludeInTopicName bool     `yaml:"mandatoryInTopicName,omitempty"`
}

func (c *CustomEnums) readValuesFromENV() {
	c.Name = envVarCheckNReplace(c.Name, "")
	for i, v := range c.Values {
		c.Values[i] = envVarCheckNReplace(v, "")
	}
}

type ShepherdDefinition struct {
	DefinitionRoot DefinitionRoot `yaml:"definitions"`
}

func (c *ShepherdDefinition) readValuesFromENV() {
	c.DefinitionRoot.readValuesFromENV()
}

type DefinitionRoot struct {
	AdhocConfigs AdhocConfig       `yaml:"adhoc,omitempty"`
	ScopeFlow    []ScopeDefinition `yaml:"scopeFlow,flow,omitempty"`
}

func (c *DefinitionRoot) readValuesFromENV() {
	c.AdhocConfigs.readValuesFromENV()
	for i := 0; i < len(c.ScopeFlow); i++ {
		c.ScopeFlow[i].readValuesFromENV()
	}
}

type AdhocConfig struct {
	Topics []TopicDefinition `yaml:"topics,flow,omitempty"`
}

func (c *AdhocConfig) readValuesFromENV() {
	for i := 0; i < len(c.Topics); i++ {
		c.Topics[i].readValuesFromENV()
	}
}

type TopicDefinition struct {
	Name                  []string         `yaml:"name,flow,omitempty"`
	Clients               ClientDefinition `yaml:"clients,omitempty"`
	IgnoreScope           []string         `yaml:"ignoreScope,flow,omitempty"`
	TopicBlueprintEnumRef string           `yaml:"blueprintEnum,omitempty"`
	ConfigOverrides       []NVPairs        `yaml:"configOverrides,flow,omitempty"`
}

func (c *TopicDefinition) readValuesFromENV() {
	for i, v := range c.Name {
		c.Name[i] = envVarCheckNReplace(v, "")
	}
	c.Clients.readValuesFromENV()
	for i, v := range c.IgnoreScope {
		c.IgnoreScope[i] = envVarCheckNReplace(v, "")
	}
	c.TopicBlueprintEnumRef = envVarCheckNReplace(c.TopicBlueprintEnumRef, "")
	c.ConfigOverrides = streamlineNVPairs(c.ConfigOverrides)
	for i := 0; i < len(c.ConfigOverrides); i++ {
		c.ConfigOverrides[i].readValuesFromENV()
	}
}

type ClientDefinition struct {
	Consumers  []ConsumerDefinition  `yaml:"consumers,flow,omitempty"`
	Producers  []ProducerDefinition  `yaml:"producers,flow,omitempty"`
	Connectors []ConnectorDefinition `yaml:"connectors,flow,omitempty"`
	Streams    []StreamDefinition    `yaml:"streams,flow,omitempty"`
	KSQL       []KSQLDefinition      `yaml:"ksql,flow,omitempty"`
}

func (c *ClientDefinition) readValuesFromENV() {
	for i := 0; i < len(c.Consumers); i++ {
		c.Consumers[i].readValuesFromENV()
	}
	for i := 0; i < len(c.Producers); i++ {
		c.Producers[i].readValuesFromENV()
	}
	for i := 0; i < len(c.Connectors); i++ {
		c.Connectors[i].readValuesFromENV()
	}
	for i := 0; i < len(c.Streams); i++ {
		c.Streams[i].readValuesFromENV()
	}
	for i := 0; i < len(c.KSQL); i++ {
		c.KSQL[i].readValuesFromENV()
	}
}

type ConsumerDefinition struct {
	Principal string   `yaml:"id,omitempty"`
	Group     string   `yaml:"group,omitempty"`
	Hostnames []string `yaml:"hostnames,omitempty,flow"`
}

func (c *ConsumerDefinition) readValuesFromENV() {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	if len(c.Principal) == 0 {
		logger.Fatal("ID needs to be defined, otherwise the ACL's cannot be set up.")
	}
	c.Group = envVarCheckNReplace(c.Group, "")
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
}

type ProducerDefinition struct {
	Principal         string   `yaml:"id,omitempty"`
	Group             string   `yaml:"group,omitempty"`
	Hostnames         []string `yaml:"hostnames,omitempty,flow"`
	EnableIdempotence bool     `yaml:"enableIdempotence"`
	TransactionalID   bool     `yaml:"enableTransactions"`
}

func (c *ProducerDefinition) readValuesFromENV() {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	if len(c.Principal) == 0 {
		logger.Fatal("ID needs to be defined, otherwise the ACL's cannot be set up.")
	}
	c.Group = envVarCheckNReplace(c.Group, "")
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	if c.EnableIdempotence && c.Group == "" {
		logger.Fatalw("If Idempotence is enabled, Producer needs to have a group defined.",
			"Producer Principal", c.Principal,
			"Producer Group", c.Group,
			"Idempotence Enabled", c.EnableIdempotence)
	}
	if c.TransactionalID && c.Group == "" {
		logger.Fatalw("If Transactions are enabled, Producer needs to have a group defined.",
			"Producer Principal", c.Principal,
			"Producer Group", c.Group,
			"Transactions Enabled", c.TransactionalID)
	}
}

type ConnectorDefinition struct {
	Principal      string   `yaml:"id,omitempty"`
	Type           string   `yaml:"type,omitempty"`
	ClusterNameRef string   `yaml:"clusterName,omitempty"`
	Hostnames      []string `yaml:"hostnames,omitempty,flow"`
}

func (c *ConnectorDefinition) readValuesFromENV() {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	if len(c.Principal) == 0 {
		logger.Fatal("ID needs to be defined, otherwise the ACL's cannot be set up.")
	}
	c.Type = envVarCheckNReplace(c.Type, "")
	if c.Type != "source" && c.Type != "sink" {
		logger.Fatalw("Connectors need to be source or sink type. null or any other values are not expected.",
			"Connector Principal", c.Principal,
			"Connector Type provided", c.Type)
	}
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	c.ClusterNameRef = envVarCheckNReplace(c.ClusterNameRef, "")
}

type StreamDefinition struct {
	Principal string   `yaml:"id,omitempty"`
	Type      string   `yaml:"type,omitempty"`
	Group     string   `yaml:"group,omitempty"`
	Hostnames []string `yaml:"hostnames,omitempty,flow"`
}

func (c *StreamDefinition) readValuesFromENV() {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	if len(c.Principal) == 0 {
		logger.Fatal("ID needs to be defined, otherwise the ACL's cannot be set up.")
	}
	c.Type = envVarCheckNReplace(c.Type, "")
	if c.Type != "read" && c.Type != "write" {
		logger.Fatalw("Streams need to be read or write type. null or any other values are not expected.",
			"Stream Principal", c.Principal,
			"Stream Type provided", c.Type)
	}
	c.Group = envVarCheckNReplace(c.Group, "")
	if c.Group == "" {
		logger.Fatalw("Streams need a group Name. It is the application.id that the Streams application is expected to use. null is not expected.",
			"Stream Principal", c.Principal,
			"Stream Group Name", c.Group)
	}
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
}

type KSQLDefinition struct {
	Principal      string   `yaml:"id,omitempty"`
	Type           string   `yaml:"type,omitempty"`
	ClusterNameRef string   `yaml:"clusterName,omitempty"`
	Hostnames      []string `yaml:"hostnames,omitempty,flow"`
}

func (c *KSQLDefinition) readValuesFromENV() {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	if len(c.Principal) == 0 {
		logger.Fatal("ID needs to be defined, otherwise the ACL's cannot be set up.")
	}
	c.Type = envVarCheckNReplace(c.Type, "")
	if c.Type != "read" && c.Type != "write" {
		logger.Fatalw("KSQL need to be read or write type. null or any other values are not expected.",
			"KSQL Principal", c.Principal,
			"KSQL Type provided", c.Type)
	}
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	c.ClusterNameRef = envVarCheckNReplace(c.ClusterNameRef, "")
	if c.ClusterNameRef == "" {
		logger.Fatalw("KSQL cluster id is required. It is the ksql.service.id that the KSQL user is expected to use. null is not expected.",
			"KSQL Principal", c.Principal,
			"KSQL Group Name", c.ClusterNameRef)
	}
}

type ScopeDefinition struct {
	ShortName          string           `yaml:"shortName,omitempty"`
	Values             []string         `yaml:"values,flow,omitempty"`
	IncludeInTopicName bool             `yaml:"addToTopicName,omitempty"`
	CustomEnumRef      string           `yaml:"blueprintEnum,omitempty"`
	Topics             TopicDefinition  `yaml:"topics,omitempty"`
	Clients            ClientDefinition `yaml:"clients,omitempty"`
	Child              *ScopeDefinition `yaml:"child,omitempty"`
}

func (c *ScopeDefinition) readValuesFromENV() {
	c.ShortName = envVarCheckNReplace(c.ShortName, "")
	for i, v := range c.Values {
		(c.Values)[i] = envVarCheckNReplace(v, "")
	}
	c.CustomEnumRef = envVarCheckNReplace(c.CustomEnumRef, "")
	c.Topics.readValuesFromENV()
	c.Clients.readValuesFromENV()
	if c.Child != nil {
		c.Child.readValuesFromENV()
	}
}

/*
	Topic Config Mapping creates and maintains the Topic mapping provided in the configuration
	files. The Key is topic name and values is a NVPair of all configuration properties
	provided in the configuration files. The logic for overrides, blacklist & whitelist is to
	be handled somewhere. This map will just maintain a uniques set of property values.
*/
type TopicConfigMapping map[string]NVPairs

/*
	This is the map which creates and maintains the mapping provided in the configuration files.
	The Key maintains a unique set of Client IDs, Client Types & their respective Group IDs from
	the configuration file. The value is a slice of topic names that are supposed to be a part of
	this unique group.
*/
type UserTopicMapping map[UserTopicMappingKey]UserTopicMappingValue

type UserTopicMappingKey struct {
	Principal  string
	ClientType ShepherdClientType
	GroupID    string
}

type UserTopicMappingValue struct {
	TopicList []string
	Hostnames []string
}

/*
	Cluster Config Mapping creates and maintains the list of enabled clusters in this map and
	allows for the consumption layers to set up their configurations accordingly. The key will
	contain the unique identification markers and the value includes clientID to be used and
	other supporting properties.
*/
type ClusterConfigMapping map[ClusterConfigMappingKey]ClusterConfigMappingValue
type ClusterConfigMappingKey struct {
	IsEnabled bool
	Name      string
}

type ClusterConfigMappingValue struct {
	IsActive                bool
	IsACLManagementEnabled  bool
	ClusterSecurityProtocol ClusterSecurityProtocol
	ClusterSASLMechanism    ClusterSASLMechanism
	ClientID                string
	Configs                 NVPairs
	ClusterDetails          NVPairs
	TopicManager            string
	ACLManager              string
}
