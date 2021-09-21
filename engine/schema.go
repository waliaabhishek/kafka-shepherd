package engine

import "errors"

type CustomParser interface {
	readValuesFromENV() error
}

type NVPairs map[string]string

func (nv *NVPairs) readValuesFromENV() error {
	for k, v := range *nv {
		(*nv)[k] = envVarCheckNReplace(v, "")
	}
	return nil
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

func (c *ShepherdCore) readValuesFromENV() error {
	err := c.Configs.readValuesFromENV()
	if err != nil {
		return err
	}
	err = c.Blueprints.readValuesFromENV()
	if err != nil {
		return err
	}
	err = c.Definitions.readValuesFromENV()
	if err != nil {
		return err
	}
	return nil
}

type ShepherdConfig struct {
	ConfigRoot ConfigRoot `yaml:"configs"`
}

func (c *ShepherdConfig) readValuesFromENV() error {
	err := c.ConfigRoot.readValuesFromENV()
	if err != nil {
		return err
	}
	return nil
}

type ConfigRoot struct {
	ShepherdCoreConfig ShepherdCoreConfig `yaml:"core"`
	Clusters           []ShepherdCluster  `yaml:"clusters,flow"`
}

func (c *ConfigRoot) readValuesFromENV() error {
	err := c.ShepherdCoreConfig.readValuesFromENV()
	if err != nil {
		return err
	}
	for idx := 0; idx < len(c.Clusters); idx++ {
		err = c.Clusters[idx].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type ShepherdCoreConfig struct {
	SeperatorToken      string `yaml:"separatorToken"`
	DeleteUnknownTopics bool   `yaml:"deleteUnknownTopics"`
	DeleteUnknownACLs   bool   `yaml:"deleteUnknownACLs"`
}

func (c *ShepherdCoreConfig) readValuesFromENV() error {
	c.SeperatorToken = envVarCheckNReplace(c.SeperatorToken, ".")
	return nil
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

func (c *ShepherdCluster) readValuesFromENV() error {
	c.Name = envVarCheckNReplace(c.Name, "")
	for i, v := range c.BootstrapServers {
		c.BootstrapServers[i] = envVarCheckNReplace(v, "")
	}
	c.ACLManager = envVarCheckNReplace(c.ACLManager, "kafka_acl")
	c.TopicManager = envVarCheckNReplace(c.TopicManager, "sarama")
	c.ClientID = envVarCheckNReplace(c.ClientID, "")
	err := c.TLSDetails.readValuesFromENV()
	if err != nil {
		return err
	}
	c.Configs = streamlineNVPairs(c.Configs)
	for idx := 0; idx < len(c.Configs); idx++ {
		err = c.Configs[idx].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	c.ClusterDetails = streamlineNVPairs(c.ClusterDetails)
	for idx := 0; idx < len(c.ClusterDetails); idx++ {
		err = c.ClusterDetails[idx].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type ShepherdCerts struct {
	Enable2WaySSL      bool     `yaml:"enable2WaySSL,flow"`
	TrustedCerts       []string `yaml:"trustedCerts,flow"`
	ClientCert         string   `yaml:"clientCert"`
	PrivateKey         string   `yaml:"privateKey"`
	PrivateKeyPassword string   `yaml:"privateKeyPass"`
}

func (c *ShepherdCerts) readValuesFromENV() error {
	for i, v := range c.TrustedCerts {
		c.TrustedCerts[i] = envVarCheckNReplace(v, "")
	}
	c.ClientCert = envVarCheckNReplace(c.ClientCert, "")
	c.PrivateKey = envVarCheckNReplace(c.PrivateKey, "")
	c.PrivateKeyPassword = envVarCheckNReplace(c.PrivateKeyPassword, "")
	return nil
}

type ShepherdBlueprint struct {
	Blueprint BlueprintRoot `yaml:"blueprints"`
}

func (c *ShepherdBlueprint) readValuesFromENV() error {
	err := c.Blueprint.readValuesFromENV()
	if err != nil {
		return err
	}
	return nil
}

type BlueprintRoot struct {
	Topic       TopicBlueprints  `yaml:"topic,omitempty"`
	Policy      PolicyBlueprints `yaml:"policy,omitempty"`
	CustomEnums []CustomEnums    `yaml:"customEnums,flow,omitempty"`
}

func (c *BlueprintRoot) readValuesFromENV() error {
	err := c.Topic.readValuesFromENV()
	if err != nil {
		return err
	}
	err = c.Policy.readValuesFromENV()
	if err != nil {
		return err
	}
	for i := 0; i < len(c.CustomEnums); i++ {
		err = c.CustomEnums[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type TopicBlueprints struct {
	TopicConfigs []TopicBlueprintConfigs `yaml:"topicConfigs,flow,omitempty"`
}

func (c *TopicBlueprints) readValuesFromENV() error {
	for i := 0; i < len(c.TopicConfigs); i++ {
		err := c.TopicConfigs[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type TopicBlueprintConfigs struct {
	Name      string    `yaml:"name"`
	Overrides []NVPairs `yaml:"configOverrides,omitempty,flow"`
}

func (c *TopicBlueprintConfigs) readValuesFromENV() error {
	c.Name = envVarCheckNReplace(c.Name, "")
	c.Overrides = streamlineNVPairs(c.Overrides)
	for i := 0; i < len(c.Overrides); i++ {
		err := c.Overrides[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type PolicyBlueprints struct {
	TopicPolicy *TopicPolicyConfigs `yaml:"topicPolicy,omitempty"`
	ACLPolicy   *ACLPolicyConfigs   `yaml:"aclPolicy,omitempty"`
}

func (c *PolicyBlueprints) readValuesFromENV() error {
	if c.TopicPolicy != nil {
		err := c.TopicPolicy.readValuesFromENV()
		if err != nil {
			return err
		}
	}
	if c.ACLPolicy != nil {
		err := c.ACLPolicy.readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type TopicPolicyConfigs struct {
	Defaults  []NVPairs            `yaml:"defaults,flow,omitempty"`
	Overrides TopicPolicyOverrides `yaml:"overrides,omitempty"`
}

func (c *TopicPolicyConfigs) readValuesFromENV() error {
	c.Defaults = streamlineNVPairs(c.Defaults)
	for i := 0; i < len(c.Defaults); i++ {
		err := c.Defaults[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	err := c.Overrides.readValuesFromENV()
	if err != nil {
		return err
	}
	return nil
}

type TopicPolicyOverrides struct {
	Whitelist []string `yaml:"whitelist,flow,omitempty"`
	Blacklist []string `yaml:"blacklist,flow,omitempty"`
}

func (c *TopicPolicyOverrides) readValuesFromENV() error {
	for i, v := range c.Whitelist {
		c.Whitelist[i] = envVarCheckNReplace(v, "")
	}
	for i, v := range c.Blacklist {
		c.Blacklist[i] = envVarCheckNReplace(v, "")
	}
	return nil
}

type ACLPolicyConfigs struct {
	SetupACLs    bool   `yaml:"setupACLs,omitempty"`
	ACLType      string `yaml:"aclType,omitempty"`
	OptimizeACLs bool   `yaml:"optimizeACLs,omitempty"`
}

func (c *ACLPolicyConfigs) readValuesFromENV() error {
	c.ACLType = envVarCheckNReplace(c.ACLType, "")
	return nil
}

type CustomEnums struct {
	Name               string   `yaml:"name,omitempty"`
	Values             []string `yaml:"values,flow,omitempty"`
	IncludeInTopicName bool     `yaml:"mandatoryInTopicName,omitempty"`
}

func (c *CustomEnums) readValuesFromENV() error {
	c.Name = envVarCheckNReplace(c.Name, "")
	for i, v := range c.Values {
		c.Values[i] = envVarCheckNReplace(v, "")
	}
	return nil
}

type ShepherdDefinition struct {
	DefinitionRoot DefinitionRoot `yaml:"definitions"`
}

func (c *ShepherdDefinition) readValuesFromENV() error {
	err := c.DefinitionRoot.readValuesFromENV()
	if err != nil {
		return err
	}
	return nil
}

type DefinitionRoot struct {
	AdhocConfigs AdhocConfig       `yaml:"adhoc,omitempty"`
	ScopeFlow    []ScopeDefinition `yaml:"scopeFlow,flow,omitempty"`
}

func (c *DefinitionRoot) readValuesFromENV() error {
	err := c.AdhocConfigs.readValuesFromENV()
	if err != nil {
		return err
	}
	for i := 0; i < len(c.ScopeFlow); i++ {
		err = c.ScopeFlow[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type AdhocConfig struct {
	Topics []TopicDefinition `yaml:"topics,flow,omitempty"`
}

func (c *AdhocConfig) readValuesFromENV() error {
	for i := 0; i < len(c.Topics); i++ {
		err := c.Topics[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type TopicDefinition struct {
	Name                  []string         `yaml:"name,flow,omitempty"`
	Clients               ClientDefinition `yaml:"clients,omitempty"`
	IgnoreScope           []string         `yaml:"ignoreScope,flow,omitempty"`
	TopicBlueprintEnumRef string           `yaml:"blueprintEnum,omitempty"`
	ConfigOverrides       []NVPairs        `yaml:"configOverrides,flow,omitempty"`
}

func (c *TopicDefinition) readValuesFromENV() error {
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
		err := c.ConfigOverrides[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type ClientDefinition struct {
	Consumers  []ConsumerDefinition  `yaml:"consumers,flow,omitempty"`
	Producers  []ProducerDefinition  `yaml:"producers,flow,omitempty"`
	Connectors []ConnectorDefinition `yaml:"connectors,flow,omitempty"`
	Streams    []StreamDefinition    `yaml:"streams,flow,omitempty"`
	KSQL       []KSQLDefinition      `yaml:"ksql,flow,omitempty"`
}

func (c *ClientDefinition) readValuesFromENV() error {
	var err error = nil
	for i := 0; i < len(c.Consumers); i++ {
		err = c.Consumers[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(c.Producers); i++ {
		err = c.Producers[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(c.Connectors); i++ {
		err = c.Connectors[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(c.Streams); i++ {
		err = c.Streams[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(c.KSQL); i++ {
		err = c.KSQL[i].readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
}

type ConsumerDefinition struct {
	Principal string   `yaml:"id,omitempty"`
	Group     string   `yaml:"group,omitempty"`
	Hostnames []string `yaml:"hostnames,omitempty,flow"`
}

func (c *ConsumerDefinition) readValuesFromENV() error {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	c.Group = envVarCheckNReplace(c.Group, "")
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	return nil
}

type ProducerDefinition struct {
	Principal         string   `yaml:"id,omitempty"`
	Group             string   `yaml:"group,omitempty"`
	Hostnames         []string `yaml:"hostnames,omitempty,flow"`
	EnableIdempotence bool     `yaml:"enableIdempotence"`
	TransactionalID   bool     `yaml:"enableTransactions"`
}

func (c *ProducerDefinition) readValuesFromENV() error {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	c.Group = envVarCheckNReplace(c.Group, "")
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	if c.EnableIdempotence && c.Group == "" {
		logger.Errorw("If Idempotence is enabled, Producer needs to have a group defined.",
			"Producer Principal", c.Principal,
			"Producer Group", c.Group,
			"Idempotence Enabled", c.EnableIdempotence)
		return errors.New("if idempotence is enabled, producer needs to have a group defined")
	}
	if c.TransactionalID && c.Group == "" {
		logger.Errorw("If Transactions are enabled, Producer needs to have a group defined.",
			"Producer Principal", c.Principal,
			"Producer Group", c.Group,
			"Transactions Enabled", c.TransactionalID)
		return errors.New("if transactions are enabled, producer needs to have a group defined")
	}
	return nil
}

type ConnectorDefinition struct {
	Principal      string   `yaml:"id,omitempty"`
	Type           string   `yaml:"type,omitempty"`
	ClusterNameRef string   `yaml:"clusterName,omitempty"`
	Hostnames      []string `yaml:"hostnames,omitempty,flow"`
}

func (c *ConnectorDefinition) readValuesFromENV() error {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	c.Type = envVarCheckNReplace(c.Type, "")
	if c.Type != "source" && c.Type != "sink" {
		logger.Errorw("Connectors need to be source or sink type. null or any other values are not expected.",
			"Connector Principal", c.Principal,
			"Connector Type provided", c.Type)
		return errors.New("connectors need to be source or sink type. null or any other values are not expected")
	}
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	c.ClusterNameRef = envVarCheckNReplace(c.ClusterNameRef, "")
	return nil
}

type StreamDefinition struct {
	Principal string   `yaml:"id,omitempty"`
	Type      string   `yaml:"type,omitempty"`
	Group     string   `yaml:"group,omitempty"`
	Hostnames []string `yaml:"hostnames,omitempty,flow"`
}

func (c *StreamDefinition) readValuesFromENV() error {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	c.Type = envVarCheckNReplace(c.Type, "")
	if c.Type != "read" && c.Type != "write" {
		logger.Errorw("Streams need to be read or write type. null or any other values are not expected.",
			"Stream Principal", c.Principal,
			"Stream Type provided", c.Type)
		return errors.New("streams need to be read or write type. null or any other values are not expected")
	}
	c.Group = envVarCheckNReplace(c.Group, "")
	if c.Group == "" {
		logger.Errorw("Streams need a group Name. It is the application.id that the Streams application is expected to use. null is not expected.",
			"Stream Principal", c.Principal,
			"Stream Group Name", c.Group)
		return errors.New("streams need a group name. it is the application.id that the streams application is expected to use. null is not expected")
	}
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	return nil
}

type KSQLDefinition struct {
	Principal      string   `yaml:"id,omitempty"`
	Type           string   `yaml:"type,omitempty"`
	ClusterNameRef string   `yaml:"ksql.service.id,omitempty"`
	Hostnames      []string `yaml:"hostnames,omitempty,flow"`
}

func (c *KSQLDefinition) readValuesFromENV() error {
	c.Principal = envVarCheckNReplace(c.Principal, "")
	c.Type = envVarCheckNReplace(c.Type, "")
	if len(c.Hostnames) == 0 {
		c.Hostnames = append(c.Hostnames, "*")
	} else {
		for i, v := range c.Hostnames {
			c.Hostnames[i] = envVarCheckNReplace(v, "")
		}
	}
	if c.Type != "read" && c.Type != "write" {
		logger.Errorw("KSQL need to be read or write type. null or any other values are not expected.",
			"KSQL Principal", c.Principal,
			"KSQL Type provided", c.Type)
		return errors.New("ksql need to be read or write type. null or any other values are not expected")
	}
	c.ClusterNameRef = envVarCheckNReplace(c.ClusterNameRef, "")
	if c.ClusterNameRef == "" {
		logger.Errorw("KSQL cluster id is required. It is the ksql.service.id that the KSQL user is expected to use. null is not expected.",
			"KSQL Principal", c.Principal,
			"KSQL Group Name", c.ClusterNameRef)
		return errors.New("ksql cluster id is required. it is the ksql.service.id that the ksql user is expected to use. null is not expected.")
	}
	return nil
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

func (c *ScopeDefinition) readValuesFromENV() error {
	c.ShortName = envVarCheckNReplace(c.ShortName, "")
	for i, v := range c.Values {
		(c.Values)[i] = envVarCheckNReplace(v, "")
	}
	c.CustomEnumRef = envVarCheckNReplace(c.CustomEnumRef, "")
	err := c.Topics.readValuesFromENV()
	if err != nil {
		return err
	}
	err = c.Clients.readValuesFromENV()
	if err != nil {
		return err
	}
	if c.Child != nil {
		err = c.Child.readValuesFromENV()
		if err != nil {
			return err
		}
	}
	return nil
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
