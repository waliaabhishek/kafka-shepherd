package core

import (
	ksmisc "shepherd/misc"
	"strings"
)

///////////////////////////////////////////////////////////////////////////////
///////// User Topic Maping and Topic Configuration Mapping Generator /////////
///////////////////////////////////////////////////////////////////////////////

func GenerateMappings(sc *ShepherdCore, utm *UserTopicMapping, tcm *TopicConfigMapping) {
	// Adhoc Topic Structure Parsing and table setup
	for _, v := range *sc.Definitions.DefinitionRoot.AdhocConfigs.Topics {
		for _, tName := range *v.Name {
			v.Clients.addClientToUTM(utm, tName)
		}
		tcm.addDataToTopicConfigMapping(sc, &v, *v.Name)
	}

	for _, v := range *sc.Definitions.DefinitionRoot.ScopeFlow {
		iter := 0
		values := [][]string{}
		val1, cont, snd := []string{}, true, &v
		sep := sc.Configs.ConfigRoot.ShepherdCoreConfig.SeperatorToken
		for cont {
			currTopics := append(*snd.Topics.Name, "*")
			currClients := snd.Clients
			currFilters := snd.Topics.IgnoreScope
			val1, cont, snd = snd.getTokensForThisLevel(iter, sc.Blueprints.Blueprint)
			if !ksmisc.IsZero1DSlice(val1) {
				values = append(values, val1)
			}
			iter += 1
			currValues := make([][]string, len(values))
			copy(currValues, values)
			currValues = append(currValues, currTopics)
			currPerms := ksmisc.GetPermutationsString(currValues)
			// Iterate and find any topics that were created and should not exist due to filters
			for _, v2 := range currPerms {
				// Get current Topic Name for the current scope
				temp := strings.Join(v2, sep)
				// Ignore topic combinations with the filterscope at that level from being added to the UTM list
				if !ksmisc.ExistsInString(temp, *currFilters, ksmisc.RemoveValuesFromSlice(currTopics, "*"), sep) {
					// fmt.Println("Inside the filter for *. Topic Name:", temp)
					currClients.addClientToUTM(utm, temp)
					if !strings.HasSuffix(temp, ".*") {
						tcm.addDataToTopicConfigMapping(sc, v.Topics, []string{temp})
					}
				}
			}
		}
	}
	// return utm, tcm
}

func (sc *ShepherdCore) addDataToClusterConfigMapping(ccm *ClusterConfigMapping) {
	for _, cluster := range *sc.Configs.ConfigRoot.Clusters {
		if cluster.IsEnabled == true {
			sp, sc := cluster.understandClusterTopology()
			(*ccm)[ClusterConfigMappingKey{IsEnabled: cluster.IsEnabled,
				Name:                    cluster.Name,
				ClusterSecurityProtocol: sp,
				ClusterSASLMechanism:    sc}] = ClusterConfigMappingValue{ClientID: cluster.ClientID}
		}
	}
}

func (sd ScopeDefinition) getTokensForThisLevel(level int, b *BlueprintRoot) ([]string, bool, *ScopeDefinition) {
	ret := []string{}
	temp := []string{}
	if sd.IncludeInTopicName {
		for _, v := range *b.CustomEnums {
			temp = append(temp, v.Name)
		}
		if len(sd.CustomEnumRef) != 0 {
			if loc, ok := ksmisc.Find(temp, sd.CustomEnumRef); ok {
				for _, v := range *(*b.CustomEnums)[loc].Values {
					if v != "" {
						ret = append(ret, v)
					}
				}
			}
		}
		if sd.Values != nil {
			for _, v := range *sd.Values {
				if v != "" {
					ret = append(ret, v)
				}
			}
		}
	}
	return ret, sd.Child != nil, sd.Child
}

func (c ClientDefinition) addClientToUTM(utm *UserTopicMapping, topic string) {
	for _, v := range *c.Consumers {
		v.addClientToUTM(utm, topic)
	}
	for _, v := range *c.Producers {
		v.addClientToUTM(utm, topic)
	}
	for _, v := range *c.Connectors {
		v.addClientToUTM(utm, topic)
	}
}

type client interface {
	addClientToUTM(utm *UserTopicMapping, t string)
}

func (c ProducerDefinition) addClientToUTM(utm *UserTopicMapping, topicName string) {
	utm.addDataToUserTopicMapping(c.ID, PRODUCER, topicName)
}

func (c ConsumerDefinition) addClientToUTM(utm *UserTopicMapping, topicName string) {
	utm.addDataToUserTopicMapping(c.ID, CONSUMER, topicName)
}

func (c ConnectorDefinition) addClientToUTM(utm *UserTopicMapping, topicName string) {
	utm.addDataToUserTopicMapping(c.ID, c.getConnectorTypeValue(), topicName)
}

func (c ConnectorDefinition) getConnectorTypeValue() ClientType {
	if strings.TrimSpace(strings.ToLower(c.Type)) == "source" {
		return SOURCE_CONNECTOR
	} else {
		return SINK_CONNECTOR
	}
}

/*
	This is the core function that implements addition to the USER to TOPIC Mapping.
*/
func (utm *UserTopicMapping) addDataToUserTopicMapping(clientId string, cType ClientType, topicName string) {
	if val, present := (*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType}]; present {
		if _, ok := ksmisc.Find(val.TopicList, topicName); !ok {
			val.TopicList = append(val.TopicList, topicName)
		}
		(*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType}] = val
	} else {
		(*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType}] = UserTopicMappingValue{TopicList: []string{topicName}}
	}
}

func (tcm *TopicConfigMapping) addDataToTopicConfigMapping(sc *ShepherdCore, td *TopicDefinition, topicName []string) {
	props := make(NVPairs)
	// Get the default values from the blueprints and merge it to the currently applied values.
	props.mergeMaps(sc.Blueprints.Blueprint.Policy.TopicPolicy.Defaults)
	// If Blueprint reference exists in the topic Config, fetch the NV Pairs for that Blueprint and add those props here.
	if td.TopicBlueprintEnumRef != "" {
		props.overrideMergeMaps([]NVPairs{sc.getBlueprintProps((*td).TopicBlueprintEnumRef)},
			*sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Whitelist,
			*sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Blacklist)
	}
	// Merge and overwrite values configured as overrides in the scopeflow or the adhoc topic configs
	props.overrideMergeMaps(td.ConfigOverrides,
		*sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Whitelist,
		*sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Blacklist)
	for _, topic := range topicName {
		(*tcm)[topic] = props
	}
}

func (in *NVPairs) overrideMergeMaps(temp []NVPairs, whitelist []string, blacklist []string) (out *NVPairs) {
	for _, v1 := range temp {
		for k, v := range v1 {
			_, wPresent := ksmisc.Find(whitelist, k)
			_, bPresent := ksmisc.Find(blacklist, k)
			//  The Property is either present in the whitelist or the whitelist is empty
			//  and it is not present the blacklist
			if (wPresent || ksmisc.IsZero1DSlice(whitelist)) && !bPresent {
				(*in)[k] = v
				// This one only cares if the property is present in blacklist and the blacklist
				// slice is non zero
			} else if bPresent && !ksmisc.IsZero1DSlice(blacklist) {
				continue
			}
		}
	}
	return in
}

func (sc *ShepherdCore) getBlueprintProps(blueprintName string) NVPairs {
	if blueprintMap == nil {
		blueprintMap = make(map[string]NVPairs)
		// for _, v := range rs.Blueprint.Blueprints.Topic.TopicConfigs {
		for _, v := range *sc.Blueprints.Blueprint.Topic.TopicConfigs {
			temp := NVPairs{}
			// Get the default values configured in Topic Blueprints Defaults as those act as our baseline
			temp.mergeMaps(sc.Blueprints.Blueprint.Policy.TopicPolicy.Defaults)
			// Get the Base Config Override Values from Topic Blueprint Configuration
			temp.overrideMergeMaps(v.Overrides,
				*sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Whitelist,
				*sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Blacklist)
			blueprintMap[strings.ToLower(strings.TrimSpace(v.Name))] = temp
		}
		// fmt.Println("Blueprint Topic Plan Map:", blueprintMap)
	}
	return blueprintMap[strings.ToLower(strings.TrimSpace(blueprintName))]
}

/*
	This function is used in determining the security mapping of the cluster. The Kafka cluster supports
	multitude of security configs and the mapper makes it easy to determine what security method is implemented
	leveraging the properties provided. The two properties it uses is `security.protocol` and the
	`sasl.mechanism` to parse and understand the security mechanism. Still is a work in progress though.
*/
func (sc *ShepherdCluster) understandClusterTopology() (ClusterSecurityProtocol, ClusterSASLMechanism) {
	var sp ClusterSecurityProtocol = UNIDENTIFIED_SECURITY_PROTOCOL
	// Figure Out the Security Protocol
	switch sc.Configs[0]["security.protocol"] {
	case "SASL_SSL":
		logger.Debug("Inside the SASL_SSL switch statement")
		sp = SASL_SSL
	case "SASL_PLAINTEXT":
		logger.Debug("Inside the SASL_PLAINTEXT switch statement")
		sp = SASL_PLAINTEXT
	case "SSL":
		logger.Debug("Inside the SSL switch statement")
		sp = SSL
	case "":
		logger.Debug("Inside the PLAINTEXT switch statement")
		sp = PLAINTEXT
	default:
		sp = UNIDENTIFIED_SECURITY_PROTOCOL
		logger.Fatalw("Unknown security mode supplied for Cluster Config",
			"Cluster Name", sc.Name,
			"Cluster Security Protocol Provided", sc.Configs[0]["security.protocol"])
	}

	var sm ClusterSASLMechanism = UNIDENTIFIED_SASL_MECHANISM
	// Figure out the sasl mechanism
	switch sc.Configs[0]["sasl.mechanism"] {
	case "PLAIN":
		logger.Debug("Inside the PLAIN switch statement")
		sm = PLAIN
		// temp.ClusterSASLMechanism = PLAIN
	case "SCRAM-SHA-256":
		logger.Debug("Inside SCRAM SSL switch statement")
		sm = SCRAM_SHA_256
	case "SCRAM-SHA-512":
		sm = SCRAM_SHA_512
	case "OAUTHBEARER":
		logger.Debug("Inside the OAUTHBEARER switch statement")
		sm = OAUTHBEARER
	case "":
		logger.Debug("Inside the EMPTY switch statement")
		sm = UNIDENTIFIED_SASL_MECHANISM
		// Check for KRB5
		//
	}

	return sp, sm
}
