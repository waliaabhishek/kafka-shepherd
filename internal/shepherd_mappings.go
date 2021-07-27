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
		v.Clients.addHostnamesToUTM(ConfMaps.UTM)
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
			currClients.addHostnamesToUTM(ConfMaps.UTM)
		}
	}
	// return utm, tcm
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
		ConfMaps.UTM.addDataToUserTopicMapping(v.ID, CT_CONSUMER, v.Group, topic)
	}
	for _, v := range *c.Producers {
		ConfMaps.UTM.addDataToUserTopicMapping(v.ID, CT_PRODUCER, v.Group, topic)
		// v.addClientToUTM(utm, topic)
	}
	for _, v := range *c.Connectors {
		ConfMaps.UTM.addDataToUserTopicMapping(v.ID, v.getConnectorTypeValue(), "", topic)
		// v.addClientToUTM(utm, topic)
	}
}

func (c ConnectorDefinition) getConnectorTypeValue() ClientType {
	if strings.TrimSpace(strings.ToLower(c.Type)) == "source" {
		return CT_SOURCE_CONNECTOR
	} else {
		return CT_SINK_CONNECTOR
	}
}

/*
	This is the core function that implements addition to the USER to TOPIC Mapping.
*/
func (utm *UserTopicMapping) addDataToUserTopicMapping(clientId string, cType ClientType, cGroup string, topicName string) {
	if val, present := (*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType, GroupID: cGroup}]; present {
		if _, ok := ksmisc.Find(val.TopicList, topicName); !ok {
			val.TopicList = append(val.TopicList, topicName)
		}
		(*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType, GroupID: cGroup}] = val
	} else {
		(*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType, GroupID: cGroup}] = UserTopicMappingValue{TopicList: []string{topicName}, Hostnames: []string{}}
	}
}

func (c ClientDefinition) addHostnamesToUTM(utm *UserTopicMapping) {
	for _, v := range *c.Consumers {
		ConfMaps.UTM.addHostnamesToUserTopicMapping(v.ID, CT_CONSUMER, v.Group, v.Hostnames)
	}
	for _, v := range *c.Producers {
		ConfMaps.UTM.addHostnamesToUserTopicMapping(v.ID, CT_PRODUCER, v.Group, v.Hostnames)
	}
	for _, v := range *c.Connectors {
		// TODO: COnnector Group Names ?????
		ConfMaps.UTM.addHostnamesToUserTopicMapping(v.ID, v.getConnectorTypeValue(), "", v.Hostnames)
	}
}

/*
	This is the core function that implements addition to the USER to TOPIC Mapping.
*/
func (utm *UserTopicMapping) addHostnamesToUserTopicMapping(clientId string, cType ClientType, cGroup string, hostnames []string) {
	if val, present := (*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType, GroupID: cGroup}]; present {
		for _, v := range hostnames {
			if _, found := ksmisc.Find(val.Hostnames, v); !found {
				val.Hostnames = append(val.Hostnames, v)
			}
		}
		(*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType, GroupID: cGroup}] = val
	} else {
		(*utm)[UserTopicMappingKey{ID: clientId, ClientType: cType, GroupID: cGroup}] = UserTopicMappingValue{TopicList: []string{}, Hostnames: hostnames}
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

func (sc *ShepherdCore) addDataToClusterConfigMapping(ccm *ClusterConfigMapping) {
	for _, cluster := range *sc.Configs.ConfigRoot.Clusters {
		if cluster.IsEnabled {
			sp, sc := cluster.understandClusterTopology()
			(*ccm)[ClusterConfigMappingKey{IsEnabled: cluster.IsEnabled,
				Name:                    cluster.Name,
				ClusterSecurityProtocol: sp,
				ClusterSASLMechanism:    sc}] = ClusterConfigMappingValue{ClientID: cluster.ClientID}
		}
	}
}

/*
	This function is used in determining the security mapping of the cluster. The Kafka cluster supports
	multitude of security configs and the mapper makes it easy to determine what security method is implemented
	leveraging the properties provided. The two properties it uses is `security.protocol` and the
	`sasl.mechanism` to parse and understand the security mechanism. Still is a work in progress though.
*/
func (sc *ShepherdCluster) understandClusterTopology() (ClusterSecurityProtocol, ClusterSASLMechanism) {
	var sp ClusterSecurityProtocol
	// Figure Out the Security Protocol
	switch sc.Configs[0]["security.protocol"] {
	case "SASL_SSL":
		logger.Debug("Inside the SASL_SSL switch statement")
		sp = SP_SASL_SSL
	case "SASL_PLAINTEXT":
		logger.Debug("Inside the SASL_PLAINTEXT switch statement")
		sp = SP_SASL_PLAINTEXT
	case "SSL":
		logger.Debug("Inside the SSL switch statement")
		sp = SP_SSL
	case "":
		logger.Debug("Inside the PLAINTEXT switch statement")
		sp = SP_PLAINTEXT
	default:
		sp = SP_UNKNOWN
		logger.Fatalw("Unknown security mode supplied for Cluster Config",
			"Cluster Name", sc.Name,
			"Cluster Security Protocol Provided", sc.Configs[0]["security.protocol"])
	}

	var sm ClusterSASLMechanism = SM_UNKNOWN
	// Figure out the sasl mechanism
	switch sc.Configs[0]["sasl.mechanism"] {
	case "PLAIN":
		logger.Debug("Inside the PLAIN switch statement")
		sm = SM_PLAIN
		// temp.ClusterSASLMechanism = PLAIN
	case "SCRAM-SHA-256":
		logger.Debug("Inside SCRAM SSL switch statement")
		sm = SM_SCRAM_SHA_256
	case "SCRAM-SHA-512":
		sm = SM_SCRAM_SHA_512
	case "OAUTHBEARER":
		logger.Debug("Inside the OAUTHBEARER switch statement")
		sm = SM_OAUTHBEARER
	case "":
		logger.Debug("Inside the EMPTY switch statement")
		sm = SM_UNKNOWN
		// Check for KRB5
		//
	}

	return sp, sm
}
