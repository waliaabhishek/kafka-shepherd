package engine

import (
	"strings"

	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
)

///////////////////////////////////////////////////////////////////////////////
///////// User Topic Maping and Topic Configuration Mapping Generator /////////
///////////////////////////////////////////////////////////////////////////////

func GenerateMappings() {
	// Adhoc Topic Structure Parsing and table setup
	for _, v := range SpdCore.Definitions.DefinitionRoot.AdhocConfigs.Topics {
		for _, tName := range v.Name {
			v.Clients.addClientToUTM(tName)
		}
		// v.Clients.addHostnamesToUTM(&ConfMaps.utm)
		ConfMaps.TCM.addDataToTopicConfigMapping(&SpdCore, &v, v.Name)
	}

	for _, v := range SpdCore.Definitions.DefinitionRoot.ScopeFlow {
		iter := 0
		values := [][]string{}
		val1, cont, snd := []string{}, true, &v
		sep := SpdCore.Configs.ConfigRoot.ShepherdCoreConfig.SeperatorToken
		for cont {
			currTopics := append(snd.Topics.Name, "*")
			currClients := snd.Clients
			currFilters := snd.Topics.IgnoreScope
			val1, cont, snd = snd.getTokensForThisLevel(iter, &SpdCore.Blueprints.Blueprint)
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
				// Ignore topic combinations with the filterscope at that level from being added to the utm list
				if !ksmisc.ExistsInString(temp, currFilters, ksmisc.RemoveValuesFromSlice(currTopics, "*"), sep) {
					// fmt.Println("Inside the filter for *. Topic Name:", temp)
					currClients.addClientToUTM(temp)
					if !strings.HasSuffix(temp, ".*") {
						ConfMaps.TCM.addDataToTopicConfigMapping(&SpdCore, &v.Topics, []string{temp})
					}
				}
			}
		}
	}
	SpdCore.addDataToClusterConfigMapping(&ConfMaps.CCM)
}

func (sd ScopeDefinition) getTokensForThisLevel(level int, b *BlueprintRoot) ([]string, bool, *ScopeDefinition) {
	ret := []string{}
	temp := []string{}
	if sd.IncludeInTopicName {
		for _, v := range b.CustomEnums {
			temp = append(temp, v.Name)
		}
		if len(sd.CustomEnumRef) != 0 {
			if loc, ok := ksmisc.Find(&temp, sd.CustomEnumRef); ok {
				for _, v := range b.CustomEnums[loc].Values {
					if v != "" {
						ret = append(ret, v)
					}
				}
			}
		}
		if sd.Values != nil {
			for _, v := range sd.Values {
				if v != "" {
					ret = append(ret, v)
				}
			}
		}
	}
	return ret, sd.Child != nil, sd.Child
}

func (c ClientDefinition) addClientToUTM(topic string) {
	for _, v := range c.Consumers {
		ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_CONSUMER, v.Group, topic, v.Hostnames)
		if v.Group != "" {
			ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_CONSUMER_GROUP, v.Group, topic, v.Hostnames)
		}
	}
	for _, v := range c.Producers {
		ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_PRODUCER, v.Group, topic, v.Hostnames)
		if v.TransactionalID {
			ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_TRANSACTIONAL_PRODUCER, v.Group, topic, v.Hostnames)
		}
		if v.EnableIdempotence {
			ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_PRODUCER_IDEMPOTENCE, v.Group, topic, v.Hostnames)
		}
	}
	for _, v := range c.Connectors {
		ConfMaps.utm.addToUserTopicMapping(v.Principal, v.getTypeValue(), v.ClusterNameRef, topic, v.Hostnames)
		// v.addClientToUTM(utm, topic)
	}
	for _, v := range c.Streams {
		// if v.getTypeValue() == ShepherdClientType_STREAM_READ {
		// 	ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_CONSUMER, v.Group, topic, v.Hostnames)
		// } else {
		// 	ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_PRODUCER, v.Group, topic, v.Hostnames)
		// }
		// ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_TRANSACTIONAL_PRODUCER, v.Group, topic, v.Hostnames)
		// ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_PRODUCER_IDEMPOTENCE, v.Group, topic, v.Hostnames)
		ConfMaps.utm.addToUserTopicMapping(v.Principal, v.getTypeValue(), v.Group, topic, v.Hostnames)
	}
	for _, v := range c.KSQL {
		ConfMaps.utm.addToUserTopicMapping(v.Principal, v.getTypeValue(), v.ClusterNameRef, topic, v.Hostnames)
		ConfMaps.utm.addToUserTopicMapping(v.Principal, ShepherdClientType_KSQL, v.ClusterNameRef, topic, v.Hostnames)
	}
}

func (c ConnectorDefinition) getTypeValue() ShepherdClientType {
	if strings.TrimSpace(strings.ToLower(c.Type)) == "source" {
		return ShepherdClientType_SOURCE_CONNECTOR
	} else {
		return ShepherdClientType_SINK_CONNECTOR
	}
}

func (c StreamDefinition) getTypeValue() ShepherdClientType {
	if strings.TrimSpace(strings.ToLower(c.Type)) == "read" {
		return ShepherdClientType_STREAM_READ
	} else {
		return ShepherdClientType_STREAM_WRITE
	}
}

func (c KSQLDefinition) getTypeValue() ShepherdClientType {
	if strings.TrimSpace(strings.ToLower(c.Type)) == "read" {
		return ShepherdClientType_CONSUMER
	} else {
		return ShepherdClientType_PRODUCER
	}
}

func (utm *UserTopicMapping) addToUserTopicMapping(clientId string, cType ShepherdClientType, cGroup string, topicName string, hostNames []string) {
	utm.addTopicToUserTopicMapping(clientId, cType, cGroup, topicName)
	utm.addHostnamesToUserTopicMapping(clientId, cType, cGroup, hostNames)
}

/*
	This is the core function that implements addition to the USER to TOPIC Mapping.
*/
func (utm *UserTopicMapping) addTopicToUserTopicMapping(clientId string, cType ShepherdClientType, cGroup string, topicName string) {
	if val, present := (*utm)[UserTopicMappingKey{Principal: clientId, ClientType: cType, GroupID: cGroup}]; present {
		if _, ok := ksmisc.Find(&val.TopicList, topicName); !ok {
			val.TopicList = append(val.TopicList, topicName)
		}
		(*utm)[UserTopicMappingKey{Principal: clientId, ClientType: cType, GroupID: cGroup}] = val
	} else {
		(*utm)[UserTopicMappingKey{Principal: clientId, ClientType: cType, GroupID: cGroup}] = UserTopicMappingValue{TopicList: []string{topicName}, Hostnames: []string{}}
	}
}

/*
	This is the core function that implements addition to the USER to TOPIC Mapping.
*/
func (utm *UserTopicMapping) addHostnamesToUserTopicMapping(clientId string, cType ShepherdClientType, cGroup string, hostnames []string) {
	if val, present := (*utm)[UserTopicMappingKey{Principal: clientId, ClientType: cType, GroupID: cGroup}]; present {
		for _, v := range hostnames {
			if _, found := ksmisc.Find(&val.Hostnames, v); !found {
				val.Hostnames = append(val.Hostnames, v)
			}
		}
		(*utm)[UserTopicMappingKey{Principal: clientId, ClientType: cType, GroupID: cGroup}] = val
	} else {
		(*utm)[UserTopicMappingKey{Principal: clientId, ClientType: cType, GroupID: cGroup}] = UserTopicMappingValue{TopicList: []string{}, Hostnames: hostnames}
	}
}

func (tcm *TopicConfigMapping) addDataToTopicConfigMapping(sc *ShepherdCore, td *TopicDefinition, topicName []string) {
	props := make(NVPairs)
	// Get the default values from the blueprints and merge it to the currently applied values.
	props.merge(sc.Blueprints.Blueprint.Policy.TopicPolicy.Defaults)
	// If Blueprint reference exists in the topic Config, fetch the NV Pairs for that Blueprint and add those props here.
	if td.TopicBlueprintEnumRef != "" {
		props.overrideMergeMaps([]NVPairs{sc.getBlueprintProps((*td).TopicBlueprintEnumRef)},
			sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Whitelist,
			sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Blacklist)
	}
	// Merge and overwrite values configured as overrides in the scopeflow or the adhoc topic configs
	props.overrideMergeMaps(td.ConfigOverrides,
		sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Whitelist,
		sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Blacklist)
	for _, topic := range topicName {
		(*tcm)[topic] = props
	}
}

func (in *NVPairs) overrideMergeMaps(temp []NVPairs, whitelist []string, blacklist []string) {
	for _, v1 := range temp {
		for k, v := range v1 {
			_, wPresent := ksmisc.Find(&whitelist, k)
			_, bPresent := ksmisc.Find(&blacklist, k)
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
}

func (sc *ShepherdCore) getBlueprintProps(blueprintName string) NVPairs {
	if blueprintMap == nil {
		blueprintMap = make(map[string]NVPairs)
		for _, v := range sc.Blueprints.Blueprint.Topic.TopicConfigs {
			temp := NVPairs{}
			// Get the default values configured in Topic Blueprints Defaults as those act as our baseline
			temp.merge(sc.Blueprints.Blueprint.Policy.TopicPolicy.Defaults)
			// Get the Base Config Override Values from Topic Blueprint Configuration
			temp.overrideMergeMaps(v.Overrides,
				sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Whitelist,
				sc.Blueprints.Blueprint.Policy.TopicPolicy.Overrides.Blacklist)
			blueprintMap[strings.ToLower(strings.TrimSpace(v.Name))] = temp
		}
		// fmt.Println("Blueprint Topic Plan Map:", blueprintMap)
	}
	return blueprintMap[strings.ToLower(strings.TrimSpace(blueprintName))]
}

func (sc *ShepherdCore) addDataToClusterConfigMapping(ccm *ClusterConfigMapping) {
	for _, cluster := range sc.Configs.ConfigRoot.Clusters {
		if cluster.IsEnabled {
			sp, sc, am := cluster.understandClusterTopology()
			value := ClusterConfigMappingValue{
				ClientID:                cluster.ClientID,
				Configs:                 cluster.Configs[0],
				ClusterDetails:          cluster.ClusterDetails[0],
				ClusterSecurityProtocol: sp,
				ClusterSASLMechanism:    sc,
				IsActive:                false,
				IsACLManagementEnabled:  am,
			}
			(*ccm)[ClusterConfigMappingKey{IsEnabled: cluster.IsEnabled, Name: cluster.Name}] = value
		}
	}
}

/*
	This function is used in determining the security mapping of the cluster. The Kafka cluster supports
	multitude of security configs and the mapper makes it easy to determine what security method is implemented
	leveraging the properties provided. The two properties it uses is `security.protocol` and the
	`sasl.mechanism` to parse and understand the security mechanism. Still is a work in progress though.
*/
func (sc *ShepherdCluster) understandClusterTopology() (ClusterSecurityProtocol, ClusterSASLMechanism, bool) {
	var sp ClusterSecurityProtocol
	var am bool = true
	// Figure Out the Security Protocol
	switch p := strings.ToUpper(sc.Configs[0]["security.protocol"]); p {
	case "SASL_SSL":
		logger.Debugf("Inside the %v switch statement", p)
		sp = ClusterSecurityProtocol_SASL_SSL
	case "SASL_PLAINTEXT":
		logger.Debugf("Inside the %v switch statement", p)
		sp = ClusterSecurityProtocol_SASL_PLAINTEXT
	case "SSL":
		logger.Debugf("Inside the %v switch statement", p)
		sp = ClusterSecurityProtocol_SSL
	case "", "PLAINTEXT":
		logger.Debug("Inside the PLAINTEXT switch statement")
		logger.Infow("Turning off ACL management as the cluster type is PLAINTEXT",
			"Cluster Name", sc.Name,
			"Cluster Security Protocol", p)
		sp = ClusterSecurityProtocol_PLAINTEXT
		am = false
	default:
		sp = ClusterSecurityProtocol_UNKNOWN
		logger.Fatalw("Unknown security mode supplied for Cluster Config",
			"Cluster Name", sc.Name,
			"Cluster Security Protocol Provided", p)
	}

	var sm ClusterSASLMechanism = ClusterSASLMechanism_UNKNOWN
	// Figure out the sasl mechanism
	switch m := strings.ToUpper(sc.Configs[0]["sasl.mechanism"]); m {
	case "PLAIN":
		logger.Debug("Inside the %v switch statement", m)
		sm = ClusterSASLMechanism_PLAIN
		// temp.ClusterSASLMechanism = PLAIN
	case "SCRAM-SHA-256":
		logger.Debug("Inside the %v switch statement", m)
		sm = ClusterSASLMechanism_SCRAM_SHA_256
	case "SCRAM-SHA-512":
		logger.Debug("Inside the %v switch statement", m)
		sm = ClusterSASLMechanism_SCRAM_SHA_512
	case "OAUTHBEARER":
		logger.Debug("Inside the %v switch statement", m)
		sm = ClusterSASLMechanism_OAUTHBEARER
	case "":
		logger.Debug("Inside the EMPTY switch statement")
		sm = ClusterSASLMechanism_UNKNOWN
		// Check for KRB5
		//
	}

	return sp, sm, am
}
