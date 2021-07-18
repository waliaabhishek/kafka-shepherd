package internal

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	ksmisc "shepherd/misc"

	yaml "gopkg.in/yaml.v2"
)

func (def RootStruct) GenerateMappings() (*UserTopicMapping, *TopicConfigMapping) {
	// Adhoc Topic Structure Parsing and table setup
	for _, v := range def.Definition.Definitions.AdhocTopicsDefinitions.TopicDefs {
		for _, tName := range v.Name {
			v.Clients.addClientToUTM(tName)
		}
		tcm.addDataToTopicConfigMapping(&v, v.Name)
	}

	for _, v := range def.Definition.Definitions.ScopeFlow {
		iter := 0
		values := [][]string{}
		val1, cont, snd := []string{}, true, &v.Scope
		for cont {
			currTopics := append(snd.Topics.Name, "*")
			currClients := snd.Clients
			currFilters := snd.Topics.FilterScope
			val1, cont, snd = snd.getTokensForThisLevel(iter, def.Blueprint.Blueprints)
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
				// TODO: Update seperator with the values from configuration
				sep := "."
				// Get current Topic Name for the current scope
				temp := strings.Join(v2, sep)
				// Ignore topic combinations with the filterscope at that level from being added to the UTM list
				if !ksmisc.ExistsInString(temp, currFilters, ksmisc.RemoveValuesFromSlice(currTopics, "*"), sep) {
					// fmt.Println("Inside the filter for *. Topic Name:", temp)
					currClients.addClientToUTM(temp)
					if !strings.HasSuffix(temp, ".*") {
						tcm.addDataToTopicConfigMapping(&v.Scope.Topics, []string{temp})
					}
				}
			}
		}
	}
	return &utm, &tcm
}

func (snd ScopeNodeDefinition) getTokensForThisLevel(level int, b Blueprint) ([]string, bool, *ScopeNodeDefinition) {
	ret := []string{}
	temp := []string{}
	if snd.IncludeInTopicName {
		for _, v := range b.Custom {
			temp = append(temp, v.Name)
		}
		if len(snd.CustomEnumRef) != 0 {
			if loc, ok := ksmisc.Find(temp, snd.CustomEnumRef); ok {
				for _, v := range b.Custom[loc].Values {
					if v != "" {
						ret = append(ret, v)
					}
				}
			}
		}
		if snd.Values != nil {
			for _, v := range snd.Values {
				if v != "" {
					ret = append(ret, v)
				}
			}
		}
	}
	return ret, snd.Child != nil, snd.Child
}

func parseConfigurations(bf string, df string) RootStruct {

	var bp Blueprints
	temp, err := ioutil.ReadFile(bf)
	if err != nil {
		fmt.Println("Error Reading File", err)
		os.Exit(1)
	}
	if err := yaml.Unmarshal(temp, &bp); err != nil {
		fmt.Println("Error Unmarshaling Blueprints File", err)
	}

	var dp Definitions
	temp, err = ioutil.ReadFile(df)
	if err != nil {
		fmt.Println("Error Reading File", err)
		os.Exit(1)
	}
	if err := yaml.Unmarshal(temp, &dp); err != nil {
		fmt.Println("Error Unmarshaling Definitions File", err)
	}

	return RootStruct{
		Blueprint:  bp,
		Definition: dp,
	}
}

type client interface {
	addClientToUTM(t string)
}

func addToUTMList(c client, topicName string) {
	c.addClientToUTM(topicName)
}

func (c ClientDefinition) addClientToUTM(topic string) {
	for _, v := range c.Producers {
		addToUTMList(v, topic)
	}
	for _, v := range c.Consumers {
		addToUTMList(v, topic)
	}
	for _, v := range c.Connectors {
		addToUTMList(v, topic)
	}
}

func (c ProducerDefinition) addClientToUTM(topicName string) {
	utm.addDataToUserTopicMapping(c.ID, PRODUCER, topicName)
}

func (c ConsumerDefinition) addClientToUTM(topicName string) {
	utm.addDataToUserTopicMapping(c.ID, CONSUMER, topicName)
}

func (c ConnectorDefinition) addClientToUTM(topicName string) {
	utm.addDataToUserTopicMapping(c.ID, c.getConnectorTypeValue(), topicName)
}

func (c ConnectorDefinition) getConnectorTypeValue() ClientType {
	if strings.TrimSpace(strings.ToLower(c.Type)) == "source" {
		return SOURCE_CONNECTOR
	} else {
		return SINK_CONNECTOR
	}
}

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

func (tcm *TopicConfigMapping) addDataToTopicConfigMapping(td *TopicDefinition, topicName []string) {
	// fmt.Println("TopicDefinition Value:", td)
	props := make(NVPairs)
	// Get the default values from the blueprints and merge it to the currently applied values.
	props.mergeMaps(rs.Blueprint.Blueprints.Policy.TopicPolicy.Defaults)
	// fmt.Println("After Default Policy Merge:", props)
	// If Blueprint reference exists in the topic Config, fetch the NV Pairs for that Blueprint and add those props here.
	if td.TopicBlueprintEnumRef != "" {
		props.overrideMergeMaps([]NVPairs{getBlueprintProps((*td).TopicBlueprintEnumRef)}, rs.Blueprint.Blueprints.Policy.TopicPolicy.Overrides.Whitelist, rs.Blueprint.Blueprints.Policy.TopicPolicy.Overrides.Blacklist)
		// fmt.Println("After BlueprintEnum Merge:", props)
	}
	// Merge and overwrite values configured as overrides in the scopeflow or the adhoc topic configs
	props.overrideMergeMaps(td.ConfigOverrides, rs.Blueprint.Blueprints.Policy.TopicPolicy.Overrides.Whitelist, rs.Blueprint.Blueprints.Policy.TopicPolicy.Overrides.Blacklist)
	// fmt.Println("After Config Overrides Merge:", props)
	for _, topic := range topicName {
		(*tcm)[topic] = props
	}
}

func (in *NVPairs) mergeMaps(temp []NVPairs) (out *NVPairs) {
	for _, v1 := range temp {
		for k, v := range v1 {
			(*in)[k] = v
		}
	}
	return in
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

func getBlueprintProps(blueprintName string) NVPairs {
	if blueprintMap == nil {
		blueprintMap = make(map[string]NVPairs)
		for _, v := range rs.Blueprint.Blueprints.Topic.TopicConfigs {
			temp := NVPairs{}
			// Get the default values configured in Topic Blueprints Defaults as those act as our baseline
			temp.mergeMaps(rs.Blueprint.Blueprints.Policy.TopicPolicy.Defaults)
			// Get the Base Config Override Values from Topic Blueprint Configuration
			temp.overrideMergeMaps(v.Overrides, rs.Blueprint.Blueprints.Policy.TopicPolicy.Overrides.Whitelist, rs.Blueprint.Blueprints.Policy.TopicPolicy.Overrides.Blacklist)
			blueprintMap[strings.ToLower(strings.TrimSpace(v.Name))] = temp
		}
		// fmt.Println("Blueprint Topic Plan Map:", blueprintMap)
	}
	return blueprintMap[strings.ToLower(strings.TrimSpace(blueprintName))]
}
