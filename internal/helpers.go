package core

import (
	"fmt"
	"os"
	"strings"
)

// This function can be used to check if a specific string value begins with the ENV Var prefix or not.
// If it begins with the ENVVAR_PREFIX constant, then it will consider everything after the ENVVAR_PREFIX
// as the ENV Variable name and try to fetch it or return the default Value
func envVarCheckNReplace(s string, def string) string {
	if strings.HasPrefix(s, ENVVAR_PREFIX) {
		return getEnvVarsWithDefaults(strings.Replace(s, ENVVAR_PREFIX, "", 1), def)
	}
	return s
}

// If the ENV Variable is empty but the default value exists, then the default value would be returned.
// If the ENV Variable is not present and the default value is also empty, it will fail with a fatal exception.
// If Env Variable is found and has a value, the string value will be returned.
func getEnvVarsWithDefaults(envVarName string, defaultValue string) string {
	envVarValue, present := os.LookupEnv(envVarName)
	if !present && defaultValue == "" {
		logger.Fatal(envVarName, " is not available in ENV Variables. Cannot proceed without it.")
	}

	if envVarValue == "" && defaultValue == "" {
		logger.Fatal(envVarName, "ENV Variable is empty. Please ensure it is instantiated properly.")
	} else if envVarValue == "" && defaultValue != "" {
		logger.Debugw("ENV Variable is empty. Using Default value\n",
			"ENV Variable Name", envVarName,
			"ENV Variable Value", envVarValue,
			"Default Value", defaultValue)
		return defaultValue
	}

	return envVarValue
}

/* The standard YAML parsers creates a lot of NVPairs and sets up arrays for all or them.
This function tries to consolidate all the NVPairs into a single []NVPairs structure.
*/
func streamlineNVPairs(in []NVPairs) []NVPairs {
	cMap := make(NVPairs)
	cMap.mergeMaps(in)
	return []NVPairs{cMap}
}

func (in *NVPairs) mergeMaps(temp []NVPairs) (out *NVPairs) {
	for _, v1 := range temp {
		for k, v := range v1 {
			(*in)[k] = v
		}
	}
	return in
}

func (sc DefinitionRoot) PrettyPrintScope() {
	for _, v1 := range *sc.ScopeFlow {
		v1.prettyPrintSND(0)
	}
}

func (snd *ScopeDefinition) prettyPrintSND(tabCounter int) {
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
