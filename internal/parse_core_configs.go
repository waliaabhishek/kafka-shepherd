package internal

import (
	"io/ioutil"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

func GetShepherdConfig() *ShepherdConfig {
	return &scf
}

func parseShepherdConfig(scf *ShepherdConfig, configFilePath string) *ShepherdConfig {

	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatal("Cannot read the filepath provided in SHEPHERD_CONFIG_FILELOCATION variable. Please Correct. Error Received", err)
	}

	if err := yaml.Unmarshal(temp, scf); err != nil {
		logger.Fatal("Error Unmarshaling Shepherd Configs File", err)
	}
	scf.validateShepherdConfig()
	scf.streamlineParsedShepherdConfig()
	scf.gatherENVVarValues()
	return scf
}

func (scf *ShepherdConfig) validateShepherdConfig() {
	count := 0
	for _, cluster := range scf.Config.Clusters {
		if cluster.IsEnabled {
			count += 1
		}
	}

	switch runMode {
	case SINGLE_CLUSTER:
		if count != 1 {
			logger.Fatalw("Unique cluster not enabled in the config file for selected run mode. Either select the correct runMode or enable only ONE cluster via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case MULTI_CLUSTER:
		if count < 1 {
			logger.Fatalw("Cannot have less than one cluster(s) enabled in the config file for selected run mode. Either select the correct runMode or enable more clusters via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case MIGRATION:
		if count < 2 {
			logger.Fatalw("Cannot have less than two cluster(s) enabled in the config file for selected run mode. Either select the correct runMode or enable more clusters via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case CREATE_CONFIGS_FROM_EXISTING_CLUSTER:
		if count != 1 {
			logger.Fatalw("Unique cluster not enabled in the config file for selected run mode. Either select the correct runMode or enable only ONE cluster via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	}
}

func (scf *ShepherdConfig) streamlineParsedShepherdConfig() {
	for idx, cluster := range scf.Config.Clusters {
		cMap := make(NVPairs)
		cMap.mergeMaps(cluster.Configs)
		scf.Config.Clusters[idx].Configs = []NVPairs{cMap}
	}
}

func addDataToClusterConfigMapping(ccm *ClusterConfigMapping) {
	for _, cluster := range scf.Config.Clusters {
		if cluster.IsEnabled == true {
			understandClusterTopology(&cluster)
			(*ccm)[ClusterConfigMappingKey{IsEnabled: cluster.IsEnabled, Name: cluster.Name, BootstrapServer: cluster.BootstrapServer}] = ClusterConfigMappingValue{ClientID: cluster.ClientID}
		}
	}
}

func validateRunMode(mode *string) RunMode {
	switch strings.ToUpper(strings.TrimSpace(*mode)) {
	case SINGLE_CLUSTER.String():
		return SINGLE_CLUSTER
	case MULTI_CLUSTER.String():
		logger.Fatal(MULTI_CLUSTER.String(), " mode has not been implemented yet, but should be available soon.")
	case MIGRATION.String():
		logger.Fatal(MIGRATION.String(), " mode has not been implemented yet, but should be available soon.")
	case CREATE_CONFIGS_FROM_EXISTING_CLUSTER.String():
		logger.Fatal(CREATE_CONFIGS_FROM_EXISTING_CLUSTER.String(), " mode has not been implemented yet, but should be available soon.")
	default:
		logger.Warnf("Selected runMode '%s' is incorrect. Reverting to %s mode to continue with the process.", *mode, SINGLE_CLUSTER.String())
	}
	return SINGLE_CLUSTER
}
