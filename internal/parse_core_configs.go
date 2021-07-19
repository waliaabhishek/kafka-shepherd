package internal

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func parseShepherdConfig(scf *ShepherdConfig, configFilePath string) *ShepherdConfig {

	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatal("Cannot read the filepath provided in SHEPHERD_CONFIG_FILELOCATION variable. Please Correct. Error Received", err)
	}

	if err := yaml.Unmarshal(temp, scf); err != nil {
		logger.Fatal("Error Unmarshaling Shepherd Configs File", err)
	}
	validateShepherdConfig(scf)
	streamlineParsedShepherdConfig(scf)
	resolveEnvOverrides(scf)
	return scf
}

func GetShepherdConfig() *ShepherdConfig {
	return &scf
}

func validateShepherdConfig(scf *ShepherdConfig) {
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

func streamlineParsedShepherdConfig(scf *ShepherdConfig) {
	for idx, cluster := range scf.Config.Clusters {
		cMap, eMap := make(NVPairs), make(NVPairs)
		cMap.mergeMaps(cluster.Configs)
		eMap.mergeMaps(cluster.EnvironmentOverrides)
		scf.Config.Clusters[idx].Configs = []NVPairs{cMap}
		scf.Config.Clusters[idx].EnvironmentOverrides = []NVPairs{eMap}
	}
}

func resolveEnvOverrides(scf *ShepherdConfig) {
	for _, cluster := range scf.Config.Clusters {
		for k, v := range cluster.EnvironmentOverrides[0] {
			getEnvVarsWithDefaults(v, cluster.Configs[0][k])
		}
	}
}
