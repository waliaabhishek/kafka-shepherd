package core

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func parseShepherdConfig(shp *ShepherdConfig, configFilePath string) *ShepherdConfig {

	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatal("Cannot read the filepath provided in SHEPHERD_CONFIG_FILELOCATION variable. Please Correct. Error Received", err)
	}

	if err := yaml.Unmarshal(temp, shp); err != nil {
		logger.Fatal("Error Unmarshaling Shepherd Configs File", err)
	}
	shp.validateShepherdConfig()
	shp.readValuesFromENV()
	return shp
}

func (scf *ShepherdConfig) validateShepherdConfig() {
	count := 0
	for _, cluster := range *scf.ConfigRoot.Clusters {
		if cluster.IsEnabled {
			count += 1
		}
	}

	switch runMode {
	case RM_SINGLE_CLUSTER:
		if count != 1 {
			logger.Fatalw("Unique cluster not enabled in the config file for selected run mode. Either select the correct runMode or enable only ONE cluster via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case RM_MULTI_CLUSTER:
		if count < 1 {
			logger.Fatalw("Cannot have less than one cluster(s) enabled in the config file for selected run mode. Either select the correct runMode or enable more clusters via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case RM_MIGRATION:
		if count < 2 {
			logger.Fatalw("Cannot have less than two cluster(s) enabled in the config file for selected run mode. Either select the correct runMode or enable more clusters via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case RM_CREATE_CONFIGS:
		if count != 1 {
			logger.Fatalw("Unique cluster not enabled in the config file for selected run mode. Either select the correct runMode or enable only ONE cluster via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	}
}
