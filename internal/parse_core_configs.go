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
	streamlineParsedShepherdConfig(scf)
	resolveEnvOverrides(scf)
	return scf
}

func GetShepherdConfig() *ShepherdConfig {
	return &scf
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
