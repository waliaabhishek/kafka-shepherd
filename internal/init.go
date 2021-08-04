package core

import (
	ksmisc "shepherd/misc"
	"strings"

	mapset "github.com/deckarep/golang-set"
	"go.uber.org/zap"
)

// Externally available variables.
var (
	ConfMaps ConfigurationMaps = ConfigurationMaps{
		TCM: &TopicConfigMapping{},
		UTM: &UserTopicMapping{},
		CCM: &ClusterConfigMapping{},
	}
	SpdCore ShepherdCore
)

// Internal variables for function
var (
	enableDebug       *bool
	enableConsoleLogs *bool
	configFile        string
	blueprintsFile    string
	definitionsFile   string
	runMode           RunMode
	spdCore           ShepherdCore
	blueprintMap      map[string]NVPairs
	logger            *zap.SugaredLogger
	topicsInConfig    mapset.Set
	aclList           ACLMapping
)

const (
	ENVVAR_PREFIX string = "env::"
)

func InitializeShepherdCore(enableDebug bool, enableConsoleLogs bool, shepherdConfigFilePath string,
	shepherdBlueprintsFilePath string, shepherdDefinitionsFilePath string, runString string) {
	// Initialize Logger
	logger = ksmisc.GetLogger(&enableDebug, &enableConsoleLogs)
	runMode = assertRunMode(&runString)

	// Parse Shepherd Internal Configurations from the YAML file.
	parseShepherdConfig(spdCore.Configs, getEnvVarsWithDefaults("SHEPHERD_CONFIG_FILE_LOCATION", shepherdConfigFilePath))
	logger.Debug("Shepherd Config File parse Result: ", spdCore.Configs)

	// Parse Shepherd Blueprints from the YAML file.
	parseShepherBlueprints(spdCore.Blueprints, getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", shepherdBlueprintsFilePath))
	logger.Debug("Shepherd Blueprints parse Result: ", spdCore.Blueprints)

	// Parse Shepherd Internal Configurations from the YAML file.
	parseShepherDefinitions(spdCore.Definitions, getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", shepherdDefinitionsFilePath))
	logger.Debug("Shepherd Definitions parse Result: ", spdCore.Definitions)

	// Understand the Blueprints & Definitions file and setup the External facing representation of the core files.
	GenerateMappings(&spdCore, GetConfigMaps().UTM, GetConfigMaps().TCM)
	logger.Debug("Config File parse Result: ", ConfMaps)

	topicsInConfig = ConfMaps.UTM.getTopicListFromUTMList()
	SpdCore = spdCore

	aclList = ConfMaps.UTM.GenerateShepherdClientTypeMappings()
}

// func ResolveFlags() {
// 	flag.BoolVar(enableDebug, "debug", false, "Turns on Debug mode log")
// 	flag.BoolVar(enableConsoleLogs, "consoleLogs", true, "Turns off unstructured mode logging features and use Structured logging instead.")
// 	flag.StringVar(&configFile, "configPath", "./configs/shepherd.yaml", "Absolute file Path for Core Configuration file. Please note that this might still be overwritten by the SHEPHERD_CONFIG_FILE_LOCATION for additional flexibility.")
// 	flag.StringVar(&blueprintsFile, "blueprintsPath", "./configs/blueprints.yaml", "Absolute file Path for Shepherd Blueprints file. Please note that this might still be overwritten by the SHEPHERD_BLUEPRINTS_FILE_LOCATION for additional flexibility.")
// 	flag.StringVar(&definitionsFile, "definitionsPath", "./configs/definitions_dev.yaml", "Absolute file Path for Shepherd Definitions file. Please note that this might still be overwritten by the SHEPHERD_DEFINITIONS_FILE_LOCATION for additional flexibility.")
// 	runMode = assertRunMode(flag.String("runMode", "SINGLE_CLUSTER", "Changes the mode in which the tool is operating. Options are SINGLE_CLUSTER, MULTI_CLUSTER, MIGRATION, CREATE_CONFIGS_FROM_EXISTING_CLUSTER"))
// 	flag.Parse()
// }

func GetConfigMaps() (sc *ConfigurationMaps) {
	return sc
}

func GetLogger() (lg *zap.SugaredLogger) {
	return logger
}

func GetConfigTopicsAsMapSet() mapset.Set {
	return topicsInConfig
}

func assertRunMode(mode *string) RunMode {
	switch strings.ToUpper(strings.TrimSpace(*mode)) {
	case RunMode_SINGLE_CLUSTER.String():
		return RunMode_SINGLE_CLUSTER
	case RunMode_MULTI_CLUSTER.String():
		logger.Fatal(RunMode_MULTI_CLUSTER.String(), " mode has not been implemented yet, but should be available soon.")
	case RunMode_MIGRATION.String():
		logger.Fatal(RunMode_MIGRATION.String(), " mode has not been implemented yet, but should be available soon.")
	case RunMode_CREATE_CONFIGS.String():
		logger.Fatal(RunMode_CREATE_CONFIGS.String(), " mode has not been implemented yet, but should be available soon.")
	default:
		logger.Warnf("Selected runMode '%s' is incorrect. Reverting to %s mode to continue with the process.", *mode, RunMode_SINGLE_CLUSTER.String())
	}
	return RunMode_SINGLE_CLUSTER
}
