package core

import (
	"flag"
	ksmisc "shepherd/misc"
	"strings"

	"go.uber.org/zap"
)

// Externally available variables.
var (
	ConfMaps ConfigurationMaps = ConfigurationMaps{
		tcm: &TopicConfigMapping{},
		utm: &UserTopicMapping{},
		ccm: &ClusterConfigMapping{},
	}
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
)

const (
	ENVVAR_PREFIX string = "env::"
)

func init() {
	ResolveFlags()
	// Initialize Logger
	logger = ksmisc.GetLogger(enableDebug, enableConsoleLogs)

	// Parse Shepherd Internal Configurations from the YAML file.
	parseShepherdConfig(spdCore.Configs, getEnvVarsWithDefaults("SHEPHERD_CONFIG_FILE_LOCATION", configFile))
	logger.Debug("Shepherd Config File parse Result: ", spdCore.Configs)

	// Parse Shepherd Blueprints from the YAML file.
	parseShepherBlueprints(spdCore.Blueprints, getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", blueprintsFile))
	logger.Debug("Shepherd Blueprints parse Result: ", spdCore.Blueprints)

	// Parse Shepherd Internal Configurations from the YAML file.
	parseShepherDefinitions(spdCore.Definitions, getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", definitionsFile))
	logger.Debug("Shepherd Definitions parse Result: ", spdCore.Definitions)

	// Understand the Blueprints & Definitions file and setup the External facing representation of the core files.
	GenerateMappings(&spdCore, GetConfigMaps().utm, GetConfigMaps().tcm)
	logger.Debug("Config File parse Result: ", ConfMaps)
}

func ResolveFlags() {
	flag.BoolVar(enableDebug, "debug", false, "Turns on Debug mode log")
	flag.BoolVar(enableConsoleLogs, "consoleLogs", true, "Turns off unstructured mode logging features and use Structured logging instead.")
	flag.StringVar(&configFile, "configPath", "./configs/shepherd.yaml", "Absolute file Path for Core Configuration file. Please note that this might still be overwritten by the SHEPHERD_CONFIG_FILE_LOCATION for additional flexibility.")
	flag.StringVar(&blueprintsFile, "blueprintsPath", "./configs/blueprints.yaml", "Absolute file Path for Shepherd Blueprints file. Please note that this might still be overwritten by the SHEPHERD_BLUEPRINTS_FILE_LOCATION for additional flexibility.")
	flag.StringVar(&definitionsFile, "definitionsPath", "./configs/definitions_dev.yaml", "Absolute file Path for Shepherd Definitions file. Please note that this might still be overwritten by the SHEPHERD_DEFINITIONS_FILE_LOCATION for additional flexibility.")
	runMode = assertRunMode(flag.String("runMode", "SINGLE_CLUSTER", "Changes the mode in which the tool is operating. Options are SINGLE_CLUSTER, MULTI_CLUSTER, MIGRATION, CREATE_CONFIGS_FROM_EXISTING_CLUSTER"))
	flag.Parse()
}

func GetConfigMaps() (sc *ConfigurationMaps) {
	return sc
}

func assertRunMode(mode *string) RunMode {
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
