package engine

import (
	"flag"
	"strings"

	ksmisc "github.com/waliaabhishek/kafka-shepherd/new/misc"

	mapset "github.com/deckarep/golang-set"
	"go.uber.org/zap"
)

// Externally available variables.
var (
	ConfMaps ConfigurationMaps = ConfigurationMaps{
		TCM: TopicConfigMapping{},
		UTM: UserTopicMapping{},
		CCM: ClusterConfigMapping{},
	}
	SpdCore ShepherdCore = ShepherdCore{
		Configs:     ShepherdConfig{},
		Blueprints:  ShepherdBlueprint{},
		Definitions: ShepherdDefinition{},
	}
	ShepherdACLList *ACLMapping
	DryRun          bool = false
)

// Internal variables for function
var (
	enableDebug          bool = false
	enableStructuredLogs bool = false
	configFile           string
	blueprintsFile       string
	definitionsFile      string
	runMode              RunMode
	blueprintMap         map[string]NVPairs
	logger               *zap.SugaredLogger
	topicsInConfig       mapset.Set
	shepherdACLList      ACLMapping
)

const (
	ENVVAR_PREFIX string = "env::"
)

// func InitializeShepherdCore(enableDebug bool, enableConsoleLogs bool, shepherdConfigFilePath string,
// 	shepherdBlueprintsFilePath string, shepherdDefinitionsFilePath string, runString string) {
func init() {
	// Initialize Logger
	ResolveFlags()
	logger = ksmisc.GetLogger(enableDebug, enableStructuredLogs)
	// runMode = assertRunMode(runString)

	// Parse Shepherd Internal Configurations from the YAML file.
	SpdCore.Configs.parseShepherdConfig(getEnvVarsWithDefaults("SHEPHERD_CONFIG_FILE_LOCATION", configFile))
	logger.Debug("Shepherd Config File parse Result: ", SpdCore.Configs)

	// Parse Shepherd Blueprints from the YAML file.
	SpdCore.Blueprints.parseShepherBlueprints(getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", blueprintsFile))
	logger.Debug("Shepherd Blueprints parse Result: ", SpdCore.Blueprints)

	// Parse Shepherd Internal Configurations from the YAML file.
	SpdCore.Definitions.parseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", definitionsFile))
	logger.Debug("Shepherd Definitions parse Result: ", SpdCore.Definitions)

	// Understand the Blueprints & Definitions file and setup the External facing representation of the core files.
	GenerateMappings()
	logger.Debug("Config File parse Result: ", ConfMaps)

	topicsInConfig = ConfMaps.UTM.getTopicList()

	shepherdACLList = ConfMaps.UTM.getShepherdACLList()
	ShepherdACLList = &shepherdACLList
}

func ResolveFlags() {
	flag.BoolVar(&DryRun, "dryRun", false, "Does not execute anything but lists what will be executed.")
	flag.BoolVar(&enableDebug, "debug", false, "Turns on Debug mode log")
	flag.BoolVar(&enableStructuredLogs, "enableStructuredLogs", false, "Turns off unstructured mode logging features and use Structured logging instead.")
	flag.StringVar(&configFile, "configPath", "./configs/shepherd.yaml", "Absolute file Path for Core Configuration file. Please note that this might still be overwritten by the SHEPHERD_CONFIG_FILE_LOCATION for additional flexibility.")
	flag.StringVar(&blueprintsFile, "blueprintsPath", "./configs/blueprints.yaml", "Absolute file Path for Shepherd Blueprints file. Please note that this might still be overwritten by the SHEPHERD_BLUEPRINTS_FILE_LOCATION for additional flexibility.")
	flag.StringVar(&definitionsFile, "definitionsPath", "./configs/definitions_dev.yaml", "Absolute file Path for Shepherd Definitions file. Please note that this might still be overwritten by the SHEPHERD_DEFINITIONS_FILE_LOCATION for additional flexibility.")
	runMode = assertRunMode(flag.String("runmode", "SINGLE_CLUSTER", "Changes the mode in which the tool is operating. Options are SINGLE_CLUSTER, MULTI_CLUSTER, MIGRATION, CREATE_CONFIGS_FROM_EXISTING_CLUSTER"))
	if !flag.Parsed() {
		flag.Parse()
	}
}

func GetConfigMaps() (sc *ConfigurationMaps) {
	return &ConfMaps
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
