package engine

import (
	"flag"
	"io/ioutil"
	"strings"

	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
	yaml "gopkg.in/yaml.v2"

	mapset "github.com/deckarep/golang-set"
	"go.uber.org/zap"
)

// Externally available variables.
var (
	ConfMaps ConfigurationMaps = ConfigurationMaps{
		TCM: TopicConfigMapping{},
		utm: UserTopicMapping{},
		CCM: ClusterConfigMapping{},
	}
	SpdCore ShepherdCore = ShepherdCore{
		Configs:     ShepherdConfig{},
		Blueprints:  ShepherdBlueprint{},
		Definitions: ShepherdDefinition{},
	}
	ShepherdACLList *ACLMapping
	DryRun          bool
	IsTest          bool
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
	shepherdACLList      *ACLMapping
)

// func InitializeShepherdCore(enableDebug bool, enableConsoleLogs bool, shepherdConfigFilePath string,
// 	shepherdBlueprintsFilePath string, shepherdDefinitionsFilePath string, runString string) {
func init() {
	// Initialize Logger
	ResolveFlags()
	logger = ksmisc.GetLogger(enableDebug, enableStructuredLogs)
	// runMode = assertRunMode(runString)

	// Parse Shepherd Internal Configurations from the YAML file.
	SpdCore.Configs.ParseShepherdConfig(getEnvVarsWithDefaults("SHEPHERD_CONFIG_FILE_LOCATION", configFile), true)
	logger.Debug("Shepherd Config File parse Result: ", SpdCore.Configs)

	// Parse Shepherd Blueprints from the YAML file.
	SpdCore.Blueprints.ParseShepherBlueprints(getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", blueprintsFile))
	logger.Debug("Shepherd Blueprints parse Result: ", SpdCore.Blueprints)

	// Parse Shepherd Internal Configurations from the YAML file.
	SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", definitionsFile), true)
	logger.Debug("Shepherd Definitions parse Result: ", SpdCore.Definitions)

	// Understand the Blueprints & Definitions file and setup the External facing representation of the core files.
	GenerateMappings()
	logger.Debug("Config File parse Result: ", ConfMaps)

	shepherdACLList = ConfMaps.utm.getShepherdACLList()
	ShepherdACLList = shepherdACLList
}

func ResolveFlags() {
	flag.BoolVar(&DryRun, "dryRun", false, "Does not execute anything but lists what will be executed.")
	flag.BoolVar(&IsTest, "testRun", false, "Executes the whole flow and then wipes out all the objects that the configurations provide (not just for this run but configured in the earlier runs as well). This helps in testing the same configurations multiple times without wiping your clusters and starting over.")
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

func GetConfigTopicsAsMapSet() mapset.Set {
	return topicsInConfig
}

func IsDebugEnabled() bool {
	return enableDebug
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

func (shp *ShepherdBlueprint) ParseShepherBlueprints(configFilePath string) {
	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatalw("Cannot read the filepath provided in SHEPHERD_BLUEPRINTS_FILE_LOCATION variable. Please Correct.",
			"Error Received", err)
	}

	if shp == nil {
		shp = &ShepherdBlueprint{}
	}

	if err := yaml.Unmarshal(temp, shp); err != nil {
		logger.Fatal("Error Unmarshaling Shepherd Blueprints File", err)
	}
	shp.validateShepherdBlueprints()
	shp.readValuesFromENV()
	// return shp
}

func (scf *ShepherdBlueprint) validateShepherdBlueprints() {
	logger.Debug("No Validations for Shepherd Blueprints at the moment.")
}

func (shp *ShepherdDefinition) ParseShepherDefinitions(configFilePath string, overwriteExisting bool) *ShepherdDefinition {
	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatalw("Cannot read the filepath provided in SHEPHERD_DEFINITIONS_FILE_LOCATION variable. Please Correct.",
			"Error Received", err)
	}

	if overwriteExisting || shp == nil {
		shp.DefinitionRoot = DefinitionRoot{}
	}

	if err := yaml.Unmarshal(temp, shp); err != nil {
		logger.Fatal("Error Unmarshalling Shepherd Definitions File", err)
	}
	shp.validateShepherdDefinitions()
	shp.readValuesFromENV()
	return shp
}

func (scf *ShepherdDefinition) validateShepherdDefinitions() {
	return
}

func (shp *ShepherdConfig) ParseShepherdConfig(configFilePath string, overwriteExisting bool) {
	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatal("Cannot read the filepath provided in SHEPHERD_CONFIG_FILELOCATION variable. Please Correct. Error Received", err)
	}

	if shp == nil || overwriteExisting {
		shp.ConfigRoot = ConfigRoot{}
	}

	if err := yaml.Unmarshal(temp, shp); err != nil {
		logger.Fatal("Error Unmarshaling Shepherd Configs File", err)
	}
	shp.validateShepherdConfig()
	shp.readValuesFromENV()
	// return shp
}

func (scf *ShepherdConfig) validateShepherdConfig() {
	count := 0
	for _, cluster := range scf.ConfigRoot.Clusters {
		if cluster.IsEnabled {
			count += 1
		}
	}

	switch runMode {
	case RunMode_SINGLE_CLUSTER:
		if count != 1 {
			logger.Fatalw("Unique cluster not enabled in the config file for selected run mode. Either select the correct runMode or enable only ONE cluster via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case RunMode_MULTI_CLUSTER:
		if count < 1 {
			logger.Fatalw("Cannot have less than one cluster(s) enabled in the config file for selected run mode. Either select the correct runMode or enable more clusters via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case RunMode_MIGRATION:
		if count < 2 {
			logger.Fatalw("Cannot have less than two cluster(s) enabled in the config file for selected run mode. Either select the correct runMode or enable more clusters via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	case RunMode_CREATE_CONFIGS:
		if count != 1 {
			logger.Fatalw("Unique cluster not enabled in the config file for selected run mode. Either select the correct runMode or enable only ONE cluster via 'is.enabled' flag.",
				"Selected RunMode", runMode.String(),
				"Enabled Clusters", count)
		}
	}
}
