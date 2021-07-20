package internal

import (
	"flag"
	"os"
	ksmisc "shepherd/misc"
	"strings"
	"text/tabwriter"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	bf, df, cf   string
	rs           RootStruct
	scf          ShepherdConfig
	utm          UserTopicMapping     = UserTopicMapping{}
	tcm          TopicConfigMapping   = TopicConfigMapping{}
	ccm          ClusterConfigMapping = ClusterConfigMapping{}
	blueprintMap map[string]NVPairs
	sca          *sarama.ClusterAdmin
	TW           *tabwriter.Writer = ksmisc.TW
	logger       *zap.SugaredLogger
)

var (
	enableDebug *bool
	runMode     RunMode
)

func init() {
	enableDebug = flag.Bool("debugMode", false, "Turns on Debug mode logging features instead Production grade Structured logging.")
	rm := flag.String("runMode", "SINGLE_CLUSTER", "Changes the mode in which the tool is operating. Options are SINGLE_CLUSTER, MULTI_CLUSTER, MIGRATION, CREATE_CONFIGS_FROM_EXISTING_CLUSTER")
	flag.Parse()
	// Initialize Zap Logger
	logger = getLogger(enableDebug)
	runMode = validateRunMode(rm)

	cf = getEnvVarsWithDefaults("SHEPHERD_CONFIG_FILE_LOCATION", "./configs/shepherd.yaml")
	bf = getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./configs/blueprints.yaml")
	df = getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", "./configs/definitions_dev.yaml")

	// Parse Shepherd Internal Configurations from the YAML file.
	parseShepherdConfig(&scf, cf)
	logger.Debug("Shepherd Config File parse Result: ", scf)

	// Parse the Blueprints & Definitions file and setup the internal representations of the file.
	rs = parseConfigurations(bf, df)
	logger.Debug("Config File parse Result: ", rs)

	// Generate internal canonical from above structures.
	rs.GenerateMappings()

	// Get the Sarama Cluster Admin connection as that is the connection that is necessary for ACL & Topic provisioning.
	sca = setupAdminConnection()
}

// This method sets up a zap logger object for use and returns back a pointer to the object.
func getLogger(mode *bool) *zap.SugaredLogger {
	var config zap.Config
	if *mode {
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		config = zap.NewProductionConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	}
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeDuration = zapcore.MillisDurationEncoder

	logger, _ := config.Build()
	defer logger.Sync()
	return logger.Sugar()
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

func GetObjects() (*RootStruct, *UserTopicMapping, *TopicConfigMapping) {
	return &rs, &utm, &tcm
}

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
