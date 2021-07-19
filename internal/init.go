package internal

import (
	"flag"
	"os"
	ksmisc "shepherd/misc"
	"text/tabwriter"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	bf, df, cf   string
	rs           RootStruct
	scf          ShepherdConfig
	utm          UserTopicMapping   = UserTopicMapping{}
	tcm          TopicConfigMapping = TopicConfigMapping{}
	blueprintMap map[string]NVPairs
	sca          *sarama.ClusterAdmin
	TW           *tabwriter.Writer = ksmisc.TW
	logger       *zap.SugaredLogger
)

func init() {

	devMode := flag.Bool("devMode", false, "Turns on Developer mode logging features instead Production grade Structured logging.")
	flag.Parse()
	// Initialize Zap Logger
	logger = getLogger(devMode)

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
	sca = GetAdminConnection()
}

// This method sets up a zap logger object for use and returns back a pointer to the object.
func getLogger(devMode *bool) *zap.SugaredLogger {
	var config zap.Config
	if *devMode {
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
