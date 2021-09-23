package engine

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

var _ = func() bool {
	testing.Init()
	os.Setenv("SHEPHERD_CONFIG_FILE_LOCATION", "./../configs/shepherd.yaml")
	os.Setenv("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./../configs/blueprints.yaml")
	os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", "./../configs/definitions_dev.yaml")
	return true
}()

type StackSuite struct {
	suite.Suite
}

func TestStackSuite(t *testing.T) {
	suite.Run(t, new(StackSuite))
}

func (s *StackSuite) SetupTest() {
	os.Setenv("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./testdata/blueprints_0.yaml")
	SpdCore.Blueprints.ParseShepherBlueprints(getEnvVarsWithDefaults("SHEPHERD_BLUEPRINTS_FILE_LOCATION", ""))
}
