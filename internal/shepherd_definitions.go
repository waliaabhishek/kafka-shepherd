package core

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func parseShepherDefinitions(shp *ShepherdDefinition, configFilePath string) *ShepherdDefinition {
	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatalw("Cannot read the filepath provided in SHEPHERD_DEFINITIONS_FILE_LOCATION variable. Please Correct.",
			"Error Received", err)
	}

	if err := yaml.Unmarshal(temp, shp); err != nil {
		logger.Fatal("Error Unmarshaling Shepherd Definitions File", err)
	}
	shp.validateShepherdDefinitions()
	shp.readValuesFromENV()
	return shp
}

func (scf *ShepherdDefinition) validateShepherdDefinitions() {
	return
}
