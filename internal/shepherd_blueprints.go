package core

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func (shp *ShepherdBlueprint) parseShepherBlueprints(configFilePath string) {
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
