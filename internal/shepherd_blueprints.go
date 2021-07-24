package core

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func parseShepherBlueprints(shp *ShepherdBlueprint, configFilePath string) *ShepherdBlueprint {
	temp, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logger.Fatalw("Cannot read the filepath provided in SHEPHERD_BLUEPRINTS_FILE_LOCATION variable. Please Correct.",
			"Error Received", err)
	}

	if err := yaml.Unmarshal(temp, shp); err != nil {
		logger.Fatal("Error Unmarshaling Shepherd Blueprints File", err)
	}
	shp.validateShepherdBlueprints()
	shp.readValuesFromENV()
	return shp
}

func (scf *ShepherdBlueprint) validateShepherdBlueprints() {
	return
}
