package engine

import (
	"fmt"
	"os"
	"reflect"
)

func (s *StackSuite) TestStackSuite_ExternalFunctions_ShepherdACLMapping() {
	for _, c := range aclMappingCases {
		os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", c.inDefFileName)
		SpdCore.Definitions = *SpdCore.Definitions.ParseShepherDefinitions(getEnvVarsWithDefaults("SHEPHERD_DEFINITIONS_FILE_LOCATION", ""), true)
		ConfMaps.utm = UserTopicMapping{}
		GenerateMappings()
		result := ShepherdOperationType_EMPTY.GenerateACLMappingStructures("", ConfMaps.utm.getShepherdACLList())
		s.True(reflect.DeepEqual(c.out, result), fmt.Sprintf("Expected Value: %v \n\n  Actual Value: %v \n\nFilename: %v\n\nError: Failed while invoking GenerateACLMappingStructures with error %v", c.out, result, c.inDefFileName, c.err))
	}
}
