package engine

// var _ = func() bool {
// 	testing.Init()
// 	os.Setenv("SHEPHERD_CONFIG_FILE_LOCATION", "./../configs/shepherd.yaml")
// 	os.Setenv("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./../configs/blueprints.yaml")
// 	os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", "./../configs/definitions_dev.yaml")
// 	return true
// }()

// func TestStackSuite_Helpers(t *testing.T) {
// 	suite.Run(t, new(StackSuite))
// }

func (s *StackSuite) TestStackSuite_Helpers() {
	_, err := GetClientCertificateFromCertNKey("./testdata/client.crt", "./testdata/client.unencrypted.key", "")
	s.NoError(err)

	_, err = GetClientCertificateFromCertNKey("./testdata/client.crt", "./testdata/client.encrypted.key", "testing1234")
	s.NoError(err)

	_, err = GetClientCertificateFromCertNKey("./testdata/client.crt_not_found", "./testdata/client.encrypted.key", "testing1234")
	s.Error(err)

	_, err = GetClientCertificateFromCertNKey("./testdata/client.crt", "./testdata/client.encrypted.key_not_found", "testing1234")
	s.Error(err)

}
