package engine

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"strings"
)

// This function can be used to check if a specific string value begins with the ENV Var prefix or not.
// If it begins with the ENVVAR_PREFIX constant, then it will consider everything after the ENVVAR_PREFIX
// as the ENV Variable name and try to fetch it or return the default Value
func envVarCheckNReplace(s string, def string) string {
	if strings.HasPrefix(s, ENVVAR_PREFIX) {
		return getEnvVarsWithDefaults(strings.Replace(s, ENVVAR_PREFIX, "", 1), def)
	}
	if s == "" && def != "" {
		return def
	}
	return s
}

// If the ENV Variable is empty but the default value exists, then the default value would be returned.
// If the ENV Variable is not present and the default value is also empty, it will fail with a fatal exception.
// If Env Variable is found and has a value, the string value will be returned.
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

/* The standard YAML parsers creates a lot of NVPairs and sets up arrays for all or them.
This function tries to consolidate all the NVPairs into a single []NVPairs structure.
*/
func streamlineNVPairs(in []NVPairs) []NVPairs {
	cMap := make(NVPairs)
	cMap.merge(in)
	return []NVPairs{cMap}
}

func (in *NVPairs) merge(temp []NVPairs) (out *NVPairs) {
	for _, v1 := range temp {
		for k, v := range v1 {
			(*in)[k] = v
		}
	}
	return in
}

func GetClientCertificateFromCertNKey(pathToClientCert string, pathToPrivateKey string, privateKeyPassword string) (*tls.Certificate, error) {

	var v *pem.Block
	var pKey []byte
	var clientCert tls.Certificate
	failFlag := false

	cCert, err := ioutil.ReadFile(pathToClientCert)
	if err != nil {
		logger.Errorw("Cannot read the Client Cert file provided in config. Skipping setting up the client certificate.",
			"Client Certificate Path", pathToClientCert,
			"Error Details", err)
		return nil, err
	}
	b, err := ioutil.ReadFile(pathToPrivateKey)
	if err != nil {
		logger.Errorw("Cannot read the Private Key file provided in config. Skipping setting up the client certificate.",
			"Private Key Path", pathToPrivateKey,
			"Error Details", err)
		return nil, err
	}
	for {
		v, b = pem.Decode(b)
		logger.Debug("Private Key Decoded.")
		if v == nil {
			logger.Debug("inside the circuit Breaker for Decoder.")
			break
		}
		if v.Type == "PRIVATE KEY" || v.Type == "RSA PRIVATE KEY" {
			logger.Debug("Triggered Private Key Encryption Check")
			if x509.IsEncryptedPEMBlock(v) {
				logger.Debug("Private Key is Encrypted. Will need the Private Key Password")
				pKey, err = x509.DecryptPEMBlock(v, []byte(privateKeyPassword))
				if err != nil {
					logger.Warnw("Cannot decrypt the Private Key file provided in config. Skipping setting up the client certificate.",
						"Client Certificate Path", pathToClientCert,
						"Private Key Path", pathToPrivateKey,
						"Error Details", err)
					failFlag = true
					break
				}
				pKey = pem.EncodeToMemory(&pem.Block{
					Type:  v.Type,
					Bytes: pKey,
				})
			} else {
				pKey = pem.EncodeToMemory(v)
			}
		}
	}

	if !failFlag {
		logger.Debug("Private Certificate & Key Loaded")
		clientCert, err = tls.X509KeyPair(cCert, pKey)
	}

	return &clientCert, err
}

// func (sc *DefinitionRoot) PrettyPrintScope() {
// 	for _, v1 := range sc.ScopeFlow {
// 		v1.prettyPrintSND(0)
// 	}
// }

// func (snd *ScopeDefinition) prettyPrintSND(tabCounter int) {
// 	fmt.Println(strings.Repeat("  ", tabCounter), "Short Name:", snd.ShortName)
// 	fmt.Println(strings.Repeat("  ", tabCounter), "Custom Enum Ref:", snd.CustomEnumRef)
// 	fmt.Println(strings.Repeat("  ", tabCounter), "Values:", snd.Values)
// 	fmt.Println(strings.Repeat("  ", tabCounter), "Include in Topic Name Flag:", snd.IncludeInTopicName)
// 	fmt.Println(strings.Repeat("  ", tabCounter), "Topics:", snd.Topics)
// 	fmt.Println(strings.Repeat("  ", tabCounter), "Clients:", snd.Clients)
// 	if snd.Child != nil {
// 		fmt.Println(strings.Repeat("  ", tabCounter), "Child Node:")
// 		snd.Child.prettyPrintSND(tabCounter + 1)
// 	}
// }
