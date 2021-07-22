package internal

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	ksmisc "shepherd/misc"

	"github.com/Shopify/sarama"
)

func setupAdminConnection() *sarama.ClusterAdmin {

	for _, cluster := range scf.Config.Clusters {
		if cluster.IsEnabled {
			conf, bs := understandClusterTopology(&cluster)
			ca, err := sarama.NewClusterAdmin([]string{bs}, conf)
			if err != nil {
				logger.Fatalw("Cannot set up the connection to Kafka Cluster. ",
					"Bootstrap Server", bs,
					"Error", err)
			}
			return &ca
		}
	}
	return nil
}

func GetAdminConnection() *sarama.ClusterAdmin {
	return sca
}

func understandClusterTopology(sc *ShepherdCluster) (conf *sarama.Config, bs string) {
	// temp := ClusterConfigMappingValue{}
	c := sarama.NewConfig()

	c.ClientID = sc.ClientID
	// Figure Out the Security Protocol
	switch sc.Configs[0]["security.protocol"] {
	case "SASL_SSL":
		logger.Debug("Inside the SASL_SSL switch statement")
		// temp.ClusterSecurityMode = SASL_SSL
		c.Net.SASL.Enable = true
		c.Net.SASL.User = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "username")
		c.Net.SASL.Password = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "password")
		c.Net.SASL.Handshake = true
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = createTLSConfig(sc)
	case "SASL_PLAINTEXT":
		logger.Debug("Inside the SASL_PLAINTEXT switch statement")
		// temp.ClusterSecurityMode = SASL_PLAINTEXT
		c.Net.SASL.Enable = true
		c.Net.SASL.User = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "username")
		c.Net.SASL.Password = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "password")
		c.Net.SASL.Handshake = true
		c.Net.TLS.Enable = false
	case "SSL":
		logger.Debug("Inside the SSL switch statement")
		// temp.ClusterSecurityMode = SSL
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = createTLSConfig(sc)
	default:
		logger.Fatalw("Unknown security mode supplied for Cluster Config",
			"Cluster Name", sc.Name,
			"Cluster Security Protocol Provided", sc.Configs[0]["security.protocol"])
	}

	// Figure out the sasl mechanism
	switch sc.Configs[0]["sasl.mechanism"] {
	case "PLAIN":
		logger.Debug("Inside the PLAIN switch statement")
		// temp.ClusterSASLMechanism = PLAIN
		c.Net.SASL.Mechanism = "PLAIN"
	case "SCRAM-SHA-256":
		logger.Debug("Inside SCRAM-256 SSL switch statement")
		// temp.ClusterSASLMechanism = SCRAM_SHA_256
		c.Net.SASL.Mechanism = "PLAIN"
	case "SCRAM-SHA-512":
		logger.Debug("Inside the SCRAM-512 switch statement")
		// temp.ClusterSASLMechanism = SCRAM_SHA_512
		c.Net.SASL.Mechanism = "PLAIN"
	case "OAUTHBEARER":
		logger.Debug("Inside the OAUTHBEARER switch statement")
		// temp.ClusterSASLMechanism = OAUTHBEARER
		c.Net.SASL.Mechanism = "OAUTHBEARER"
	case "":
		logger.Debug("Inside the EMPTY switch statement")
		// Check for KRB5
		//
	}

	return c, sc.BootstrapServer
}

func createTLSConfig(sc *ShepherdCluster) *tls.Config {
	var t *tls.Config

	// Generate the CACertPool from the SystemCertPool.
	// If it fails, instantiate an empty pool.
	caCertPool, err := x509.SystemCertPool()
	if err != nil {
		caCertPool = x509.NewCertPool()
	}

	execFlag := false
	// Addding Certificates provided in the configuration file to Certificate Pool
	for _, cert := range sc.TLSDetails.TrustedCerts {
		execFlag = true
		caCert, err := ioutil.ReadFile(cert)
		// Skipping the add if fails.
		if err != nil {
			logger.Warnw("Cannot read the CA Certificate file provided in the config. Skipping this certificate.",
				"Certificate File Path", cert,
				"Error Details", err, err)
		} else {
			logger.Debugw("Trying to load the certificate into the Truststore",
				"Certificate Path", cert)
			if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
				logger.Warnw("Cannot append the CA Certificate into the truststore. Skipping this certificate.",
					"Certificate File Path", cert)
			}
		}
	}

	if !execFlag {
		logger.Debug("No Certificates were added to Truststore as no Trusted Certs were supplied via config.clusters.tlsDetails.trustCertFilePath")
	}

	execFlag = false
	// Work on Client Certificates If Provided.
	if sc.TLSDetails.ClientCertPath != "" && sc.TLSDetails.PrivateKeyPath != "" {
		var v *pem.Block
		var pKey []byte
		var clientCert tls.Certificate
		failFlag := false
		execFlag = true

		cCert, err := ioutil.ReadFile(sc.TLSDetails.ClientCertPath)
		if err != nil {
			logger.Warnw("Cannot read the Client Cert file provided in config. Skipping setting up the client certificate.",
				"Client Certificate Path", sc.TLSDetails.ClientCertPath,
				"Error Details", err)
			failFlag = true
		}
		b, err := ioutil.ReadFile(sc.TLSDetails.PrivateKeyPath)
		if err != nil {
			logger.Warnw("Cannot read the Private Key file provided in config. Skipping setting up the client certificate.",
				"Private Key Path", sc.TLSDetails.PrivateKeyPath,
				"Error Details", err)
			failFlag = true
		} else {
			if !failFlag {
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
							pKey, err = x509.DecryptPEMBlock(v, []byte(sc.TLSDetails.PrivateKeyPassword))
							if err != nil {
								logger.Warnw("Cannot decrypt the Private Key file provided in config. Skipping setting up the client certificate.",
									"Client Certificate Path", sc.TLSDetails.ClientCertPath,
									"Private Key Path", sc.TLSDetails.PrivateKeyPath,
									"Error Details", err)
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
			}
		}

		if !failFlag {
			logger.Debug("Private Certificate & Key Loaded")
			clientCert, err = tls.X509KeyPair(cCert, pKey)
			if err != nil {
				logger.Warnw("Cannot Setup the Client certificate provided in config. Skipping setting up the client certificate.",
					"Client Certificate Path", sc.TLSDetails.ClientCertPath,
					"Private Key Path", sc.TLSDetails.PrivateKeyPath,
					"Error Details", err)
			} else {
				t = &tls.Config{
					Certificates:       []tls.Certificate{clientCert},
					RootCAs:            caCertPool,
					InsecureSkipVerify: !sc.TLSDetails.Enable2WaySSL,
				}
			}

		}

	}

	if !execFlag {
		logger.Debug("No Private SSL certs were loaded as no Private Key + Cert pair was supplied via config.clusters.tlsDetails.clientCertFilePath, privateKeyFilePath")
	}

	return t
}
