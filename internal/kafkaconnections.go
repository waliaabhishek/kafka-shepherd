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
			_, conf, bs := understandClusterTopology(&cluster)
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

func understandClusterTopology(sc *ShepherdCluster) (ccmv *ClusterConfigMappingValue, conf *sarama.Config, bs string) {
	temp := ClusterConfigMappingValue{}
	c := sarama.NewConfig()

	c.ClientID = sc.ClientID
	// Figure Out the Security Protocol
	switch sc.Configs[0]["security.protocol"] {
	case "SASL_SSL":
		temp.ClusterSecurityMode = SASL_SSL
		c.Net.SASL.Enable = true
		c.Net.SASL.User = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "username")
		c.Net.SASL.Password = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "password")
		c.Net.SASL.Handshake = true
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = createTLSConfig(sc)
	case "SASL_PLAINTEXT":
		temp.ClusterSecurityMode = SASL_PLAINTEXT
		c.Net.SASL.Enable = true
		c.Net.SASL.User = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "username")
		c.Net.SASL.Password = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "password")
		c.Net.SASL.Handshake = true
		c.Net.TLS.Enable = false
	case "SSL":
		temp.ClusterSecurityMode = SSL
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = createTLSConfig(sc)
	case "":
		temp.ClusterSecurityMode = PLAINTEXT
	default:
		logger.Fatalw("Unknown security mode supplied for Cluster Config",
			"Cluster Name", sc.Name,
			"Cluster Security Protocol Provided", sc.Configs[0]["security.protocol"])
	}

	// Figure out the sasl mechanism
	switch sc.Configs[0]["sasl.mechanism"] {
	case "PLAIN":
		temp.ClusterSASLMechanism = PLAIN
		c.Net.SASL.Mechanism = "PLAIN"
	case "SCRAM-SHA-256":
		temp.ClusterSASLMechanism = SCRAM_SHA_256
		c.Net.SASL.Mechanism = "PLAIN"
	case "SCRAM-SHA-512":
		temp.ClusterSASLMechanism = SCRAM_SHA_512
		c.Net.SASL.Mechanism = "PLAIN"
	case "OAUTHBEARER":
		temp.ClusterSASLMechanism = OAUTHBEARER
		c.Net.SASL.Mechanism = "OAUTHBEARER"
	case "":
		// Check for KRB5
		//
	}

	return &temp, c, sc.BootstrapServer
}

func createTLSConfig(sc *ShepherdCluster) *tls.Config {
	var t *tls.Config

	// Generate the CACertPool from the SystemCertPool.
	// If it fails, instantiate an empty pool.
	caCertPool, err := x509.SystemCertPool()
	if err != nil {
		caCertPool = x509.NewCertPool()
	}

	// Addding Certificates provided in the configuration file to Certificate Pool
	for _, cert := range sc.TLSDetails.TrustedCerts {
		caCert, err := ioutil.ReadFile(cert)
		// Skipping the add if fails.
		if err != nil {
			logger.Warnw("Cannot read the CA Certificate file provided in the config. Skipping this certificate.",
				"Certificate File Path", cert,
				"Error Details", err)
		} else {
			caCertPool.AppendCertsFromPEM(caCert)
		}
	}

	// Work on Client Certificates If Provided.
	if sc.TLSDetails.ClientCertPath != "" && sc.TLSDetails.PrivateKeyPath != "" {
		var v *pem.Block
		var pKey []byte
		var clientCert tls.Certificate
		failFlag := false

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
					if v == nil {
						break
					}
					if v.Type == "RSA PRIVATE KEY" {
						if x509.IsEncryptedPEMBlock(v) {
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

	return t
}
