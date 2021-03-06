package kafkamanagers

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
	ksmisc "github.com/waliaabhishek/kafka-shepherd/misc"
	"github.com/xdg/scram"
)

var (
	wg     sync.WaitGroup
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
	term                          = make(chan os.Signal)
	logger *ksengine.ShepherdLogger
)

type SaramaConnection struct {
	ConnectionObjectBaseImpl
	SCA *sarama.ClusterAdmin
}

func (c *SaramaConnection) InitiateAdminConnection(cConfig ksengine.ShepherdCluster) {
	logger = ksengine.Shepherd.GetLogger()
	if c.SCA == nil {
		c.validateInputDetails(cConfig)
		conf := c.understandClusterTopology(&cConfig)
		ca, err := sarama.NewClusterAdmin(cConfig.BootstrapServers, conf)
		if err != nil {
			logger.Fatalw("Cannot set up the connection to Kafka Cluster. ",
				"Bootstrap Server", cConfig.BootstrapServers,
				"Error", err)
		}
		addShutdownHook(&ca)
		c.SCA = &ca
	}
}

func (c *SaramaConnection) validateInputDetails(cConfig ksengine.ShepherdCluster) {
	c.executeBaseValidations(&cConfig)
	if len(cConfig.BootstrapServers) == 0 {
		c.generateCustomError(true, "BootstrapServers", "")
	}
	if cConfig.Configs[0]["security.protocol"] == "" {
		c.generateCustomError(true, "security.protocol", "")
	}

}

func (c *SaramaConnection) CloseAdminConnection() {
	wg.Add(1)
	term <- syscall.SIGQUIT
	wg.Wait()
}

func (conn *SaramaConnection) understandClusterTopology(sc *ksengine.ShepherdCluster) (conf *sarama.Config) {
	c := sarama.NewConfig()

	c.ClientID = sc.ClientID
	// This is the minimum version required to support prefixed ACLs.
	c.Version = sarama.V2_0_0_0
	// Figure Out the Security Protocol
	switch s := sc.Configs[0]["security.protocol"]; s {
	case "SASL_SSL":
		logger.Debugf("Inside the %v switch statement", s)
		if sc.Configs[0]["sasl.jaas.config"] == "" {
			conn.generateCustomError(true, "sasl.jaas.config", "SASL_SSL security protocol needs sasl.jaas.config to be configured. Exiting process.")
		}
		c.Net.SASL.Enable = true
		c.Net.SASL.User = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "username")
		c.Net.SASL.Password = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "password")
		c.Net.SASL.Handshake = true
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = createTLSConfig(sc)
	case "SASL_PLAINTEXT":
		logger.Debugf("Inside the %v switch statement", s)
		if sc.Configs[0]["sasl.jaas.config"] == "" {
			conn.generateCustomError(true, "sasl.jaas.config", "SASL_SSL security protocol needs sasl.jaas.config to be configured. Exiting process.")
		}
		c.Net.SASL.Enable = true
		c.Net.SASL.User = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "username")
		c.Net.SASL.Password = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "password")
		// logger.Debugw("TestRun", "Username", c.Net.SASL.User, "password", c.Net.SASL.Password)
		c.Net.SASL.Handshake = true
		c.Net.TLS.Enable = false
	case "SSL":
		logger.Debugf("Inside the %v switch statement", s)
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = createTLSConfig(sc)
	case "PLAINTEXT", "":
		// Do Nothing
		logger.Debug("Inside the PLAINTEXT switch statement")
	default:
		logger.Fatalw("Unknown security mode supplied for Cluster Config",
			"Cluster Name", sc.Name,
			"Cluster Security Protocol Provided", s)
	}

	// Figure out the sasl mechanism
	switch m := sc.Configs[0]["sasl.mechanism"]; m {
	case "PLAIN":
		logger.Debugf("Inside the %v switch statement", m)
		c.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case "SCRAM-SHA-256":
		logger.Debugf("Inside the %v switch statement", m)
		c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		c.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case "SCRAM-SHA-512":
		logger.Debugf("Inside the %v switch statement", m)
		c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		c.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	case "OAUTHBEARER":
		logger.Debugf("Inside the %v switch statement", m)
		c.Net.SASL.Mechanism = "OAUTHBEARER"
	case "":
		logger.Debug("Inside the EMPTY switch statement")
		// Check for KRB5
		//
	}

	return c
}

func addShutdownHook(sca *sarama.ClusterAdmin) {
	var err error = nil
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	go func() {
		defer wg.Done()
		s := <-term
		logger.Infof("%s received. Shutdown Process Initiated", s.String())
		logger.Info("Trying to Close Kafka Cluster connection. Initial Try")
		err = (*sca).Close()
		for i := 2; i <= 5 && err != nil; i++ {
			logger.Errorw("Failed to Close Kafka Cluster connection.", "Error", err)
			logger.Warnw("Trying to Close Kafka Cluster connection again.", "Try", i)
			err = (*sca).Close()
			if err != nil {
				logger.Errorw("Failed to Close Kafka Cluster connection.", "Error", err)
			}
		}
		if err != nil {
			logger.Fatalf("Aborting retries as I was still not able to close the connection even after 5 retries. Will exit ungracefully.")
		} else {
			logger.Info("Kafka Cluster Connection Successfully closed")
		}
	}()
}

func createTLSConfig(sc *ksengine.ShepherdCluster) *tls.Config {
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
	if sc.TLSDetails.ClientCert != "" && sc.TLSDetails.PrivateKey != "" {
		var v *pem.Block
		var pKey []byte
		var clientCert tls.Certificate
		failFlag := false
		execFlag = true

		cCert, err := ioutil.ReadFile(sc.TLSDetails.ClientCert)
		if err != nil {
			logger.Warnw("Cannot read the Client Cert file provided in config. Skipping setting up the client certificate.",
				"Client Certificate Path", sc.TLSDetails.ClientCert,
				"Error Details", err)
			failFlag = true
		}
		b, err := ioutil.ReadFile(sc.TLSDetails.PrivateKey)
		if err != nil {
			logger.Warnw("Cannot read the Private Key file provided in config. Skipping setting up the client certificate.",
				"Private Key Path", sc.TLSDetails.PrivateKey,
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
									"Client Certificate Path", sc.TLSDetails.ClientCert,
									"Private Key Path", sc.TLSDetails.PrivateKey,
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
					"Client Certificate Path", sc.TLSDetails.ClientCert,
					"Private Key Path", sc.TLSDetails.PrivateKey,
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

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
