package internal

import (
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
		// c.Net.TLS.Config = createTLSConfig()
	case "SASL_PLAINTEXT":
		temp.ClusterSecurityMode = SASL_PLAINTEXT
		c.Net.SASL.Enable = true
		c.Net.SASL.User = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "username")
		c.Net.SASL.Password = ksmisc.FindSASLValues(sc.Configs[0]["sasl.jaas.config"], "password")
		c.Net.SASL.Handshake = true
		c.Net.TLS.Enable = false
	case "SSL":
		temp.ClusterSecurityMode = SSL
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
