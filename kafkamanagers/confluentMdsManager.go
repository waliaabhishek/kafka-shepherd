package kafkamanagers

import (
	"encoding/json"
	"net/url"

	"github.com/go-resty/resty/v2"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
)

type ConfluentMDSConnection struct {
	ConnectionObjectBaseImpl
	MDS              *resty.Client
	KafkaClusterID   string
	ConnectClusterID string
	KSQLClusterID    string
	SRClusterID      string
}

const (
	erp_kafkaClusterID    string = "/kafka/v3/clusters"
	erp_altKafkaClusterID string = "/v1/metadata/id"
	mds_kafkaClusterID    string = "/security/1.0/authenticate"
)

// var (
// 	ConfAdminConnection ConnectionObject = &ConfluentMDSConnection{}
// )

func (c *ConfluentMDSConnection) InitiateAdminConnection(cConfig ksengine.ShepherdCluster) {
	if c.MDS == nil {
		c.validateInputDetails(cConfig)
		erpNeeded := true
		if cConfig.Configs[0]["kafka-cluster"] != "" {
			c.KafkaClusterID = cConfig.Configs[0]["kafka-cluster"]
			erpNeeded = false
		}

		if cConfig.Configs[0]["connect-cluster"] != "" {
			c.ConnectClusterID = cConfig.Configs[0]["connect-cluster"]
		}

		if cConfig.Configs[0]["ksql-cluster"] != "" {
			c.KSQLClusterID = cConfig.Configs[0]["ksql-cluster"]
		}

		if cConfig.Configs[0]["schema-registry-cluster"] != "" {
			c.SRClusterID = cConfig.Configs[0]["schema-registry-cluster"]
		}

		client1 := resty.New()
		var client2 *resty.Client
		if erpNeeded {
			client2 = resty.New()
		}
		if len(cConfig.TLSDetails.TrustedCerts) > 0 {
			for _, cert := range cConfig.TLSDetails.TrustedCerts {
				client1.SetRootCertificate(cert)
				if erpNeeded {
					client2.SetRootCertificate(cert)
				}
			}
		}
		if cConfig.TLSDetails.Enable2WaySSL {
			cert, err := ksengine.GetClientCertificateFromCertNKey(cConfig.TLSDetails.ClientCert, cConfig.TLSDetails.PrivateKey, cConfig.TLSDetails.PrivateKeyPassword)
			if err != nil {
				logger.Fatalw("Expected to Enable 2 Way SSL. But I was not able to create the Keystore. Cannot set up connection without it",
					"Private Cert Path", cConfig.TLSDetails.ClientCert,
					"Private Key Path", cConfig.TLSDetails.PrivateKey,
					"Error Received", err)
			}
			client1.SetCertificates(*cert)
			if erpNeeded {
				client2.SetCertificates(*cert)
			}
		}

		if erpNeeded {
			altResp := false
			client2.SetAuthScheme("Basic")
			client2.SetHeader("Accept", "application/json")
			client2.SetBasicAuth(cConfig.Configs[0]["mds.username"], cConfig.Configs[0]["mds.password"])
			client2.SetLogger(logger)
			client2.SetDebug(ksengine.IsDebugEnabled())

			url, err := url.Parse(cConfig.Configs[0]["erp.url"])
			if err != nil {
				logger.Fatalw("Cannot parse the URL. Please check the URLL and try again.",
					"URL", cConfig.Configs[0]["erp.url"])
			}
			client2.SetHostURL(url.String())

			resp, err := client2.SetCloseConnection(true).R().Get(erp_kafkaClusterID)
			if err != nil || resp.StatusCode() > 400 {
				resp, err = client2.SetCloseConnection(true).R().Get(erp_altKafkaClusterID)
				altResp = true
			}
			if err != nil || resp.StatusCode() > 400 {
				logger.Fatalw("Not able to call the ERP URL with provided details. Cannot proceed",
					"Error", string(resp.Body()))
			}
			var r map[string]interface{}
			err = json.Unmarshal(resp.Body(), &r)
			if err != nil {
				logger.Fatalw("Error while Parsing Response Data. Please try again.",
					"Response Received", resp,
					"Error", err)
			}
			var cluster_id string
			if !altResp {
				cluster_id = r["data"].(map[string]interface{})["cluster_id"].(string)
			} else {
				cluster_id = r["id"].(string)
			}
			logger.Debugw("Extracted the Cluster ID successfully. ERP Connection Succeeded.",
				"Cluster Name", cConfig.Name,
				"ERP Server", cConfig.Configs[0]["erp.url"],
				"Cluster ID", cluster_id)
			c.KafkaClusterID = cluster_id
		}

		client1.SetAuthScheme("Basic")
		client1.SetHeader("Accept", "application/json")
		client1.SetBasicAuth(cConfig.Configs[0]["mds.username"], cConfig.Configs[0]["mds.password"])
		client1.SetDebug(ksengine.IsDebugEnabled())
		url, err := url.Parse(cConfig.Configs[0]["mds.url"])
		if err != nil {
			logger.Fatalw("Cannot parse the URL. Please check the URL and try again.",
				"URL", cConfig.Configs[0]["mds.url"])
		}
		client1.SetHostURL(url.String())
		resp, err := client1.R().Get(mds_kafkaClusterID)
		if err != nil {
			logger.Fatalw("Failed Request Execution with MDS Server using provided details. Cannot proceed.",
				"Error", err)
		}

		if resp.StatusCode() > 400 && resp.StatusCode() < 500 {
			logger.Fatalw("Authorization failed using the current credentials. Please ensure correct credentials. Cannot Proceed.",
				"Status Code", resp.StatusCode(),
				"Response", resp,
				"Error Details", err)
		}

		logger.Infow("Authentication successful with MDS.")

		//Getting the Auth Token and updating it into the client so that the User/pass are not exposed externally.
		r := make(map[string]interface{})
		err = json.Unmarshal(resp.Body(), &r)
		if err != nil {
			logger.Fatalw("Error while Parsing Response Data. Please try again.",
				"Response Received", resp,
				"Error", err)
		}
		auth_token := r["auth_token"].(string)
		client1.SetAuthScheme("Bearer")
		client1.SetAuthToken(auth_token)
		client1.SetLogger(logger)
		logger.Debugw("Set MDS Client")
		c.MDS = client1
	}
}

func (c *ConfluentMDSConnection) validateInputDetails(cConfig ksengine.ShepherdCluster) {
	c.executeBaseValidations(&cConfig)
	if cConfig.Configs[0]["erp.url"] == "" && cConfig.Configs[0]["kafka-cluster"] == "" {
		c.generateCustomError(true, "cluster.configOverrides[\"erp.url\"]", "Need Embedded REST Proxy URL for Cluster Identification or the Kafka Cluster ID. Ensure that the user has the right access for ACL Execution")
	}
	if cConfig.Configs[0]["mds.url"] == "" {
		c.generateCustomError(true, "cluster.configOverrides[\"mds.url\"]", "Need Server URL for MDS Connectivity. Ensure that the user has the right access for ACL Execution")
	}
	if cConfig.Configs[0]["mds.username"] == "" {
		c.generateCustomError(true, "cluster.configOverrides[\"mds.username\"]", "Need MDS Server Username for MDS Connectivity. Ensure that the user has the right access for ACL Execution")
	}
	if cConfig.Configs[0]["mds.password"] == "" {
		c.generateCustomError(true, "cluster.configOverrides[\"mds.password\"]", "Need MDS Server Password for MDS Connectivity. Ensure that the user has the right access for ACL Execution")
	}
}

func (c *ConfluentMDSConnection) CloseAdminConnection() {
	return
}
