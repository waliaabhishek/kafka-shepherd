package kafkamanagers

import (
	"net/http"

	"github.com/go-resty/resty/v2"
	ksengine "github.com/waliaabhishek/kafka-shepherd/engine"
)

type ConfluentMDSConnection struct {
	MDS *http.Client
}

func (c *ConfluentMDSConnection) InitiateAdminConnection(cConfig ksengine.ShepherdCluster) {
	client := resty.New()
	if len(cConfig.TLSDetails.TrustedCerts) > 0 {
		for _, cert := range cConfig.TLSDetails.TrustedCerts {
			client.SetRootCertificate(cert)
		}
	}
	panic("not implemented") // TODO: Implement
}

func (c *ConfluentMDSConnection) validateInputDetails(cConfig ksengine.ShepherdCluster) {

	panic("not implemented") // TODO: Implement
}

func (c *ConfluentMDSConnection) CloseAdminConnection() {
	panic("not implemented") // TODO: Implement
}
