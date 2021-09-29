package kafkamanagers

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/waliaabhishek/kafka-shepherd/engine"
	"github.com/waliaabhishek/kafka-shepherd/misc"
)

var _ = func() bool {
	testing.Init()
	return true
}()

func init() {
}

type StackSuite struct {
	suite.Suite
}

func TestStackSuite(t *testing.T) {
	suite.Run(t, new(StackSuite))
}

func (s *StackSuite) SetupTest() {
	os.Setenv("SHEPHERD_CONFIG_FILE_LOCATION", "./testdata/configs/shepherd_0.yaml")
	os.Setenv("SHEPHERD_BLUEPRINTS_FILE_LOCATION", "./testdata/configs/blueprints_0.yaml")
	os.Setenv("SHEPHERD_DEFINITIONS_FILE_LOCATION", "./testdata/configs/definitions_0.yaml")
	os.Setenv("HOSTNAME_REPLACEMENT_TEST", "replacement.hostname")
}

func (s *StackSuite) TestStackSuite_SaramaConnection() {
	cases := []struct {
		inConfigFile     string
		inClusterName    string
		inClusterType    string
		inClusterVersion []string
		addlCmds         []string
		out              []int
		err              string
	}{
		{"{relativePath}/testdata/cluster_config/shepherd_0.yaml", "dev_plaintext", "plaintext", []string{"5.4.5", "5.5.2", "6.0.1", "6.1.1", "6.2.0"}, nil, []int{1, 101}, "Plaintext Cluster connection failed."},
		{"{relativePath}/testdata/cluster_config/shepherd_2.yaml", "test2_sasl_plaintext", "sasl_plaintext", []string{"5.4.5", "5.5.2", "6.0.1", "6.1.1", "6.2.0"}, nil, []int{2, 101}, "SASL Plain Cluster connection failed."},
		{"{relativePath}/testdata/cluster_config/shepherd_3.yaml", "test3_sasl_plaintext_scram", "sasl_scram_256", []string{"5.4.5", "5.5.2", "6.0.1", "6.1.1", "6.2.0"}, nil, []int{2, 101}, "SASL Plain (SCRAM-256) Cluster connection failed."},
	}
	for _, c := range cases {
		for _, ver := range c.inClusterVersion {
			pwd, _ := os.Getwd()
			os.Setenv("SHEPHERD_CONFIG_FILE_LOCATION", strings.ReplaceAll(c.inConfigFile, "{relativePath}", pwd))
			engine.Init()
			misc.DottedLineOutput(fmt.Sprintf("Testing for %v cluster with Kafka Version %v", c.inClusterType, ver), "=", 80)
			cMap, err := setupContainers(c.inClusterType, ver)
			if err != nil {
				for _, ctr := range cMap.containers {
					if err := ctr.Terminate(cMap.ctx); err != nil {
						fmt.Println("Failed to Terminate a container. Will continue to try and tear down the network. Error: ", err)
					}
				}
				if cMap.network != nil {
					if err := cMap.network.Remove(cMap.ctx); err != nil {
						fmt.Println("Failed to tear down the network. Error: ", err)
					}
				}
				s.Fail("Unable to setup the Kafka containers for testing. Error: ", err)
			}
			InitiateAllKafkaConnections(engine.SpdCore.Configs.ConfigRoot)
			conn := Connections[KafkaConnectionsKey{ClusterName: c.inClusterName, ConnectionType: ConnectionType_SARAMA}].Connection.(*SaramaConnection)
			brokers, controller, _ := (*conn.SCA).DescribeCluster()
			fmt.Println(brokers, controller)

			s.EqualValues(c.out[1], controller, fmt.Sprintf("FileName: %v\nCluster Type: %v\nCluster Version: %v\nError: %v; %v", c.inConfigFile, c.inClusterType, c.inClusterVersion, c.err, err))
			s.EqualValues(c.out[0], len(brokers), fmt.Sprintf("FileName: %v\nCluster Type: %v\nCluster Version: %v\nError: %v; %v", c.inConfigFile, c.inClusterType, c.inClusterVersion, c.err, err))

			for _, ctr := range cMap.containers {
				ctr.Terminate(cMap.ctx)
			}
			cMap.network.Remove(cMap.ctx)
		}
	}
}
