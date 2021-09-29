package kafkamanagers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type tContainerConfig struct {
	Hostname             string
	ImageName            string
	ExposedPorts         []string
	NetworkName          string
	NetworkAlias         []string
	LogLineToWaitFor     string
	StartupTimeoutInSecs int
	EnvVariables         map[string]string
	CopyFiles            map[string]string
	ExecCommands         []string
}

type tClusterConfig map[int]tContainerConfig

type tClusterValue struct {
	tClusterConfig tClusterConfig
}

var clusters = map[string]tClusterValue{}

var _ = func() bool {

	clusters["plaintext"] = tClusterValue{
		tClusterConfig{
			0: {
				Hostname:             "zookeeper",
				ImageName:            "confluentinc/cp-zookeeper:{version}",
				ExposedPorts:         []string{"2181:2181"},
				NetworkName:          "plaintext",
				NetworkAlias:         []string{"zookeeper"},
				LogLineToWaitFor:     "Started AdminServer on address",
				StartupTimeoutInSecs: 20,
				EnvVariables: map[string]string{
					"ZOOKEEPER_CLIENT_PORT": "2181",
					"ZOOKEEPER_TICK_TIME":   "2000",
				},
			},
			1: {
				Hostname:             "kafka",
				ImageName:            "confluentinc/cp-kafka:{version}",
				ExposedPorts:         []string{"9093:9093"},
				NetworkName:          "plaintext",
				NetworkAlias:         []string{"kafka"},
				LogLineToWaitFor:     "INFO Kafka startTimeMs:",
				StartupTimeoutInSecs: 30,
				EnvVariables: map[string]string{
					"KAFKA_BROKER_ID":                        "101",
					"KAFKA_ZOOKEEPER_CONNECT":                "zookeeper:2181",
					"KAFKA_ADVERTISED_LISTENERS":             "PLAINTEXT://127.0.0.1:9093,BROKER://127.0.0.1:9092",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
					"KAFKA_INTER_BROKER_LISTENER_NAME":       "BROKER",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
				},
			},
		},
	}

	clusters["sasl_plaintext"] = tClusterValue{
		tClusterConfig{
			0: {
				Hostname:             "zookeeper",
				ImageName:            "confluentinc/cp-zookeeper:{version}",
				ExposedPorts:         []string{"2181:2181"},
				NetworkName:          "sasl_plaintext",
				NetworkAlias:         []string{"zookeeper"},
				LogLineToWaitFor:     "Started AdminServer on address",
				StartupTimeoutInSecs: 20,
				EnvVariables: map[string]string{
					"ZOOKEEPER_CLIENT_PORT": "2181",
					"ZOOKEEPER_TICK_TIME":   "2000",
				},
			},
			1: {
				Hostname:             "kafka",
				ImageName:            "confluentinc/cp-kafka:{version}",
				ExposedPorts:         []string{"9093:9093"},
				NetworkName:          "sasl_plaintext",
				NetworkAlias:         []string{"kafka"},
				LogLineToWaitFor:     "INFO Kafka startTimeMs:",
				StartupTimeoutInSecs: 30,
				EnvVariables: map[string]string{
					"KAFKA_BROKER_ID":                                      "101",
					"KAFKA_ZOOKEEPER_CONNECT":                              "zookeeper:2181",
					"KAFKA_ADVERTISED_LISTENERS":                           "EXTERNAL://127.0.0.1:9093,BROKER://kafka:9092",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                 "BROKER:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT",
					"KAFKA_INTER_BROKER_LISTENER_NAME":                     "BROKER",
					"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL":           "PLAIN",
					"KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS":   "PLAIN",
					"KAFKA_LISTENER_NAME_EXTERNAL_SASL_ENABLED_MECHANISMS": "PLAIN",
					"KAFKA_AUTHORIZER_CLASS_NAME":                          "kafka.security.auth.SimpleAclAuthorizer",
					"KAFKA_SUPER_USERS":                                    "User:kafka;User:admin",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":               "1",
					"KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG":    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" ;",
					"KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG":  "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" user_producer=\"producer-secret\" user_consumer=\"consumer-secret\" ;",
					"KAFKA_SASL_JAAS_CONFIG":                               "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";",
				},
			},
			2: {
				Hostname:             "kafka2",
				ImageName:            "confluentinc/cp-kafka:{version}",
				ExposedPorts:         []string{"9094:9094"},
				NetworkName:          "sasl_plaintext",
				NetworkAlias:         []string{"kafka"},
				LogLineToWaitFor:     "INFO Kafka startTimeMs:",
				StartupTimeoutInSecs: 30,
				EnvVariables: map[string]string{
					"KAFKA_BROKER_ID":                                      "102",
					"KAFKA_ZOOKEEPER_CONNECT":                              "zookeeper:2181",
					"KAFKA_ADVERTISED_LISTENERS":                           "BROKER://kafka2:9092, EXTERNAL://127.0.0.1:9094,",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                 "BROKER:SASL_PLAINTEXT, EXTERNAL:SASL_PLAINTEXT",
					"KAFKA_INTER_BROKER_LISTENER_NAME":                     "BROKER",
					"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL":           "PLAIN",
					"KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS":   "PLAIN",
					"KAFKA_LISTENER_NAME_EXTERNAL_SASL_ENABLED_MECHANISMS": "PLAIN",
					"KAFKA_AUTHORIZER_CLASS_NAME":                          "kafka.security.auth.SimpleAclAuthorizer",
					"KAFKA_SUPER_USERS":                                    "User:kafka;User:admin",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":               "1",
					"KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG":    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" ;",
					"KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG":  "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" user_producer=\"producer-secret\" user_consumer=\"consumer-secret\" ;",
					"KAFKA_SASL_JAAS_CONFIG":                               "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";",
				},
			},
		},
	}

	clusters["sasl_scram_256"] = tClusterValue{
		tClusterConfig{
			0: {
				Hostname:             "zookeeper",
				ImageName:            "confluentinc/cp-zookeeper:{version}",
				ExposedPorts:         []string{"2181:2181"},
				NetworkName:          "sasl_scram_256",
				NetworkAlias:         []string{"zookeeper"},
				LogLineToWaitFor:     "Started AdminServer on address",
				StartupTimeoutInSecs: 20,
				EnvVariables: map[string]string{
					"ZOOKEEPER_CLIENT_PORT": "2181",
					"ZOOKEEPER_TICK_TIME":   "2000",
					"KAFKA_OPTS":            "-Djava.security.auth.login.config=/tmp/zk_jaas.conf -Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider",
				},
				CopyFiles: map[string]string{
					fmt.Sprintf("%v/testdata/sasl_scram/zk/zk.sasl.jaas.conf", "."): "/tmp/zk_jaas.conf",
				},
				ExecCommands: []string{},
			},
			1: {
				Hostname:             "kafka",
				ImageName:            "confluentinc/cp-kafka:{version}",
				ExposedPorts:         []string{"9093:9093"},
				NetworkName:          "sasl_scram_256",
				NetworkAlias:         []string{"kafka"},
				LogLineToWaitFor:     "INFO Kafka startTimeMs:",
				StartupTimeoutInSecs: 20,
				EnvVariables: map[string]string{
					"KAFKA_BROKER_ID":                            "101",
					"KAFKA_ZOOKEEPER_CONNECT":                    "zookeeper:2181",
					"KAFKA_ADVERTISED_LISTENERS":                 "BROKER://kafka:9092, EXTERNAL://127.0.0.1:9093",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":       "BROKER:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT",
					"KAFKA_INTER_BROKER_LISTENER_NAME":           "BROKER",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":     "1",
					"KAFKA_SASL_ENABLED_MECHANISMS":              "SCRAM-SHA-256",
					"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL": "SCRAM-SHA-256",
					"KAFKA_ZOOKEEPER_SET_ACL":                    "true",
					"KAFKA_AUTHORIZER_CLASS_NAME":                "kafka.security.auth.SimpleAclAuthorizer",
					"KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND":       "false",
					"KAFKA_SUPER_USERS":                          "User:kafka;User:admin",
					"KAFKA_OPTS":                                 "-Djava.security.auth.login.config=/tmp/kafka.sasl.jaas.conf",
				},
				CopyFiles: map[string]string{
					fmt.Sprintf("%v/testdata/sasl_scram/kafka/kafka.sasl.jaas.conf", "."): "/tmp/kafka.sasl.jaas.conf",
					fmt.Sprintf("%v/testdata/sasl_scram/kafka/kafka.conf", "."):           "/tmp/kafka.conf",
					fmt.Sprintf("%v/testdata/sasl_scram/kafka/cmd.sh", "."):               "/tmp/cmd.sh",
				},
				ExecCommands: []string{},
			},
			2: {
				Hostname:             "kafka2",
				ImageName:            "confluentinc/cp-kafka:{version}",
				ExposedPorts:         []string{"9094:9094"},
				NetworkName:          "sasl_scram_256",
				NetworkAlias:         []string{"kafka"},
				LogLineToWaitFor:     "INFO Kafka startTimeMs:",
				StartupTimeoutInSecs: 20,
				EnvVariables: map[string]string{
					"KAFKA_BROKER_ID":                            "102",
					"KAFKA_ZOOKEEPER_CONNECT":                    "zookeeper:2181",
					"KAFKA_ADVERTISED_LISTENERS":                 "BROKER://kafka2:9092, EXTERNAL://127.0.0.1:9094",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":       "BROKER:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT",
					"KAFKA_INTER_BROKER_LISTENER_NAME":           "BROKER",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":     "1",
					"KAFKA_SASL_ENABLED_MECHANISMS":              "SCRAM-SHA-256",
					"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL": "SCRAM-SHA-256",
					"KAFKA_ZOOKEEPER_SET_ACL":                    "true",
					"KAFKA_AUTHORIZER_CLASS_NAME":                "kafka.security.auth.SimpleAclAuthorizer",
					"KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND":       "false",
					"KAFKA_SUPER_USERS":                          "User:kafka;User:admin",
					"KAFKA_OPTS":                                 "-Djava.security.auth.login.config=/tmp/kafka.sasl.jaas.conf",
				},
				CopyFiles: map[string]string{
					fmt.Sprintf("%v/testdata/sasl_scram/kafka/kafka.sasl.jaas.conf", "."): "/tmp/kafka.sasl.jaas.conf",
					fmt.Sprintf("%v/testdata/sasl_scram/kafka/kafka.conf", "."):           "/tmp/kafka.conf",
					fmt.Sprintf("%v/testdata/sasl_scram/kafka/cmd.sh", "."):               "/tmp/cmd.sh",
				},
				ExecCommands: []string{
					"/tmp/cmd.sh",
				},
			},
		},
	}

	return true
}()

type containerMap struct {
	ctx        context.Context
	network    testcontainers.Network
	containers []testcontainers.Container
}

var contextMap map[string]containerMap

func setupContainers(clusterType string, clusterVersion string) (*containerMap, error) {
	if _, found := contextMap[clusterType]; found {
		delete(contextMap, clusterType)
	}
	var cMap containerMap
	ctx := context.Background()
	cMap.ctx = ctx

	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name: clusterType,
		},
	})
	if err != nil {
		fmt.Printf("Could not setup %v network. Error: %v", clusterType, err.Error())
		return &cMap, err
	}
	cMap.network = network
	dNw := network.(*testcontainers.DockerNetwork)
	for i := 0; i < len(clusters[clusterType].tClusterConfig); i++ {
		v := clusters[clusterType].tClusterConfig[i]
		img := strings.ReplaceAll(v.ImageName, "{version}", clusterVersion)
		ctrReq := testcontainers.ContainerRequest{
			Name:           v.Hostname,
			Hostname:       v.Hostname,
			Image:          img,
			ExposedPorts:   v.ExposedPorts,
			Networks:       []string{dNw.Name},
			NetworkAliases: map[string][]string{dNw.Name: v.NetworkAlias},
			WaitingFor:     wait.ForLog(v.LogLineToWaitFor).WithPollInterval(1 * time.Second).WithStartupTimeout(time.Duration(v.StartupTimeoutInSecs * int(time.Second))),
			Env:            v.EnvVariables,
			// SkipReaper:     true,
			// VolumeMounts:   v.CopyFiles,
		}
		ctrReq.PrintBuildLog = true
		ctr, _ := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: ctrReq,
			Started:          false,
		})
		for hostPath, contPath := range v.CopyFiles {
			fmt.Println("Copying File ", hostPath, "to container path ", contPath)
			ctr.CopyFileToContainer(ctx, hostPath, contPath, 0777)
		}
		err = ctr.Start(ctx)
		if err != nil {
			fmt.Printf("Could not start %v container. Error: %v", v.Hostname, err.Error())
			return &cMap, err
		}
		cMap.containers = append(cMap.containers, ctr)

		for _, cmd := range v.ExecCommands {
			cName, _ := ctr.Name(ctx)
			fmt.Println("Executing command on container", cName, "; Command", cmd)
			// i, err := ctr.
			i, err := ctr.Exec(ctx, []string{"bash", cmd})
			if err != nil {
				fmt.Println(i, err)
			}
		}

	}
	return &cMap, nil
}
