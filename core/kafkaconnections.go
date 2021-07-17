package internal

import (
	"fmt"
	"os"

	"github.com/Shopify/sarama"
)

func GetAdminConnection() sarama.ClusterAdmin {
	conf := sarama.NewConfig()
	conf.ClientID = "abhishekgotest"

	cluster, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, conf)

	if err != nil {
		fmt.Println("Something Went Wrong. Here are the details: ", err)
		os.Exit(1)
	}
	return cluster
}
