package kafkamanager

type KafkaConnection interface {
	SetupAdminConnection() ConnectionObject
}

type ConnectionObject interface{}
