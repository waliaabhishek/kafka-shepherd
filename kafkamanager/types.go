package kafkamanager

type KafkaConnection interface {
	GetAdminConnection() ConnectionObject
	CloseAdminConnection()
}

type ConnectionObject interface {
	Close() error
}
