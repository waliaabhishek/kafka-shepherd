kafka-configs --zookeeper zookeeper:2181 --alter --add-config 'SCRAM-SHA-256=[password=kafkapass],SCRAM-SHA-512=[password=kafkapass]' --entity-type users --entity-name kafka
kafka-configs --zookeeper zookeeper:2181 --alter --add-config 'SCRAM-SHA-256=[password=password],SCRAM-SHA-512=[password=password]' --entity-type users --entity-name admin
kafka-configs --zookeeper zookeeper:2181 --alter --add-config 'SCRAM-SHA-256=[password=consumerpass],SCRAM-SHA-512=[password=consumerpass]' --entity-type users --entity-name consumer
kafka-configs --zookeeper zookeeper:2181 --alter --add-config 'SCRAM-SHA-256=[password=producerpass],SCRAM-SHA-512=[password=producerpass]' --entity-type users --entity-name producer
kafka-acls  --bootstrap-server kafka:9092 --command-config /tmp/kafka.conf --add --allow-principal User:producer --producer --topic=*
kafka-acls  --bootstrap-server kafka:9092 --command-config /tmp/kafka.conf --add --allow-principal User:consumer --consumer --topic=* --group=*
