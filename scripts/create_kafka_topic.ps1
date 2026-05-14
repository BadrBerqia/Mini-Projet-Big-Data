docker exec mnp_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic reviews --partitions 1 --replication-factor 1

