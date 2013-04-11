kafka-s3-go-consumer
====================

A `golang` consumer of Kafka topics, with an S3 sink.

Run
--------------------
```bash
go run consumer.go -c <config_file_path> -o <comma_separated_offsets_per_topic>
```

-c Defaults to conf.properties in the current working directory
-o Defaults to 0,0,0,... (a zero offset per topic specified in conf file)
