kafka-s3-go-consumer
====================

A `golang` consumer of Kafka topics, with an S3 sink.

Install
--------------------
Several dependencies:

```bash
  sudo /usr/local/go/bin/go get github.com/crowdmob/kafka
  sudo /usr/local/go/bin/go get github.com/crowdmob/goconfig
  sudo /usr/local/go/bin/go get github.com/crowdmob/goamz/s3
```

Run
--------------------
```bash
go run consumer.go -c <config_file_path> -k <true|false>
```

* `-c` Defaults to conf.properties in the current working directory
* `-k` Defaults to false and specifies wehther or not to keep chunkbuffer files around for inspection
