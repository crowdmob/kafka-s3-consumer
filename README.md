kafka-s3-go-consumer
====================

A `golang` consumer of Kafka topics, with an S3 sink.

Install
--------------------

First, get all dependencies with `go get .`

Don't forget to make your own config file, by copying and updating the example one, found at `consumer.example.properties`.

Run
--------------------
```bash
go run consumer.go -c <config_file_path> -k <true|false>
```

* `-c` Defaults to conf.properties in the current working directory
* `-k` Defaults to false and specifies whether or not to keep chunkbuffer files around for inspection

Deployment
--------------------

There's a Chef recipe to fetch and build this on any server, at https://github.com/crowdmob/chef-kafka-s3-consumer-cookbook

This recipe also works with Amazon OpsWorks, and we've been using it in a production environment.


Dependencies
---------------------
Several dependencies:

```
  github.com/crowdmob/kafka
  github.com/crowdmob/goconfig
  github.com/crowdmob/goamz/s3
```

But these can all be gotten by `go get .`


License and Author
===============================
Author:: Matthew Moore

Copyright:: 2013, CrowdMob Inc.


Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

