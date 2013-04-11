/*

github.com/jedsmith/kafka: Go bindings for Kafka

Copyright 2000-2011 NeuStar, Inc. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of NeuStar, Inc., Jed Smith, nor the names of
    contributors may be used to endorse or promote products derived from
    this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL NEUSTAR OR JED SMITH BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

NeuStar, the Neustar logo and related names and logos are registered
trademarks, service marks or tradenames of NeuStar, Inc. All other 
product names, company names, marks, logos and symbols may be trademarks
of their respective owners.  

*/

package main

import (
  "flag"
  "fmt"
  "github.com/jedsmith/kafka"
  "os"
  "os/signal"
  "syscall"
  "io/ioutil"
  "strings"
  "strconv"
  
  configfile "github.com/crowdmob/goconfig"
)

var configFilename string
var offset uint64

func init() {
  flag.StringVar(&configFilename, "c", "conf.properties", "path to config file")
	flag.Uint64Var(&offset, "offset", 0, "offset to start consuming from")
}

func main() {
  flag.Parse()
  config, err := configfile.ReadConfigFile(configFilename)
  if err != nil {
    fmt.Errorf("Couldn't read config file %s because: %#v", configFilename, err)
  }
  
  debug, _ := config.GetBool("default", "debug")
  host, _ := config.GetString("kafka", "host")
  port, _ := config.GetString("kafka", "port")
  maxSize, _ := config.GetInt64("kafka", "maxmessagesize")
  tempfilePath, _ := config.GetString("default", "filebufferpath")
  topicsRaw, _ := config.GetString("kafka", "topics")
  topics := strings.Split(topicsRaw, ",")
  for i, _ := range topics { topics[i] = strings.TrimSpace(topics[i]) }
  partitionsRaw, _ := config.GetString("kafka", "partitions")
  partitionStrings := strings.Split(partitionsRaw, ",")
  partitions := make([]int64, len(partitionStrings))
  for i, _ := range partitionStrings { partitions[i], _ = strconv.ParseInt(strings.TrimSpace(partitionStrings[i]),10,64) }

  hostname := fmt.Sprintf("%s:%s", host, port)
  
  topic := topics[0] // TMP FIXME
  partition := partitions[0] // TMP FIXME
  consumerForever := true // TMP FIXME
  
  fmt.Println("Consuming Messages :")
  fmt.Printf("From: %s, topic: %s, partition: %d\n", hostname, topic, partition)
  fmt.Println(" ---------------------- ")
  broker := kafka.NewBrokerConsumer(hostname, topic, int(partition), offset, uint32(maxSize))
  

  
  var payloadFile *os.File = nil
  payloadFile, err = ioutil.TempFile(tempfilePath, "kafka-s3-go-consumer-buffer")
  if err != nil {
    fmt.Println("Error opening file: ", err)
    payloadFile = nil
  }

  consumerCallback := func(msg *kafka.Message) {
    if debug {
      msg.Print()
    }
    if payloadFile != nil {
      payloadFile.Write([]byte(fmt.Sprintf("Message at: %d\n", msg.Offset())))
      payloadFile.Write(msg.Payload())
      payloadFile.Write([]byte("\n-------------------------------\n"))
    }
  }

  if consumerForever {
    quit := make(chan bool, 1)
    go func() {
      sigChan := make(chan os.Signal)
      signal.Notify(sigChan)
      for {
        sig := <-sigChan
        if sig == syscall.SIGINT {
          quit <- true
        }
      }
    }()

    msgChan := make(chan *kafka.Message)
    go broker.ConsumeOnChannel(msgChan, 10, quit)
    for msg := range msgChan {
      if msg != nil {
        consumerCallback(msg)
      } else {
        break
      }
    }
  } else {
    broker.Consume(consumerCallback)
  }

  if payloadFile != nil {
    payloadFile.Close()
  }

}