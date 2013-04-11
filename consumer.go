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
  "time"
  "mime"
  "path/filepath"
  
  configfile "github.com/crowdmob/goconfig"
  "github.com/crowdmob/goamz/aws"
  "github.com/crowdmob/goamz/s3"
)

var configFilename string
var offsetsRaw string
var keepBufferFiles bool
var debug bool

func init() {
  flag.StringVar(&configFilename, "c", "conf.properties", "path to config file")
	flag.StringVar(&offsetsRaw, "o", "0", "comma separated offsets to start consuming from for each topic")
	flag.BoolVar(&keepBufferFiles, "k", false, "keep buffer files around for inspection")
}

func saveToS3(s3bucket *s3.Bucket, bufferFile *os.File, topic *string, partition int64) (bool, error) {
  var s3path string
  var err error
  
  
  contents, err := ioutil.ReadFile(bufferFile.Name())
  if err != nil {
    return false, err
  }
  
  if len(contents) <= 0 {
    if debug {
      fmt.Printf("Nothing to store to s3 for: %s\n", bufferFile.Name())
    }
    return true, nil
  }
  
  for alreadyExists {
    s3path = fmt.Sprintf("%s/p%d/%d", *topic, partition, time.Now().UnixNano())
    alreadyExists, err = s3bucket.Exists(s3path)
    // if err != nil {
    //   panic(err)
    //   return false, err
    // }
  } 

  alreadyExists := true
  if debug {
    fmt.Printf("Going to write to s3: %s//%s\n", s3bucket.Name, s3path)
  }
  err = s3bucket.Put(s3path, contents, mime.TypeByExtension(filepath.Ext(bufferFile.Name())), s3.Private)
  if err != nil {
    panic(err)
  }
  return (err != nil), err
}

func main() {
  // Read argv
  flag.Parse()
  config, err := configfile.ReadConfigFile(configFilename)
  if err != nil {
    fmt.Errorf("Couldn't read config file %s because: %#v\n", configFilename, err)
    panic(err)
  }
  
  // Read configuration file
  host, _ := config.GetString("kafka", "host")
  debug, _ = config.GetBool("default", "debug")
  port, _ := config.GetString("kafka", "port")
  hostname := fmt.Sprintf("%s:%s", host, port)
  awsKey, _ := config.GetString("s3", "accesskey")
  awsSecret, _ := config.GetString("s3", "secretkey")
  awsRegion, _ := config.GetString("s3", "region")
  s3BucketName, _ := config.GetString("s3", "bucket")
  s3bucket := s3.New(aws.Auth{awsKey, awsSecret}, aws.Regions[awsRegion]).Bucket(s3BucketName)
  maxSize, _ := config.GetInt64("kafka", "maxmessagesize")
  tempfilePath, _ := config.GetString("default", "filebufferpath")
  topicsRaw, _ := config.GetString("kafka", "topics")
  topics := strings.Split(topicsRaw, ",")
  for i, _ := range topics { topics[i] = strings.TrimSpace(topics[i]) }
  partitionsRaw, _ := config.GetString("kafka", "partitions")
  partitionStrings := strings.Split(partitionsRaw, ",")
  partitions := make([]int64, len(partitionStrings))
  for i, _ := range partitionStrings { partitions[i], _ = strconv.ParseInt(strings.TrimSpace(partitionStrings[i]),10,64) }
  offsetStrings := strings.Split(offsetsRaw, ",")
  offsets := make([]int64, len(topics))
  for i, _ := range topics { 
    if i >= len(offsetStrings) {
      offsets[i] = 0
    } else {
      offsets[i], _ = strconv.ParseInt(strings.TrimSpace(offsetStrings[i]),10,64)
    }
  }
  
  if debug {
    fmt.Printf("Read %d topics, setting up a consumer for each.\n", len(topics))
  }
  brokers := make([]*kafka.BrokerConsumer, len(topics))
  for i, _ := range partitionStrings { 
    if debug {
      fmt.Printf("Consumer[%s #%d]:: topic: %s, partition: %d, offset: %d, maxMessageSize: %d\n", hostname, i, topics[i], partitions[i], offsets[i], maxSize)
    }
    brokers[i] = kafka.NewBrokerConsumer(hostname, topics[i], int(partitions[i]), uint64(offsets[i]), uint32(maxSize)) 
  }
  
  if debug {
    fmt.Printf("Making sure bufferfile path exists at %s\n", tempfilePath)
  }
  err = os.MkdirAll(tempfilePath, 0700)
  if err != nil {
    fmt.Errorf("Error ensuring buffer file path %s: %#v\n", tempfilePath, err)
    panic(err)
  }
  
  if debug {
    fmt.Printf("Created %d brokers, opening a buffer file for each.\n", len(brokers))
  }
  buffers := make([]*os.File, len(brokers))
  for i, _ := range brokers {
    bufferFilename := fmt.Sprintf("kafka-s3-go-consumer-buffer-topic_%s-partition_%d-offset_%d-", topics[i], partitions[i], offsets[i])
    buffers[i], err = ioutil.TempFile(tempfilePath, bufferFilename)
    if err != nil {
      fmt.Errorf("Error opening buffer file: %#v\n", err)
      panic(err)
    }
    if debug {
      fmt.Printf("Consumer[%s #%d]:: buffer-file: %s\n", hostname, i, buffers[i].Name())
    }
  }

  quitSignals := make(chan bool, len(brokers))
  for _ = range brokers {
    go func() { // setup quit notifiers for SIGINT
      signalChannel := make(chan os.Signal)
      signal.Notify(signalChannel)
      for {
        sig := <-signalChannel
        if sig == syscall.SIGINT {
          quitSignals <- true
        }
      }
    }()
  }

  for i, broker := range brokers {
    messageChannel := make(chan *kafka.Message)
    go broker.ConsumeOnChannel(messageChannel, 10, quitSignals)
    for msg := range messageChannel {
      if msg != nil {
        if debug {
          fmt.Printf("`%s` } ", topics[i])
          msg.Print()
        }
        buffers[i].Write(msg.Payload())
        buffers[i].Write([]byte("\n"))
      } else {
        break
      }
    }
  }


  cleanDoneSignals := make(chan bool, len(buffers))
  for i, bufferFile := range buffers {
    go func() {
      if debug {
        fmt.Printf("Closing buffer-file: %s\n", bufferFile.Name())
      }
      bufferFile.Close()
    
      // Write anything remaining to s3
      saveToS3(s3bucket, bufferFile, &topics[i], partitions[i])
    
      if !keepBufferFiles {
        if debug {
          fmt.Printf("Deleting buffer-file: %s\n", bufferFile.Name())
        }
        err = os.Remove(bufferFile.Name())
        if err != nil {
          fmt.Errorf("Error deleting buffer file %s: %#v", bufferFile.Name(), err)
        }
      }

      cleanDoneSignals <- true
    }()
  
    // wait for all cleanup
    <-cleanDoneSignals
  }
  

}