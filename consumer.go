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
const ONE_MINUTE_IN_NANOS = 60000000000

func init() {
  flag.StringVar(&configFilename, "c", "conf.properties", "path to config file")
	flag.StringVar(&offsetsRaw, "o", "0", "comma separated offsets to start consuming from for each topic")
	flag.BoolVar(&keepBufferFiles, "k", false, "keep buffer files around for inspection")
}


type ChunkBuffer struct {
  File            *os.File
  FilePath        *string
  MaxAgeInMins    int64
  MaxSizeInBytes  int64
  Topic           *string
  Partition       int64
  Offset          int64
  expiresAt       int64
  length          int64
}

func (chunkBuffer *ChunkBuffer) BaseFilename() string {
  return fmt.Sprintf("kafka-s3-go-consumer-buffer-topic_%s-partition_%d-offset_%d-", *chunkBuffer.Topic, chunkBuffer.Partition, chunkBuffer.Offset)
}

func (chunkBuffer *ChunkBuffer) CreateBufferFileOrPanic() {
  tmpfile, err := ioutil.TempFile(*chunkBuffer.FilePath, chunkBuffer.BaseFilename())
  chunkBuffer.File = tmpfile
  chunkBuffer.expiresAt = time.Now().UnixNano() + (chunkBuffer.MaxAgeInMins * ONE_MINUTE_IN_NANOS)
  chunkBuffer.length = 0
  if err != nil {
    fmt.Errorf("Error opening buffer file: %#v\n", err)
    panic(err)
  }
}

func (chunkBuffer *ChunkBuffer) TooBig() bool {
  return chunkBuffer.length >= chunkBuffer.MaxSizeInBytes
}

func (chunkBuffer *ChunkBuffer) TooOld() bool {
  return time.Now().UnixNano() >= chunkBuffer.expiresAt
}

func (chunkBuffer *ChunkBuffer) NeedsRotation() bool {
  return chunkBuffer.TooBig() || chunkBuffer.TooOld()
}

func (chunkBuffer *ChunkBuffer) PutMessage(msg *kafka.Message) {
  uuid := []byte(fmt.Sprintf("t_%s-p_%d-o_%d|", *chunkBuffer.Topic, chunkBuffer.Partition, msg.Offset()))
  lf := []byte("\n")
  chunkBuffer.Offset = msg.Offset()
  chunkBuffer.File.Write(uuid)
  chunkBuffer.File.Write(msg.Payload())
  chunkBuffer.File.Write(lf)

  chunkBuffer.length += int64(len(uuid)) + int64(len(msg.Payload())) + int64(len(lf))
}

func (chunkBuffer *ChunkBuffer) StoreToS3AndRelease(s3bucket *s3.Bucket) (bool, error) {
  var s3path string
  var err error
  
  if debug {
    fmt.Printf("Closing bufferfile: %s\n", chunkBuffer.File.Name())
  }
  chunkBuffer.File.Close()
  
  contents, err := ioutil.ReadFile(chunkBuffer.File.Name())
  if err != nil {
    return false, err
  }
  
  if len(contents) <= 0 {
    if debug {
      fmt.Printf("Nothing to store to s3 for bufferfile: %s\n", chunkBuffer.File.Name())
    }
  } else {  // Write to s3 in a new filename
    alreadyExists := true
    for alreadyExists {
      s3path = fmt.Sprintf("%s/p%d/%d", *chunkBuffer.Topic, chunkBuffer.Partition, time.Now().UnixNano())
      alreadyExists, err = s3bucket.Exists(s3path)
      if err != nil {
        panic(err)
        return false, err
      }
    } 

    if debug {
      fmt.Printf("Going to write to s3: %s.s3.amazonaws.com/%s with mimetype:%s\n", s3bucket.Name, s3path, mime.TypeByExtension(filepath.Ext(chunkBuffer.File.Name())))
    }
    
    err = s3bucket.Put(s3path, contents, mime.TypeByExtension(filepath.Ext(chunkBuffer.File.Name())), s3.Private)
    if err != nil {
      panic(err)
    }
  }
  
  if !keepBufferFiles {
    if debug {
      fmt.Printf("Deleting bufferfile: %s\n", chunkBuffer.File.Name())
    }
    err = os.Remove(chunkBuffer.File.Name())
    if err != nil {
      fmt.Errorf("Error deleting bufferfile %s: %#v", chunkBuffer.File.Name(), err)
    }
  }
  
  return true, nil
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
  bufferMaxSizeInByes, _ := config.GetInt64("default", "maxchunksizebytes")
  bufferMaxAgeInMinutes, _ := config.GetInt64("default", "maxchunkagemins")
  port, _ := config.GetString("kafka", "port")
  hostname := fmt.Sprintf("%s:%s", host, port)
  awsKey, _ := config.GetString("s3", "accesskey")
  awsSecret, _ := config.GetString("s3", "secretkey")
  awsRegion, _ := config.GetString("s3", "region")
  s3BucketName, _ := config.GetString("s3", "bucket")
  s3bucket := s3.New(aws.Auth{awsKey, awsSecret}, aws.Regions[awsRegion]).Bucket(s3BucketName)

  if debug {
    fmt.Printf("Config: bucketName=%s and s3bucket.Name=%s\n", s3BucketName, s3bucket.Name)
  }
  
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
    fmt.Printf("Making sure chunkbuffer directory structure exists at %s\n", tempfilePath)
  }
  err = os.MkdirAll(tempfilePath, 0700)
  if err != nil {
    fmt.Errorf("Error ensuring chunkbuffer directory structure %s: %#v\n", tempfilePath, err)
    panic(err)
  }
  
  if debug {
    fmt.Printf("Watching %d topics, opening a chunkbuffer for each.\n", len(topics))
  }
  buffers := make([]*ChunkBuffer, len(topics))
  for i, _ := range topics {
    buffers[i] = &ChunkBuffer{FilePath: &tempfilePath, 
      MaxSizeInBytes: bufferMaxSizeInByes, 
      MaxAgeInMins: bufferMaxAgeInMinutes, 
      Topic: &topics[i], 
      Partition: partitions[i],
      Offset: offsets[i],
    }
    buffers[i].CreateBufferFileOrPanic()
    if debug {
      fmt.Printf("Consumer[%s#%d][chunkbuffer]: %s\n", hostname, i, buffers[i].File.Name())
    }
  }
  
  
  if debug {
    fmt.Printf("Setting up a broker for each of the %d topics.\n", len(topics))
  }
  brokers := make([]*kafka.BrokerConsumer, len(topics))
  for i, _ := range partitionStrings { 
    if debug {
      fmt.Printf("Consumer[%s#%d][broker]: { topic: %s, partition: %d, offset: %d, maxMessageSize: %d }\n", 
        hostname, 
        i,
        topics[i], 
        partitions[i], 
        offsets[i], 
        maxSize,
      )
    }
    brokers[i] = kafka.NewBrokerConsumer(hostname, topics[i], int(partitions[i]), uint64(offsets[i]), uint32(maxSize)) 
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
  
  if debug {
    fmt.Printf("Brokers created, quit signal listeners initialized, starting to listen with %d brokers...\n", len(brokers))
  }
  for i, broker := range brokers {
    messageChannel := make(chan *kafka.Message)
    go broker.ConsumeOnChannel(messageChannel, 10, quitSignals)
    for msg := range messageChannel {
      if msg != nil {
        if debug {
          fmt.Printf("`%s` { ", topics[i])
          msg.Print()
          fmt.Printf("}\n")
        }
        buffers[i].PutMessage(msg)
        
        // check for max size and max age ... if over, rotate
        // to new buffer file and upload the old one.
        if buffers[i].NeedsRotation()  {
          rotatedOutBuffer := buffers[i]

          if debug {
            fmt.Printf("Broker#%d: Log Rotation needed! Rotating out of %s\n", i, rotatedOutBuffer.File.Name())
          }
            
          buffers[i] = &ChunkBuffer{FilePath: &tempfilePath, 
            MaxSizeInBytes: bufferMaxSizeInByes, 
            MaxAgeInMins: bufferMaxAgeInMinutes, 
            Topic: &topics[i], 
            Partition: partitions[i],
            Offset: msg.Offset(),
          }
          buffers[i].CreateBufferFileOrPanic()

          if debug {
            fmt.Printf("Broker#%d: Rotating into %s\n", i, buffers[i].File.Name())
          }

          go rotatedOutBuffer.StoreToS3AndRelease(s3bucket)
        }
        
      } else {
        break
      }
    }
    
    // buffer stopped, let's clean up nicely
    buffers[i].StoreToS3AndRelease(s3bucket)
  }
}