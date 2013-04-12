/*

Author: Matthew Moore, CrowdMob Inc.

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
var keepBufferFiles bool
var debug bool
const ONE_MINUTE_IN_NANOS = 60000000000

func init() {
  flag.StringVar(&configFilename, "c", "conf.properties", "path to config file")
	flag.BoolVar(&keepBufferFiles, "k", false, "keep buffer files around for inspection")
}


type ChunkBuffer struct {
  File            *os.File
  FilePath        *string
  MaxAgeInMins    int64
  MaxSizeInBytes  int64
  Topic           *string
  Partition       int64
  Offset          uint64
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

func S3TopicPartitionPrefix(topic *string, partition int64) string {
  return fmt.Sprintf("%s/p%d/", *topic, partition)
}

func KafkaMsgGuidPrefix(topic *string, partition int64) string {
  return fmt.Sprintf("t_%s-p_%d-o_", *topic, partition)
}

func (chunkBuffer *ChunkBuffer) PutMessage(msg *kafka.Message) {
  uuid := []byte(fmt.Sprintf("%s%d|", KafkaMsgGuidPrefix(chunkBuffer.Topic, chunkBuffer.Partition), msg.Offset()))
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
      s3path = fmt.Sprintf("%s%d", S3TopicPartitionPrefix(chunkBuffer.Topic, chunkBuffer.Partition), time.Now().UnixNano())
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

func LastS3KeyWithPrefix(bucket *s3.Bucket, prefix *string) (string, error) {
  keyMarker := ""
  lastKey := ""
  moreResults := true
  for moreResults {
    results, err := bucket.List(*prefix, "", keyMarker, 0)
    if err != nil { return lastKey, err }
    
    if len(results.Contents) == 0 { // empty request, return last found lastKey
      return lastKey, nil
    }
    
    lastKey = results.Contents[len(results.Contents)-1].Key
    moreResults = results.IsTruncated
  }
  return lastKey, nil
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

  maxSize, _ := config.GetInt64("kafka", "maxmessagesize")
  tempfilePath, _ := config.GetString("default", "filebufferpath")
  topicsRaw, _ := config.GetString("kafka", "topics")
  topics := strings.Split(topicsRaw, ",")
  for i, _ := range topics { topics[i] = strings.TrimSpace(topics[i]) }
  partitionsRaw, _ := config.GetString("kafka", "partitions")
  partitionStrings := strings.Split(partitionsRaw, ",")
  partitions := make([]int64, len(partitionStrings))
  for i, _ := range partitionStrings { partitions[i], _ = strconv.ParseInt(strings.TrimSpace(partitionStrings[i]),10,64) }

  // Fetch Offsets from S3 (look for last written file and guid)
  if debug {
    fmt.Printf("Fetching offsets for each topic from s3 bucket %s ...\n", s3bucket.Name)
  }
  offsets := make([]uint64, len(topics))
  for i, _ := range offsets {
    prefix := S3TopicPartitionPrefix(&topics[i], partitions[i])
    if debug {
      fmt.Printf("  Looking at %s object versions: ", prefix)
    }
    latestKey, err := LastS3KeyWithPrefix(s3bucket, &prefix)
    if err != nil { panic(err) }

    if debug {
      fmt.Printf("Got: %#v\n", latestKey)
    }
    
    if len(latestKey) == 0 { // no keys found, there aren't any files written, so start at 0 offset
      offsets[i] = 0
      if debug {
        fmt.Printf("  No s3 object found, assuming Offset:%d\n", offsets[i])
      }
    } else { // if a key was found we have to open the object and find the last offset
      if debug {
        fmt.Printf("  Found s3 object %s, got: ", latestKey)
      }
      contentBytes, err := s3bucket.Get(latestKey)
      guidPrefix := KafkaMsgGuidPrefix(&topics[i], partitions[i])
      lines := strings.Split(string(contentBytes), "\n")
      for l := len(lines)-1; l <= 0; l-- {
        if strings.HasPrefix(lines[l], guidPrefix) { // found a line with a guid, extract offset and escape out
          guidSplits := strings.SplitN(strings.SplitN(lines[l], "|", 2)[0], guidPrefix, 2)
          offsetString := guidSplits[len(guidSplits)-1]
          offsets[i], err = strconv.ParseUint(offsetString, 10, 64)
          if err != nil {
            panic (err)
          }
          if debug {
            fmt.Printf("OffsetString:%s(L#%d), Offset:%d\n", offsetString, l, offsets[i])
          }
          break
        }
      }
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