from FlinkRESTAPIMethods import *
from kafkaUtils import *
import ruamel.yaml
import os
import subprocess
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable
import sys


bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092"  #cluster
# bootStrapServers = "localhost:9092"    #local
latencyTopic = "latency"
parallelism = [30]

idleness = 10  # should be greater or twice than windowSize
windowSize = [5]
stateExpirationTimer = 50
sourceOffset = "earliest" #"earliest" or "latest

approach = ["sync"]  #loop, async or sync
maximumSuperStep = [60]
mode = "V-Mode"
userComputeClass = "MaxValueComputeFunction"

conf_file_home = "/home/komal/PycharmProjects/FlinkRESTAPI/pregel.yml"
# conf_file_dest = "localhost:/Users/komalmariam/conf/pregel.yml"
conf_file_dest =  "aaic-shk-flink001:/home/ubuntu/conf/pregel.yml"

base_url = "http://localhost:29999/"
flinkBinaryPath = "aaic-shk-flink001 /mnt/flink/flinkBinaries/flink-1.20.2/"
kafkaBinaryPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0/"
#kafkaBinaryPath = "localhost /Users/komalmariam/Kafka_Main/kafka_2.13-3.2.1/"
jarFilePath = "/home/komal/IdeaProjects/FlinkPregel/target/FlinkPregel-1.0.jar"
experimentFrequency = 2
executionTimeSeconds = 8 * 60
waitBetweenExecutionsSec = 30

outputbaseName = "loopExperiments"
latencyFilePathAndName = "output/" + outputbaseName + "_latency.csv"
latencyTopicListFile = "output/latency_topics.txt"

yaml = ruamel.yaml.YAML()
# yaml.indent(mapping=4, sequence=4, offset=0)
yaml.preserve_quotes = True
yaml.width = 1000096  # do not wrap long lines in yml file
stopCluster = "ssh " + flinkBinaryPath + "bin/stop-cluster.sh"
startCluster = "ssh " + flinkBinaryPath + "bin/start-cluster.sh"

# topic = "latency-loop-10-5-60"

check_topic_exists(topic, bootStrapServers)