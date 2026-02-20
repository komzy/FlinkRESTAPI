import subprocess, shlex, time
from FlinkRESTAPIMethods import *


def createKafkaTopic(topic, partitions, bootstrap_servers, kafkaBinaryPath):

    baseCommand = (
        f"bin/kafka-topics.sh --create --replication-factor 1 "
        f"--partitions {partitions} --topic {topic} --bootstrap-server {bootstrap_servers}"
    )

    createTopic = "ssh " + kafkaBinaryPath + baseCommand

    stdout_data, stderr_data = subprocess.Popen(createTopic, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    print("STDOUT:", stdout_data.decode())
    print("STDERR:", stderr_data.decode())
    print(f"created topic: {topic}")
    time.sleep(3)


def deleteKafkaTopic(topic, bootstrap_servers, kafkaBinaryPath):
    baseCommand = (f"bin/kafka-topics.sh --bootstrap-server {bootstrap_servers} --delete --topic {topic}");

    deleteTopic = "ssh " + kafkaBinaryPath + baseCommand

    stdout_data, stderr_data = subprocess.Popen(deleteTopic, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    print("STDOUT:", stdout_data.decode())
    print("STDERR:", stderr_data.decode())
    print(f"deleted topic: {topic}")
    time.sleep(3)



def load_kafka_dataset(topic, bootstrap_servers, sendToKafkaJarPath):
    # Split host and remote path
    parts = sendToKafkaJarPath.split(maxsplit=1)
    if len(parts) != 2:
        raise ValueError("sendToKafkaJarPath must be 'host remote_path'")
    host, remote_path = parts

    # Java command arguments
    java_args = [
        "java", "-jar",
        "-Dkafka.topic=" + topic,
        "-Dkafka.kafkaBootStrapServers=" + bootstrap_servers,
        "-Dkafka.dateFormat=yyyy-MM-dd'T'HH:mm:ss.SSS",
        "-Dkafka.nodeFilePath=cnr-2000-nodes.txt",
        "-Dkafka.scenarioFilePath=cnr-2000-edges.txt",
        "SendToFlinkPregel-1.0-jar-with-dependencies.jar"
    ]

    # Join Java command safely for SSH
    java_cmd = " ".join(shlex.quote(arg) for arg in java_args)
    ssh_cmd = f"ssh {shlex.quote(host)} 'cd {shlex.quote(remote_path)} && {java_cmd}'"

    print("Running command:", ssh_cmd)

    # Execute command
    result = subprocess.run(ssh_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Print outputs
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}")

    print("Finished loading data in topic:", topic)
    time.sleep(3)


def load_kafka_dataset2(topic, bootstrap_servers, datasetFileName, generatorPath, generatorJarPath, rateLimiter):
    # Split host and remote path
    parts = generatorPath.split(maxsplit=1)
    if len(parts) != 2:
        raise ValueError("sendToKafkaJarPath must be 'host remote_path'")
    host, remote_path = parts



    # copy conf to kafka cluster
    os.system("scp " + generatorJarPath + " " + "aaic-shk-kafka001:/home/ubuntu/kafka/kafka_2.12-2.4.0/SendGzipToKafka-1.0-jar-with-dependencies.jar")

    # Java command arguments
    java_args = [
        "java", "-jar",
        "-Dkafka.topic=" + topic,
        "-Dkafka.kafkaBootStrapServers=" + bootstrap_servers,
        "-Dkafka.gzipFile=" + datasetFileName,
        "-Dkafka.rateLimiter=" + str(rateLimiter),
        "SendGzipToKafka-1.0-jar-with-dependencies.jar"
    ]

    # Join Java command safely for SSH
    java_cmd = " ".join(shlex.quote(arg) for arg in java_args)
    ssh_cmd = f"ssh {shlex.quote(host)} 'cd {shlex.quote(remote_path)} && {java_cmd}'"

    print("Running command:", ssh_cmd)

    # Execute command
    result = subprocess.run(ssh_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Print outputs
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}")

    print("Finished loading data in topic:", topic)
    time.sleep(3)


