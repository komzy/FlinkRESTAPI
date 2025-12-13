from FlinkRESTAPIMethods import *
import ruamel.yaml
import os
import subprocess
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable
import sys



def main():

    # bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092"  #cluster
    bootStrapServers = "localhost:9092"    #local
    latencyTopic = "latency"
    parallelism = [1, 10, 20,30]

    idleness = 10  # should be greater or twice than windowSize
    windowSize = [5, 10, 15]
    stateExpirationTimer = 4
    sourceOffset = "earliest" #"earliest" or "latest

    approach = ["loop", "async", "sync"]  #loop, async or sync
    maximumSuperStep = [1,5,10,15]
    mode = "V-mode"
    userComputeClass = "MaxValueComputeFunction"

    conf_file_home = "/home/komal/Pycharm/FlinkViaRESTAPI/pregel.yml"
    conf_file_dest = "aaic-shk-flink001:/home/ubuntu/conf/pregel.yml"

    base_url = "http://localhost:29999/"
    flinkBinaryPath = "aaic-shk-flink001 ./../../mnt/flink/flinkBinaries/flink-1.13.6/bin/"
    kafkaZookeeperPath = "aaic-shk-kafka001 ./../../mnt/kafka/kafkaBinaries/kafka-1.13.6/bin/"
    jarFilePath = "/home/komal/IdeaProjects/SpatialDataGen-Issue24/target/SpatialDataGen-0.1.jar"
    experimentFrequency = 5
    executionTimeSeconds = 30
    waitBetweenExecutionsSec = 15


    outputbaseName = "pregelExperiments"
    outputFilePathAndName = "output/" + outputbaseName + ".csv"
    logFilePathAndName = "logs/" + outputbaseName + ".txt"
    latencyFilePathAndName = "output/" + outputbaseName + "_latency.csv"


    yaml = ruamel.yaml.YAML()
    # yaml.indent(mapping=4, sequence=4, offset=0)
    yaml.preserve_quotes = True
    yaml.width = 1000096  # do not wrap long lines in yml file
    stopCluster = "ssh " + flinkBinaryPath + "stop-cluster.sh"
    startCluster = "ssh " + flinkBinaryPath + "start-cluster.sh"

    subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    time.sleep(waitBetweenExecutionsSec)  # wait before starting cluster again

    for p in parallelism:
        for ws in windowSize:
            for a in approach:
                for steps in maximumSuperStep:
                    with open(conf_file_home, 'r') as file:
                        conf = yaml.load(file)


                    latencyTopic = f"{latencyTopic}-{a}-{p}-{ws}-{steps}"
                    createKafkaLatencyTopic(latencyTopic,bootStrapServers,kafkaZookeeperPath)
                    check_topic_exists(latencyTopic,bootStrapServers)

                    conf['pregel']['kafka']['parallelism'] = p
                    conf['pregel']['kafka']['kafkaBootStrapServers'] = bootStrapServers
                    conf['pregel']['kafka']['latency'] = latencyTopic
                    conf['pregel']['flink']['idleness'] = idleness
                    conf['pregel']['flink']['windowSize'] = ws
                    conf['pregel']['flink']['windowSlide'] = ws
                    conf['pregel']['flink']['stateExpirationTimer'] = stateExpirationTimer
                    conf['pregel']['flink']['sourceOffset'] = sourceOffset
                    conf['pregel']['app']['approach'] = a
                    conf['pregel']['app']['maximumSuperStep'] = steps
                    conf['pregel']['app']['mode'] = mode
                    conf['pregel']['app']['userComputeClass'] = userComputeClass

                    time.sleep(1)

                    with open(conf_file_home, 'w') as file:
                        yaml.dump(conf, file)

                    time.sleep(1)
                    # os.system('ls -l')

                    # copy conf to cluster
                    os.system("scp " + conf_file_home + " " + conf_file_dest)

                    time.sleep(2)

                    executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, p, ws, a, steps,
                                          base_url, jarFilePath, startCluster, stopCluster, outputFilePathAndName, logFilePathAndName)

                    consumeLatencyTopic(latencyTopic,bootStrapServers,latencyFilePathAndName)




def executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                          windowStep, approach, steps, base_url, jarFilePath, startCluster, stopCluster,
                          outputFilePathAndName, logFilePathAndName):

    executionCostList = []
    numberRecordList = []
    vertexNameList = []
    execDurationList = []
    readRecordsList = []
    writeRecordsList = []
    expfrequencyList = []

    executionCostList.clear()
    numberRecordList.clear()

    logFile = openFile(logFilePathAndName)

    i = 0
    while i < experimentFrequency:

        # start cluster
        subprocess.Popen(startCluster, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()


        time.sleep(30)

        uploadJar(base_url, jarFilePath)

        time.sleep(5)

        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']

        parameters = {}

        x = submitJob(base_url, jar_id, parameters)



        if x.status_code == 200:
            print(
                str(datetime.now()) + " Job submitted! " + "Parallelism " + str(parallelism) + ", windowStep/Slide " + str(
                    windowStep) + ", Approach " + str(approach) + ", Steps " + str(steps) + ", Frequency " + str(i))
            logFile.write(
                str(datetime.now()) + " Job submitted! " + "Parallelism " + str(
                    parallelism) + ", windowStep/Slide " + str(windowStep) + ", Approach " + str(approach) +
                ", Steps " + str(steps) + ", Frequency " + str(i) + "\n \n")
        else:
            # print(str(datetime.now()) + " Job could not be submitted: " + x.text)
            logFile.write(str(datetime.now()) + "\n Job could not be submitted: " + x.text + "\n \n")

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        # print(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")

        while str(json.dumps(y.json()['vertices'][-1]['metrics']['write-records-complete'], indent=4)) != "true":
            time.sleep(1)
            y = getJobOverview(base_url, job_id)
            # print(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text)
            logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")

        jsonTxt = json.loads(y.text)

        z = terminateJob(base_url, job_id)
        # print(str(datetime.now()) + "Job Statuz: " + str(z.status_code) + ", " + z.text)
        logFile.write(str(datetime.now()) + "Terminate Job Status: " + str(z.status_code) + ", " + z.text + "\n \n")

        actualExecutionDuration = 0
        for vertex in jsonTxt["vertices"]:
            actualExecutionDuration = json.dumps(vertex['duration'], indent=4)


        if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED":
            print("STATE FAILED")
            time.sleep(waitBetweenExecutionsSec)
            subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            time.sleep(60)
            continue

        if int(actualExecutionDuration) < (((executionTimeSeconds - 10) * 1000)):
            print("EXECUTION TIME INSUFFICIENT")
            time.sleep(waitBetweenExecutionsSec)
            subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            time.sleep(60)
            continue


        for vertex in jsonTxt["vertices"]:
            vertexNameList.append(json.dumps(vertex['name'], indent=4))
            execDurationList.append(json.dumps(vertex['duration'], indent=4))
            readRecordsList.append(json.dumps(vertex['metrics']['read-records'], indent=4))
            writeRecordsList.append(json.dumps(vertex['metrics']['write-records'], indent=4))
            expfrequencyList.append(i)

        while str(json.dumps(y.json()['state'], indent=4)).strip('\"') != "CANCELED":
            time.sleep(10)
            terminateJob(base_url, job_id)  # why again?
            y = getJobOverview(base_url, job_id)
            # print(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text)
            logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")
            # print(str(datetime.now()) + "Job Status: " + str(json.dumps(y.json()['state'], indent=4)))
            logFile.write(str(datetime.now()) + "Job Status: " + str(json.dumps(y.json()['state'], indent=4)) + "\n \n")
            if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED":
                break

        # wait at-least waitBetweenExecutionsSec seconds before starting next job
        print("waiting for cluster to stop ....")
        time.sleep(waitBetweenExecutionsSec)

        # stop cluster
        subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        time.sleep(40)  # wait before starting cluster again
        # incrementing loop variable
        i += 1


    # print(vertexNameList)
    # print(len(vertexNameList))
    # print(len(readRecordsList))
    # print(readRecordsList)

    statsFile = openFile(outputFilePathAndName)
    statsFile.write("parallelism,windowStep,approach,steps,expfrequency,vertexName,execDuration,readRecords,writeRecords,throughput" + "\n")


    for j in range(len(vertexNameList)):

        execDuration = ""
        readRecords = ""
        writeRecords = ""
        vertexName = ""
        expfrequency = ""
        throughput = ""

        if j != len(vertexNameList) - 1:
            vertexName = vertexName + str(vertexNameList[j].replace(',', '-'))  # + ","
            execDuration = execDuration + str(execDurationList[j])  # + ","
            readRecords = readRecords + str(readRecordsList[j])  # + ","
            writeRecords = writeRecords + str(writeRecordsList[j])  # + ","
            expfrequency = expfrequency + str(expfrequencyList[j])

        else:
            vertexName = vertexName + str(vertexNameList[j].replace(',', '-'))
            execDuration = execDuration + str(execDurationList[j])
            readRecords = readRecords + str(readRecordsList[j])
            writeRecords = writeRecords + str(writeRecordsList[j])
            expfrequency = expfrequency + str(expfrequencyList[j])

        if readRecordsList[j] != 0:
            throughput = throughput + str(int(readRecordsList[j])/(int(execDurationList[j])/1000))


        vertexStr = expfrequency + ", " + vertexName + ", " + execDuration + ", " + readRecords + ", " + writeRecords + ", " + throughput
        statsFile.write(str(parallelism) + "," + str(windowStep) + "," + str(approach) + "," + str(steps) +
                        "," + vertexStr + "\n")

    statsFile.flush()
    statsFile.close()
    logFile.flush()
    logFile.close()


def check_topic_exists(topic, bootstrap_servers):
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = admin.list_topics()
        if topic not in topics:
            raise ValueError(f"Topic '{topic}' does not exist.")
        admin.close()
    except NoBrokersAvailable:
        raise ConnectionError(f"Kafka broker not available at {bootstrap_servers}")
    except KafkaError as e:
        raise RuntimeError(f"Kafka error: {str(e)}")

def createKafkaLatencyTopic(topic, bootstrap_servers, kafka_binary_path):

    baseCommand =  "bin/kafka-topics.sh --create --replication-factor 1 --partitions 60 --topic " + topic + "--bootstrap-server " + bootstrap_servers
    createTopic = "ssh " + kafka_binary_path + " " + baseCommand
    subprocess.Popen(createTopic, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    time.sleep(3)



def consumeLatencyTopic(latencyTopic, bootStrapServers, latencyFilePathAndName, approach, parallelism, windowStep,
                        steps,expfrequency):

    print(f"Computing Avg. Latency value from '{latencyTopic}'...\n")

    check_topic_exists(latencyTopic,bootStrapServers)
    # Create a KafkaConsumer instance
    consumer = KafkaConsumer(
        latencyTopic,
        bootstrap_servers=[bootStrapServers],
        auto_offset_reset='earliest',  # Start consuming from the beginning of the topic
        value_deserializer=lambda x: int(x.decode('utf-8'))  # Deserialize byte string to integer
    )

    latencyFile = openFile(latencyFilePathAndName)
    latencyFile.write("topic,approach,parallelism,windowStep,steps,frequency,elements,average" + "\n")

    count = 0
    total = 0
    average = 0

    # Consume messages
    for message in consumer:
        latency_value = message.value
        count += 1
        total += latency_value
        average = total / count

    latencyStr = latencyTopic + "," + approach + "," + parallelism + "," + windowStep + "," + steps + "," + expfrequency + "," + count + "," + average
    latencyFile.write(latencyStr + "\n")

    latencyFile.flush()
    latencyFile.close()
    consumer.close()



if __name__ == "__main__":
    main()
