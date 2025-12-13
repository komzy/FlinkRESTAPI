from FlinkRESTAPIMethods import *
from kafkaUtils import *
import ruamel.yaml
import os
import subprocess



def main():

    bootStrapServers = "172.16.0.64:9092,172.16.0.81:9092"  #cluster
    # bootStrapServers = "localhost:9092"    #local
    latencyTopicPrefix = "latency"
    parallelism = [30]

    idleness = 60  # should be greater or twice than windowSize
    windowSize = [15]
    stateExpirationTimer = 50
    sourceOffset = "latest" #"earliest" or "latest

    approach = ["sync", "loop", "async"]  #loop, async or sync
    maximumSuperStep = [1, 10, 20, 30, 40, 50]
    mode = "V-Mode"
    userComputeClass = "MaxValueComputeFunction"
    topicPrefix = "max"

    conf_file_home = "/home/komal/PycharmProjects/FlinkRESTAPI/pregel.yml"
    # conf_file_dest = "localhost:/Users/komalmariam/conf/pregel.yml"
    conf_file_dest =  "aaic-shk-flink001:/home/ubuntu/conf/pregel.yml"

    base_url = "http://localhost:29999/"
    flinkBinaryPath = "aaic-shk-flink001 /mnt/flink/flinkBinaries/flink-1.20.2/"
    kafkaBinaryPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0/"
    sendToKafkaJarPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0"
    #kafkaBinaryPath = "localhost /Users/komalmariam/Kafka_Main/kafka_2.13-3.2.1/"
    jarFilePath = "/home/komal/IdeaProjects/FlinkPregel/target/FlinkPregel-1.0.jar"
    experimentFrequency = 1
    executionTimeSeconds =  60 * 4
    waitBetweenExecutionsSec = 20

    outputbaseName = "loopExperiments"
    latencyFilePathAndName = "output/" + outputbaseName + "_latency.csv"
    latencyTopicListFile = "output/latency_topics.txt"

    yaml = ruamel.yaml.YAML()
    # yaml.indent(mapping=4, sequence=4, offset=0)
    yaml.preserve_quotes = True
    yaml.width = 1000096  # do not wrap long lines in yml file
    stopCluster = "ssh " + flinkBinaryPath + "bin/stop-cluster.sh"
    startCluster = "ssh " + flinkBinaryPath + "bin/start-cluster.sh"

    subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    time.sleep(20)  # wait before starting cluster again

    for p in parallelism:
        for ws in windowSize:
            for a in approach:
                for steps in maximumSuperStep:
                    with open(conf_file_home, 'r') as file:
                        conf = yaml.load(file)
                    print("yaml loaded successfully")

                    deleteKafkaTopic("messages",  bootStrapServers, kafkaBinaryPath)
                    deleteKafkaTopic("output", bootStrapServers, kafkaBinaryPath)

                    modePrefix = mode.replace("-", "")
                    topic = f"{topicPrefix}-{modePrefix}-{a}-{p}-{ws}-{steps}"
                    latencyTopic = f"{latencyTopicPrefix}-{modePrefix}-{a}-{p}-{ws}-{steps}"

                    createKafkaTopic(topic, 10, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic("messages",10, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic("output",10, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic(latencyTopic,10,bootStrapServers,kafkaBinaryPath)

                    with open(latencyTopicListFile, "a") as topic_file:
                        topic_file.write(latencyTopic + "\n")

                    conf['pregel']['kafka']['inputTopic'] = topic
                    conf['pregel']['kafka']['parallelism'] = p
                    conf['pregel']['kafka']['kafkaBootStrapServers'] = bootStrapServers
                    conf['pregel']['kafka']['latencyTopic'] = latencyTopic
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

                    # copy conf to cluster
                    os.system("scp " + conf_file_home + " " + conf_file_dest)

                    time.sleep(2)

                    outputFilePathAndName = f"output/{outputbaseName}_{modePrefix}_{a}_{p}p_{ws}ws_{steps}steps.csv"
                    logFilePathAndName = f"logs/{outputbaseName}_{modePrefix}_{a}_{p}p_{ws}ws_{steps}steps.csv"

                    executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, p, ws, a, steps,
                                          base_url, jarFilePath, startCluster, stopCluster, outputFilePathAndName, logFilePathAndName, sendToKafkaJarPath, bootStrapServers, topic, latencyTopic)

                    # consumeLatencyTopic(latencyTopic,bootStrapServers,latencyFilePathAndName, a, p, ws, steps, experimentFrequency, mode)




def executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                          windowStep, approach, steps, base_url, jarFilePath, startCluster, stopCluster,
                          outputFilePathAndName, logFilePathAndName, sendToKafkaJarPath, bootStrapServers, topic, latencyTopic):


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
        print("Cluster Started..")

        time.sleep(30)

        uploadJar(base_url, jarFilePath)
        print("jar uploaded..")

        time.sleep(5)

        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']
        # print("jar ID: " + jar_id)


        x = submitJob(base_url, jar_id, {})


        if x.status_code == 200:
            print(
                str(datetime.now()) + " Job submitted! " + latencyTopic + " Parallelism " + str(parallelism) + ", windowStep/Slide " + str(
                    windowStep) + ", Approach " + str(approach) + ", Steps " + str(steps) + ", Frequency " + str(i))
            logFile.write(
                str(datetime.now()) + " Job submitted! "+ latencyTopic + "Parallelism " + str(
                    parallelism) + ", windowStep/Slide " + str(windowStep) + ", Approach " + str(approach) +
                ", Steps " + str(steps) + ", Frequency " + str(i) + "\n \n")
        else:
            print(str(datetime.now()) + " Job could not be submitted: " + x.text)
            logFile.write(str(datetime.now()) + "\n Job could not be submitted: " + x.text + "\n \n")
            exit(0)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')

        while True:
            y = getJobOverview(base_url, job_id)

            # Extract vertex list
            vertices = y.json().get('vertices', [])

            # Get the states for all vertices
            vertex_states = [v.get('status') for v in vertices]

            # Log status
            log_line = f"{datetime.now()} Job Status: {y.status_code}, Vertices: {vertex_states}\n"
            print(log_line.strip())
            logFile.write(log_line + "\n")

            # Break only when *all* vertices are RUNNING
            if all(state in ("RUNNING", "FINISHED") for state in vertex_states):
                print("✅ All vertices are RUNNING or FINISHED. Job is fully started or completed.")
                logFile.write(
                    f"{datetime.now()} All vertices are RUNNING or FINISHED. Job is fully started or completed.\n\n")
                break

            # Otherwise, sleep and poll again
            # time.sleep(3)

        print(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")

        # Load kafka dataset
        load_kafka_dataset2(topic, bootStrapServers, sendToKafkaJarPath )

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
        # terminate job
        z = terminateJob(base_url, job_id)
        # # print(str(datetime.now()) + "Job Statuz: " + str(z.status_code) + ", " + z.text)
        logFile.write(str(datetime.now()) + "Terminate Job Status: " + str(z.status_code) + ", " + z.text + "\n \n")
        time.sleep(waitBetweenExecutionsSec)
        deleteJar(base_url, jar_id)
        print("Running calculateLatencies jar ...")
        uploadJar(base_url, "/home/komal/IdeaProjects/calculateLatency/target/calculateLateny-1.0.jar")
        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']
        time.sleep(1)
        status = submitJob(base_url, jar_id, {"programArgs": f"{latencyTopic} aws"})

        if status.status_code == 200:
            print(str(datetime.now()) + " Latencies Job submitted! ")
        else:
            print(str(datetime.now()) + " Latencies job could not be submitted: " + status.text)
            logFile.write(str(datetime.now()) + "\n Job could not be submitted: " + status.text + "\n \n")
            exit(0)
        job_id = json.dumps(status.json()['jobid'], indent=4).replace('"', '')

        time.sleep(90)
        z = terminateJob(base_url, job_id)
        print("Finished calculating latencies for " + latencyTopic)


        for vertex in jsonTxt["vertices"]:
            vertexNameList.append(json.dumps(vertex['name'], indent=4))
            execDurationList.append(json.dumps(vertex['duration'], indent=4))
            readRecordsList.append(json.dumps(vertex['metrics']['read-records'], indent=4))
            writeRecordsList.append(json.dumps(vertex['metrics']['write-records'], indent=4))
            expfrequencyList.append(i)

        #wait at-least waittime seconds before starting next job
        print("waiting for cluster to stop ....")
        time.sleep(waitBetweenExecutionsSec)

        # stop cluster
        subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        time.sleep(waitBetweenExecutionsSec)  # wait before starting cluster again
        #incrementing loop variable
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


if __name__ == "__main__":
    main()
