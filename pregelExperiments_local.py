import threading

from kafkaUtils import *
import ruamel.yaml
import subprocess
from throughputDaemon import *
from memoryDaemon import *

def main():

    bootStrapServers = "172.16.0.64:9092,172.16.0.81:9092"  #cluster
    # bootStrapServers = "localhost:9092"    #local
    latencyTopicPrefix = "latency-synth"
    parallelism = [20,10,1]

    idleness =  1
    windowSize = [5]
    stateExpirationTimer = 120
    sourceOffset = "latest" #"earliest" or "latest

    approach = ["async", "sync"]  #loop, async or sync
    maximumSuperStep = [60, 30]
    mode = "V-Mode"
    userComputeClass = "MaxValueComputeFunction"
    topicPrefix = "max-synth"
    rateLimiter = 150000000

    experimentFrequency = 1
    executionTimeSeconds = 60 * 10
    waitBetweenExecutionsSec = 20
    deploymentTime = (3) * 60


    conf_file_home = "/home/komal/PycharmProjects/FlinkRESTAPI/pregel.yml"
    # conf_file_dest = "localhost:/Users/komalmariam/conf/pregel.yml"
    conf_file_dest =  "aaic-shk-flink001:/home/ubuntu/conf/pregel.yml"

    base_url = "http://localhost:29999/"
    flinkBinaryPath = "aaic-shk-flink001 /mnt/flink/flinkBinaries/flink-1.20.2/"
    kafkaBinaryPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0/"
    genertorPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0"
    #kafkaBinaryPath = "localhost /Users/komalmariam/Kafka_Main/kafka_2.13-3.2.1/"
    jarFilePath = "/home/komal/IdeaProjects/FlinkPregel/target/FlinkPregel-1.0.jar"
    generatorJarPath = "/home/komal/IdeaProjects/SendGzipToKafka/target/SendGzipToKafka-1.0-jar-with-dependencies.jar"




    outputbaseName = f"Experiments"
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

                    modePrefix = mode.replace("-", "")
                    topic = f"{topicPrefix}-{modePrefix}-{a}-{p}-{ws}-{steps}"
                    latencyTopic = f"{latencyTopicPrefix}-{modePrefix}-{a}-{p}-{ws}-{steps}"

                    deleteKafkaTopic(topic, bootStrapServers, kafkaBinaryPath)
                    deleteKafkaTopic(latencyTopic, bootStrapServers, kafkaBinaryPath)
                    deleteKafkaTopic("messages", bootStrapServers, kafkaBinaryPath)
                    deleteKafkaTopic("output", bootStrapServers, kafkaBinaryPath)

                    createKafkaTopic(topic, p, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic(latencyTopic, p, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic("messages",p, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic("output",p, bootStrapServers, kafkaBinaryPath)


                    with open(latencyTopicListFile, "a") as topic_file:
                        topic_file.write(latencyTopic + "\n")

                    conf['pregel']['kafka']['inputTopic'] = topic
                    conf['pregel']['kafka']['parallelism'] = p
                    conf['pregel']['kafka']['kafkaBootStrapServers'] = bootStrapServers
                    conf['pregel']['kafka']['latencyTopic'] = latencyTopic

                    # if steps > 10:
                    #     idleness = steps
                    # elif steps == 10:
                    #     idleness = 16
                    # else:
                    #     idleness = 13


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
                                          base_url, jarFilePath, startCluster, stopCluster, outputFilePathAndName, logFilePathAndName,
                                          genertorPath, bootStrapServers, topic, latencyTopic, idleness, stateExpirationTimer,
                                          rateLimiter, generatorJarPath, deploymentTime)

                    # consumeLatencyTopic(latencyTopic,bootStrapServers,latencyFilePathAndName, a, p, ws, steps, experimentFrequency, mode)




def executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                          windowStep, approach, steps, base_url, jarFilePath, startCluster, stopCluster,
                          outputFilePathAndName, logFilePathAndName, sendToKafkaJarPath, bootStrapServers, topic,
                          latencyTopic, idleness, stateExpirationTimer, rateLimiter, generatorJarPath, deploymentTime):



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
            #logFile.write(str(datetime.now()) + "\n Job could not be submitted: " + x.text + "\n \n")
            exit(0)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')

        sourceDeploymentTime = 0
        sinkDeploymentTime = 0

        while True:
            y = getJobOverview(base_url, job_id)

            # Extract vertex list
            vertices = y.json().get('vertices', [])

            sourceDeploymentTime = vertices[0].get("duration",0)
            sinkDeploymentTime = vertices[-3].get("duration", 0)

            # Get the states for all vertices
            vertex_states = [v.get('status') for v in vertices]

            # check job status
            log_line = f"{datetime.now()} Job Status: {y.status_code}, Vertices: {vertex_states}\n"
            # print(log_line.strip())

            # Break only when *all* vertices are RUNNING
            if all(state in ("RUNNING", "FINISHED") for state in vertex_states):
                print("✅ All vertices are RUNNING or FINISHED. Job is fully started or completed.")
                logFile.write(
                    f"{datetime.now()} All vertices are RUNNING or FINISHED. Job is fully started or completed.\n\n")
                break

            # Otherwise, sleep and poll again
            time.sleep(1)

        print(str(datetime.now()) + " Job Status: " + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")

        time.sleep(deploymentTime)

        # collect source stats by polling
        pollRestAPIThread = threading.Thread(target=poll_stats_change_daemon,
            args=(base_url, job_id, outputFilePathAndName,parallelism, windowStep,
                  approach, steps, i,sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer, deploymentTime),daemon=True)

        pollRestAPIThread.start()

        pollMemoryThread = threading.Thread(target=poll_memory_daemon,
            args=(base_url, job_id, outputFilePathAndName,parallelism, windowStep,
                  approach, steps, i,sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer),daemon=True)

        pollMemoryThread.start()


        # Load kafka dataset
        load_kafka_dataset2(topic, bootStrapServers, "Synthetic_w263_e60_p100.jsonl.gz", sendToKafkaJarPath, generatorJarPath, rateLimiter)

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)
        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        # print(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")


        # terminate job
        z = terminateJob(base_url, job_id)
        #logFile.write(str(datetime.now()) + "Terminate Job Status: " + str(z.status_code) + ", " + z.text + "\n \n")
        time.sleep(waitBetweenExecutionsSec)
        deleteJar(base_url, jar_id)
        print("Running calculateLatencies jar ...")
        uploadJar(base_url, "/home/komal/IdeaProjects/calculateLatency/target/calculateLateny-1.0.jar")
        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']
        time.sleep(1)
        status = submitJob(base_url, jar_id, {"programArgs": f"{latencyTopic} awso"})

        if status.status_code == 200:
            print(str(datetime.now()) + " Latencies Job submitted! ")
        else:
            print(str(datetime.now()) + " Latencies job could not be submitted: " + status.text)
            logFile.write(str(datetime.now()) + "\n Job could not be submitted: " + status.text + "\n \n")
            exit(0)
        job_id = json.dumps(status.json()['jobid'], indent=4).replace('"', '')

        time.sleep(55)
        z = terminateJob(base_url, job_id)
        print("Finished calculating latencies for " + latencyTopic)


        #wait at-least waittime seconds before starting next job
        print("waiting for cluster to stop ....")
        time.sleep(waitBetweenExecutionsSec)

        # stop cluster
        subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        time.sleep(waitBetweenExecutionsSec)  # wait before starting cluster again
        #incrementing loop variable
        i += 1

    logFile.flush()
    logFile.close()



if __name__ == "__main__":
    main()
