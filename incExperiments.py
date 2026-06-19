import threading

from kafkaUtils import *
import ruamel.yaml
import subprocess
from throughputDaemon import *
from memoryDaemon import *
from verticeRecordsDaemon import poll_vertices_daemon


def main():

    #App (Flink Job) Configuration - parameters.yml
    bootStrapServers = "172.16.0.64:9092,172.16.0.81:9092"  #cluster
    # bootStrapServers = "localhost:9092"    #local
    topicPrefix = "testCC"
    outputTopicPrefix = "testCCoutput"
    parallelism = [30]
    idleness = 1
    windowSize = [5]
    windowSlide = [5]
    stateExpirationTimer = 21
    sourceOffset = "latest" #"earliest" or "latest
    query = "CC"
    approach = ["inc"]

    #Input Generator rate
    rateLimiter = 150000000.0

    #Experiment parameters
    experimentFrequency = 1
    executionTimeSeconds = 60 * 1.5
    waitBetweenExecutionsSec = 10
    deploymentTime = 3  #pause between flink pipeline deployment and input data generation

    #paths
    conf_file_home = "/home/komal/PycharmProjects/FlinkRESTAPI/parameters_streamingGraph.yml" #path your system
    conf_file_dest = "aaic-shk-flink001:/home/ubuntu/conf/parameters.yml" #path on cluster

    generatorConf_home = "/home/komal/PycharmProjects/FlinkRESTAPI/parameters_generator.yml" #path your system
    generatorConf_dest = "aaic-shk-kafka001:/home/ubuntu/kafka/kafka_2.12-2.4.0/conf/parameters.yml" #path on cluster


    flinkBinaryPath = "aaic-shk-flink001 /mnt/flink/flinkBinaries/flink-1.20.2/"
    kafkaBinaryPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0/"
    jarFilePath = "/home/komal/IdeaProjects/StreamingGraph/target/StreamingGraph-1.0.jar" #flink job jar on your system
    generatorJarPath = "aaic-shk-kafka001:/home/ubuntu/kafka/kafka_2.12-2.4.0/SendGzipToKafka-1.0-jar-with-dependencies.jar" #generator jar on cluster



    yaml = ruamel.yaml.YAML()
    yaml.preserve_quotes = True
    yaml.width = 1000096  # do not wrap long lines in yml file
    stopCluster = "ssh " + flinkBinaryPath + "bin/stop-cluster.sh"
    startCluster = "ssh " + flinkBinaryPath + "bin/start-cluster.sh"

    subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    time.sleep(10)  # wait before starting cluster again

    for p in parallelism:
        for wSize in windowSize:
            for wSlide in windowSlide:
                for a in approach:
                    with open(conf_file_home, 'r') as file:
                        conf = yaml.load(file)
                    print("yaml loaded successfully")

                    topic = f"{topicPrefix}-{a}-{p}p-{wSize}wsz-{wSlide}wsl"
                    outputTopic = f"{outputTopicPrefix}-{a}-{p}p-{wSize}wsz-{wSlide}wsl"

                    deleteKafkaTopic(topic, bootStrapServers, kafkaBinaryPath)
                    deleteKafkaTopic(outputTopic, bootStrapServers, kafkaBinaryPath)
                    deleteKafkaTopic("messages", bootStrapServers, kafkaBinaryPath)

                    createKafkaTopic(topic, p, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic(outputTopic, p, bootStrapServers, kafkaBinaryPath)
                    createKafkaTopic("messages",p, bootStrapServers, kafkaBinaryPath)

                    conf['parameters']['kafka']['kafkaBootStrapServers'] = bootStrapServers
                    conf['parameters']['kafka']['inputTopic'] = topic
                    conf['parameters']['kafka']['outputTopic'] = outputTopic
                    conf['parameters']['kafka']['parallelism'] = p
                    conf['parameters']['flink']['idleness'] = idleness
                    conf['parameters']['flink']['windowSize'] = wSize
                    conf['parameters']['flink']['windowSlide'] = wSlide
                    conf['parameters']['flink']['stateExpirationTimer'] = stateExpirationTimer
                    conf['parameters']['flink']['sourceOffset'] = sourceOffset
                    conf['parameters']['app']['query'] = query
                    conf['parameters']['app']['approach'] = a

                    with open(conf_file_home, 'w') as file:
                        yaml.dump(conf, file)

                    # copy conf to cluster
                    subprocess.run(["scp", conf_file_home, conf_file_dest], check=True)

                    executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, p, wSize, wSlide, a,
                                          jarFilePath, startCluster,stopCluster, topicPrefix, bootStrapServers, topic, outputTopic,
                                          idleness, stateExpirationTimer, rateLimiter, generatorJarPath, deploymentTime, generatorConf_home,generatorConf_dest)


def executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                          wSize, wSlide, approach, jarFilePath, startCluster,stopCluster, outputbaseName, bootStrapServers, topic,
                          outputTopic, idleness, stateExpirationTimer, rateLimiter, generatorJarPath, deploymentTime, generatorConf_home,generatorConf_dest):

    outputFilePathAndName = f"throughput/{outputbaseName}-{approach}-{parallelism}p-{wSize}wsz-{wSlide}wsl.csv"
    logFilePathAndName = f"logs/{outputbaseName}-{approach}-{parallelism}p-{wSize}wsz-{wSlide}wsl.csv"

    base_url = "http://localhost:29999/"

    logFile = openFile(logFilePathAndName)

    i = 0
    while i < experimentFrequency:

        # start cluster
        subprocess.Popen(startCluster, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        print("Cluster Started..")
        time.sleep(20)

        uploadJar(base_url, jarFilePath)
        print("Flink job jar uploaded..")

        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']

        x = submitJob(base_url, jar_id, {})
        if x.status_code == 200:
            status =f"{datetime.now()} Job submitted! {outputTopic} Parallelism: {parallelism} windowStep: {wSize} windowSlide: {wSlide} " \
            f"Approach: {approach} Frequency: {i}"
            print(status)
            logFile.write(status + "\n \n")
        else:
            print(str(datetime.now()) + " Job could not be submitted: " + x.text)
            exit(0)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')

        sourceDeploymentTime = 0
        sinkDeploymentTime = 0

        while True:
            y = getJobOverview(base_url, job_id)

            # Extract vertex list
            vertices = y.json().get('vertices', [])

            sourceDeploymentTime = vertices[1].get("duration",0)
            sinkDeploymentTime = vertices[-1].get("duration", 0)

            # Get the states for all vertices
            vertex_states = [v.get('status') for v in vertices]

            # Break only when *all* vertices are RUNNING
            if all(state in ("RUNNING", "FINISHED") for state in vertex_states):
                print("All vertices are RUNNING or FINISHED. Job is fully started or completed.")
                logFile.write(f"{datetime.now()} All vertices are RUNNING or FINISHED. Job is fully started or completed.\n\n")
                break

            # Otherwise, sleep and poll again
            time.sleep(1)

        print(str(datetime.now()) + " Job Status: " + str(y.status_code) + ", " + y.text)
        logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")

        time.sleep(deploymentTime)     #wait until pipeline turns blue


        # collect source stats by polling
        pollThroughputThread = threading.Thread(target=poll_throughput_daemon,
            args=(base_url, job_id, outputFilePathAndName,parallelism, wSlide,wSize,
                  approach, i,sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer, deploymentTime),daemon=True)

        pollThroughputThread.start()

        pollMemoryThread = threading.Thread(target=poll_memory_daemon,
            args=(base_url, job_id, outputFilePathAndName,parallelism, wSlide,wSize,
                  approach, i,sourceDeploymentTime, sinkDeploymentTime, idleness),daemon=True)

        pollMemoryThread.start()
        #
        pollVerticesThread = threading.Thread(target=poll_vertices_daemon,
            args=(base_url, job_id, outputFilePathAndName),daemon=True)

        pollVerticesThread.start()

        # Load kafka dataset
        load_kafka_data(topic,bootStrapServers,generatorJarPath,generatorConf_home,generatorConf_dest,"bitcoinAlpha",rateLimiter)

        # Execute for executionTimeSeconds
        time.sleep(executionTimeSeconds)

        job_id = json.dumps(x.json()['jobid'], indent=4).replace('"', '')
        y = getJobOverview(base_url, job_id)
        logFile.write(str(datetime.now()) + "Job Status: " + str(y.status_code) + ", " + y.text + "\n \n")

        # terminate job
        z = terminateJob(base_url, job_id)
        logFile.write(str(datetime.now()) + "Terminate Job Status: " + str(z.status_code) + ", " + z.text + "\n \n")
        time.sleep(waitBetweenExecutionsSec)
        deleteJar(base_url, jar_id)

        print("Running calculateLatencies jar ...")

        uploadJar(base_url, "/home/komal/IdeaProjects/calculateLatency/target/calculateLateny-1.0.jar")
        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']
        status = submitJob(base_url, jar_id, {"programArgs": f"{outputTopic}"})

        if status.status_code == 200:
            print(str(datetime.now()) + " Latencies Job submitted! ")
        else:
            print(str(datetime.now()) + " Latencies job could not be submitted: " + status.text)
            logFile.write(str(datetime.now()) + "\n Job could not be submitted: " + status.text + "\n \n")
            exit(0)
        job_id = json.dumps(status.json()['jobid'], indent=4).replace('"', '')

        time.sleep(60)
        z = terminateJob(base_url, job_id)
        print("Finished calculating latencies for " + outputTopic)
        time.sleep(5)
        # stop cluster
        print("waiting for cluster to stop ....")
        subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        time.sleep(10)  # wait before starting cluster again
        # #incrementing loop variable
        i += 1

    logFile.flush()
    logFile.close()



if __name__ == "__main__":
    main()
