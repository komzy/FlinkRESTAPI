import threading

from kafkaUtils import *
import ruamel.yaml
import subprocess
from throughputDaemon import *
from memoryDaemon import *

def main():

    #App (Flink Job) Configuration - parameters.yml
    bootStrapServers = "172.16.0.64:9092,172.16.0.81:9092"  #cluster
    # bootStrapServers = "localhost:9092"    #local
    topicPrefix = "max"
    outputTopicPrefix = "latency"
    parallelism = [20,10,1]
    idleness = 1
    windowSize = [5]
    windowSlide = [5]
    stateExpirationTimer = 120
    sourceOffset = "latest" #"earliest" or "latest
    query = "MaxValueComputeFunction"
    approach = ["naive", "inc"]


    #Input Generator rate
    rateLimiter = 150000000

    #Experiment parameters
    experimentFrequency = 1
    executionTimeSeconds = 60 * 10
    waitBetweenExecutionsSec = 20
    deploymentTime = (3) * 60  #pause between flink pipeline deployment and input data generation

    #paths
    conf_file_home = "/home/komal/PycharmProjects/FlinkRESTAPI/parameters.yml" #path your system
    conf_file_dest = "aaic-shk-flink001:/home/ubuntu/conf/parameters.yml" #path on cluster


    flinkBinaryPath = "aaic-shk-flink001 /mnt/flink/flinkBinaries/flink-1.20.2/"
    kafkaBinaryPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0/"
    genertorPath = "aaic-shk-kafka001 /home/ubuntu/kafka/kafka_2.12-2.4.0"  #generator jar on kafka cluster
    jarFilePath = "/home/komal/IdeaProjects/FlinkPregel/target/FlinkPregel-1.0.jar" #flink job jar
    generatorJarPath = "/home/komal/IdeaProjects/SendGzipToKafka/target/SendGzipToKafka-1.0-jar-with-dependencies.jar" #on your system

    outputbaseName = f"Throughput"

    yaml = ruamel.yaml.YAML()
    yaml.preserve_quotes = True
    yaml.width = 1000096  # do not wrap long lines in yml file
    stopCluster = "ssh " + flinkBinaryPath + "bin/stop-cluster.sh"
    startCluster = "ssh " + flinkBinaryPath + "bin/start-cluster.sh"

    subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    time.sleep(20)  # wait before starting cluster again

    for p in parallelism:
        for wSize in windowSize:
            for wSlide in windowSlide:
                for a in approach:
                    with open(conf_file_home, 'r') as file:
                        conf = yaml.load(file)
                    print("yaml loaded successfully")

                    topic = f"{topicPrefix}-{a}-{p}p-{wSize}wSize-{wSlide}wSlide"
                    outputTopic = f"{outputTopicPrefix}-{a}-{p}p-{wSize}wSize-{wSlide}wSlide"

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

                    time.sleep(1)

                    with open(conf_file_home, 'w') as file:
                        yaml.dump(conf, file)

                    time.sleep(1)

                    # copy conf to cluster
                    os.system("scp " + conf_file_home + " " + conf_file_dest)
                    time.sleep(2)



                    executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, p, wSize, wSlide, a,
                                          jarFilePath, startCluster,stopCluster, outputbaseName, genertorPath, bootStrapServers, topic, outputTopic,
                                          idleness, stateExpirationTimer, rateLimiter, generatorJarPath, deploymentTime)


def executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                          wSize, wSlide, approach, jarFilePath, startCluster,stopCluster, outputbaseName, sendToKafkaJarPath, bootStrapServers, topic,
                          outputTopic, idleness, stateExpirationTimer, rateLimiter, generatorJarPath, deploymentTime):

    outputFilePathAndName = f"output/{outputbaseName}-{a}-{p}p-{wSize}wSize-{wSlide}wSlide.csv"
    logFilePathAndName = f"logs/{outputbaseName}-{a}-{p}p-{wSize}wSize-{wSlide}wSlide.csv"

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

        time.sleep(3)

        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']

        x = submitJob(base_url, jar_id, {})
        if x.status_code == 200:
            status =f"{datetime.now()} Job submitted! {outputTopic} Parallelism: {parallelism} windowStep: {wSize} windowSlide: {wSlide}" \
            f"Approach: {approach} Frequency: {i}";
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
            sinkDeploymentTime = vertices[-3].get("duration", 0)

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

        steps = "var"

        # collect source stats by polling
        pollThroughputThread = threading.Thread(target=poll_throughput_daemon,
            args=(base_url, job_id, outputFilePathAndName,parallelism, wSlide,wSize,
                  approach, steps, i,sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer, deploymentTime),daemon=True)

        pollThroughputThread.start()

        pollMemoryThread = threading.Thread(target=poll_memory_daemon,
            args=(base_url, job_id, outputFilePathAndName,parallelism, wSlide,wSize,
                  approach, i,sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer),daemon=True)

        pollMemoryThread.start()

        pollVerticesThread = threading.Thread(target=poll_vertices_daemon,
            args=(base_url, job_id, outputFilePathAndName,parallelism, wSlide,wSize,
                  approach, i,sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer),daemon=True)

        pollVerticesThread.start()


        # Load kafka dataset
        load_kafka_dataset2(topic, bootStrapServers, "Synthetic_w263_e60_p100.jsonl.gz", sendToKafkaJarPath, generatorJarPath, rateLimiter)

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
        time.sleep(1)
        status = submitJob(base_url, jar_id, {"programArgs": f"{outputTopic} awso"})

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
        time.sleep(20)

        # stop cluster
        subprocess.Popen(stopCluster, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        time.sleep(waitBetweenExecutionsSec)  # wait before starting cluster again
        #incrementing loop variable
        i += 1

    logFile.flush()
    logFile.close()



if __name__ == "__main__":
    main()
