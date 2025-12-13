from FlinkRESTAPIMethods import *
import ruamel.yaml
import os
import subprocess


def main():
    base_url = "http://localhost:29999/"

    experimentFrequency = 3
    executionTimeSeconds = 30
    waitBetweenExecutionsSec = 15

    parallelism = [1,10,20,30]
    sync = True #VERY IMPORTANT!
    synchronicty = [75.0, 50.0, 100.0]
    # synchronicty = [123.45] #sync = false but boradcasting 100% tuples
    # synchronicty = [99.99]  # none (no braodcast) 0% data sharing
    gentype = "random"
    interWorkersDataSharing = "broadcast"

    conf_file = "/home/komal/Pycharm/FlinkViaRESTAPI/spatialdatagen-conf.yml"
    # outputFilePathAndName = "output/spatialFlinkExperiments_Issue_24_Redis_500k.csv"
    # logFilePathAndName = "logs/spatialFlinkExperiments_Issue24_Redis_500k.txt"
    # finalSinkOutputFile = "output/spatialFlinkExperimentsOutput_TSSide_Issue24_Redis_500k.csv"
    outputFilePathAndName = "output/spatialFlinkExperiments_Issue24_400k_sync2.csv"
    logFilePathAndName = "logs/spatialFlinkExperiments_Issue24_400k_sync2.txt"
    finalSinkOutputFile = "output/spatialFlinkExperimentsOutput_TSSide_Issue24_400k_sync2.csv"

    yaml = ruamel.yaml.YAML()
    # yaml.indent(mapping=4, sequence=4, offset=0)
    yaml.preserve_quotes = True
    yaml.width = 1000096  # do not wrap long lines in yml file

    subprocess.Popen("ssh aaic-shk-flink001 ./../../mnt/flink/flinkBinaries/flink-1.13.6/bin/stop-cluster.sh", shell=True,
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    time.sleep(15)  # wait before starting cluster again

    if not sync:
        print("\nWARNING: Sync value is set to false (ASYNC)")

    for p in parallelism:
        for s in synchronicty:
            with open(conf_file, 'r') as file:
                spatial_conf = yaml.load(file)

            spatial_conf['query']['mappedTrajectories']['trajStartEndSelectionApproach'] = gentype
            spatial_conf['parallelism'] = p
            spatial_conf['query']['mappedTrajectories']['syncPercentage'] = s
            spatial_conf['query']['mappedTrajectories']['sync'] = sync
            spatial_conf['query']['mappedTrajectories']['interWorkersDataSharing'] = interWorkersDataSharing
            print(spatial_conf['query']['mappedTrajectories']['trajStartEndSelectionApproach'])

            time.sleep(2)

            with open(conf_file, 'w') as file:
                yaml.dump(spatial_conf, file)

            time.sleep(2)
            # os.system('ls -l')

            # copy spatialdatagen-conf.yml to cluster
            os.system('scp /home/komal/Pycharm/FlinkViaRESTAPI/spatialdatagen-conf.yml   aaic-shk-flink001:/home/ubuntu/conf/spatialdatagen-conf.yml')

            time.sleep(2)

            executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, p, s,
                                  base_url, outputFilePathAndName, logFilePathAndName, finalSinkOutputFile);


def executeAndSaveLatency(experimentFrequency, executionTimeSeconds, waitBetweenExecutionsSec, parallelism,
                          synchronicty, base_url,
                          outputFilePathAndName, logFilePathAndName, finalSinkOutputFile):
    bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092"
    # bootStrapServers = "localhost:9092"
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
        subprocess.Popen("ssh aaic-shk-flink001 ./../../mnt/flink/flinkBinaries/flink-1.13.6/bin/start-cluster.sh", shell=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

        time.sleep(30)

        # uploadJar(base_url, "/home/komal/IdeaProjects/SpatialDataGen-TSsideCT/target/SpatialDataGen-0.1.jar")
        uploadJar(base_url, "/home/komal/IdeaProjects/SpatialDataGen-Issue24/target/SpatialDataGen-0.1.jar")

        time.sleep(5)
        x = getAllJars(base_url)
        jar_id = x.json()['files'][0]['id']

        parameters = {}

        x = submitJob(base_url, jar_id, parameters)



        if x.status_code == 200:
            print(
                str(datetime.now()) + " Job submitted! " + "Parallelism " + str(parallelism) + ", Synchronicty " + str(
                    synchronicty) + ", Frequency " + str(i))
            logFile.write(
                str(datetime.now()) + " Job submitted! " + "Parallelism " + str(parallelism) + ", Synchronicty " + str(
                    synchronicty) + ", Frequency " + str(i) + "\n \n")
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
        logFile.write(str(datetime.now()) + "Job Statuz: " + str(z.status_code) + ", " + z.text + "\n \n")

        actualExecutionDuration = 0
        for vertex in jsonTxt["vertices"]:
            actualExecutionDuration = json.dumps(vertex['duration'], indent=4)


        if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED":
            print("STATE FAILED")
            time.sleep(waitBetweenExecutionsSec)
            subprocess.Popen("ssh aaic-shk-flink001 ./../../mnt/flink/flinkBinaries/flink-1.13.6/bin/stop-cluster.sh", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            time.sleep(60)
            continue

        if int(actualExecutionDuration) < (((executionTimeSeconds - 10) * 1000)):
            print("EXECUTION TIME INSUFFICIENT")
            time.sleep(waitBetweenExecutionsSec)
            subprocess.Popen("ssh aaic-shk-flink001 ./../../mnt/flink/flinkBinaries/flink-1.13.6/bin/stop-cluster.sh", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            time.sleep(60)
            continue

        # if str(json.dumps(y.json()['state'], indent=4)).strip('\"') == "FAILED" or int(actualExecutionDuration) < (
        #         ((executionTimeSeconds - 10) * 1000)):
        #     print("FAILED OR EXECUTION TIME NOT SUFFICIENT")
        #     time.sleep(waitBetweenExecutionsSec)
        #     subprocess.Popen("ssh aaic-shk-flink001 ./flink/flink-1.13.6/bin/stop-cluster.sh", shell=True,
        #                      stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        #     time.sleep(60)
        #     continue

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
        subprocess.Popen("ssh aaic-shk-flink001 ./../../mnt/flink/flinkBinaries/flink-1.13.6/bin/stop-cluster.sh",
                         shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        time.sleep(40)  # wait before starting cluster again
        # incrementing loop variable
        i += 1

    # vertexStr = ""
    # for j in range(len(vertexNameList)):
    #     vertexStr = vertexStr + ", " + vertexNameList[j].replace(',', '-') + ", " + execDurationList[j] + ", " + \s
    #                 readRecordsList[j] + ", " + writeRecordsList[j]

    # print(vertexNameList)
    # print(len(vertexNameList))
    # print(len(readRecordsList))
    # print(readRecordsList)

    statsFile = openFile(outputFilePathAndName)
    statsFile.write("parallelism,synchronicty,expfrequency,vertexName,execDuration,readRecords,writeRecords,throughput" + "\n")

    # sinkFile = openFile(finalSinkOutputFile)
    # sinkFile.write("parallelism,synchronicty,expfrequency,vertexName,execDuration,readRecords,writeRecords,throughput" + "\n")

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
        statsFile.write(str(parallelism) + "," + str(synchronicty) + "," + vertexStr + "\n")

        # if (j + 1) % 6 == 0:
        #     sinkFile.write(str(parallelism) + "," + str(synchronicty) + "," + vertexStr + "\n")

        # print("Parallelism " + str(parallelism) + "," + vertexStr)

    statsFile.flush()
    statsFile.close()
    # sinkFile.flush()
    # sinkFile.close()
    logFile.flush()
    logFile.close()


if __name__ == "__main__":
    main()
