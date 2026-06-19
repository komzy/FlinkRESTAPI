from FlinkRESTAPIMethods import *
import json
import time


def poll_throughput_daemon(base_url, job_id, outputFilePathAndName, parallelism, wSlide,wSize, approach, expFrequency,
                             sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTime, deploymentTime):
    print("polling source stats...")

    prevValues = [0,0, None]

    with open(outputFilePathAndName, "w") as statsFile:
        # Write header once
        statsFile.write(
            "parallelism,wSlide,wSize,approach,expfrequency,idleness,edges,outputDuration,outRate,rec\n"
        )

        while True:
             prevValues = read_stats(statsFile, base_url, job_id, parallelism, wSlide,wSize, approach,
                        expFrequency,sourceDeploymentTime, sinkDeploymentTime, idleness, prevValues, stateExpirationTime, deploymentTime)
             statsFile.flush()  # Ensure data is written immediately



def read_stats(statsFile, base_url, job_id, parallelism, wSlide,wSize, approach,
               expFrequency, sourceDeploymentTime, sinkDeploymentTime, idleness, prevValues,
               stateExpirationTime, deploymentTime):

    response = getJobOverview(base_url, job_id)
    jsonTxt = json.loads(response.text)

    prevEdges = prevValues[0]
    prevNodes = prevValues[1]
    waitTime = prevValues[2]

    vertices = jsonTxt.get("vertices", [])

    sourceVertex = vertices[1]
    sinkVertex = vertices[-1]
    idleness_ms = idleness * 1000
    if approach == "loop":
        idleness_ms = 0
    deploymentTime_ms = deploymentTime * 1000

    # initialize variables
    outputDuration = 0
    outRate = 0
    minusidleOutRate = 0

    execDurationSource = int(sourceVertex.get("duration", 0))
    edges = int(sourceVertex.get("metrics", {}).get("write-records", 0))

    execDurationSink = int(sinkVertex.get("duration", 0))
    nodes = int(sinkVertex.get("metrics", {}).get("read-records", 0))

    # Save waitTime only the first time edges become > 0
    if waitTime is None and edges > 0:
        waitTime = execDurationSink

    if edges <= prevEdges and nodes <= prevNodes:
        return prevValues

    if nodes > prevNodes:
        outputDuration = execDurationSink - waitTime       #time until sink status turns to running
        outputDurationIdle = outputDuration - (stateExpirationTime * 1000); #subtract waittime and state expiration time
        if approach == "flinkWin":
            outputDurationIdle = outputDuration - idleness_ms
        if outputDuration > 0:
            outRate = edges / (outputDuration / 1000)
            minusidleOutRate = edges / (outputDurationIdle / 1000)

        row = (f"{parallelism},{wSlide},{wSize},{approach},{expFrequency},"
               f"{idleness},{edges},{outputDuration},"
               f"{outRate},{nodes}")

        statsFile.write(row + "\n")


    return [edges,nodes, waitTime]



