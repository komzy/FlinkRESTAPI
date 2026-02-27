from FlinkRESTAPIMethods import *
import json
import time


def poll_stats_change_daemon(base_url, job_id, outputFilePathAndName, parallelism, windowStep, approach, steps, expFrequency,
                             sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer, deploymentTime):
    print("polling source stats...")

    prevValues = [0,0]

    with open(outputFilePathAndName, "w") as statsFile:
        # Write header once
        statsFile.write(
            "parallelism,windowStep,approach,steps,expfrequency,idleness,edges,inputDuration,nodes,outputDuration,inRate,outRate,minusidleInRate,minusidleOutRate\n"
        )

        while True:
             prevValues = read_stats(statsFile, base_url, job_id, parallelism, windowStep, approach,
                       steps, expFrequency,sourceDeploymentTime, sinkDeploymentTime, idleness, prevValues, stateExpirationTimer, deploymentTime)
             statsFile.flush()  # Ensure data is written immediately
             time.sleep(1)




def read_stats(statsFile, base_url, job_id, parallelism, windowStep, approach,
               steps, expFrequency, sourceDeploymentTime, sinkDeploymentTime, idleness, prevValues, stateExpirationTimer, deploymentTime):

    response = getJobOverview(base_url, job_id)
    jsonTxt = json.loads(response.text)

    prevEdges = prevValues[0]
    prevNodes = prevValues[1]

    vertices = jsonTxt.get("vertices", [])

    sourceVertex = vertices[0]
    sinkVertex = vertices[-3]
    idleness_ms = idleness * 1000
    if approach == "loop":
        idleness_ms = 0
    deploymentTime_ms = deploymentTime * 1000

    # initialize variables
    inputDuration = 0
    inRate = 0
    minusidleInRate = 0
    outputDuration = 0
    outRate = 0
    minusidleOutRate = 0

    execDurationSource = int(sourceVertex.get("duration", 0))
    edges = int(sourceVertex.get("metrics", {}).get("write-records", 0))

    execDurationSink = int(sinkVertex.get("duration", 0))
    nodes = int(sinkVertex.get("metrics", {}).get("write-records", 0))

    if edges <= prevEdges and  nodes <= prevNodes:
        return prevValues

    if edges > prevEdges:
        inputDuration = execDurationSource - sourceDeploymentTime
        inRate = edges / (inputDuration / 1000) if inputDuration > 0 else 0
        inputDurationIdle = inputDuration - idleness_ms - deploymentTime_ms
        minusidleInRate = edges / (inputDurationIdle / 1000) if inputDurationIdle > 0 else 0

    if nodes > prevNodes:
        outputDuration = execDurationSink - sinkDeploymentTime
        outputDurationIdle = outputDuration - idleness_ms - deploymentTime_ms
        if approach == "async":
            outputDurationIdle = outputDurationIdle - ((stateExpirationTimer - 2)* 1000)

        outRate = edges / (outputDuration / 1000) if outputDuration > 0 else 0
        minusidleOutRate = edges / (outputDurationIdle / 1000) if outputDurationIdle > 0 else 0

    row = (f"{parallelism},{windowStep},{approach},{steps},{expFrequency},"
           f"{idleness},{edges},{inputDuration},{nodes},{outputDuration},"
           f"{inRate},{outRate},{minusidleInRate},{minusidleOutRate}")

    statsFile.write(row + "\n")


    return [edges,nodes]



