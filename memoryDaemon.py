from FlinkRESTAPIMethods import *
import json
import time


def poll_memory_daemon(base_url, job_id, outputFilePathAndName, parallelism, windowStep, approach, steps, expFrequency, sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer):
    print("polling source memory stats...")

    prevValues = [0,0]
    outputFilePathAndName=outputFilePathAndName.replace("output", "memory")

    with open(outputFilePathAndName, "w") as statsFile:
        # Write header once
        statsFile.write(
            "parallelism,{wSlide,wSize},approach,steps,expfrequency,idleness,edges,inputDuration,nodes,"
            "outputDuration,TotalJVMHeap,AvgJVMHeap,sourceBackPressure,sinkBackPressure,sourceBusyTime,sinkBusyTime""\n"
        )

        while True:
             prevValues = memory_stats(statsFile, base_url, job_id, parallelism, windowStep, approach,
                       steps, expFrequency,sourceDeploymentTime, sinkDeploymentTime, idleness, prevValues, stateExpirationTimer)
             statsFile.flush()  # Ensure data is written immediately
             time.sleep(1)




def memory_stats(statsFile, base_url, job_id, parallelism, windowStep, approach,
               steps, expFrequency, sourceDeploymentTime, sinkDeploymentTime, idleness, prevValues, stateExpirationTimer):

    response = getJobOverview(base_url, job_id)
    jsonTxt = json.loads(response.text)

    prevEdges = prevValues[0]
    prevNodes = prevValues[1]

    vertices = jsonTxt.get("vertices", [])

    sourceVertex = vertices[0]
    sinkVertex = vertices[-3]

    # initialize variables
    inputDuration = 0
    outputDuration = 0


    execDurationSource = int(sourceVertex.get("duration", 0))
    edges = int(sourceVertex.get("metrics", {}).get("write-records", 0))
    sourceBackPressure = int(sourceVertex.get("accumulated-backpressured-time", 0))
    sourceBusyTime = int(sourceVertex.get("accumulated-busy-time", 0))

    execDurationSink = int(sinkVertex.get("duration", 0))
    nodes = int(sinkVertex.get("metrics", {}).get("write-records", 0))
    sinkBackPressure = int(sinkVertex.get("accumulated-backpressured-time", 0))
    sinkBusyTime = int(sinkVertex.get("accumulated-busy-time", 0))


    # do not write if prev values same
    if edges <= prevEdges and  nodes <= prevNodes:
        return prevValues

    if edges > prevEdges:
        inputDuration = execDurationSource - sourceDeploymentTime

    if nodes > prevNodes:
        outputDuration = execDurationSink - sinkDeploymentTime

    metrics = getTaskManagerMetrics(base_url,getTaskManagers(base_url))
    total_heap = sum_heap_used(metrics)
    average_heap = avg_heap_used(metrics)

    row = (f"{parallelism},{windowStep},{approach},{steps},{expFrequency},"
           f"{idleness},{edges},{inputDuration},{nodes},{outputDuration},"
           f"{total_heap},{average_heap},{sourceBackPressure},"
           f"{sinkBackPressure},{sourceBusyTime},{sinkBusyTime}")

    statsFile.write(row + "\n")


    return [edges,nodes]



