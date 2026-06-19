from FlinkRESTAPIMethods import *
import json
import time


def poll_memory_daemon(base_url, job_id, outputFilePathAndName, parallelism, wSlide,wSize, approach, expFrequency, sourceDeploymentTime, sinkDeploymentTime, idleness):
    print("polling source memory stats...")

    prevValues = [0,0]
    outputFilePathAndName=outputFilePathAndName.replace("throughput", "memory")

    with open(outputFilePathAndName, "w") as statsFile:
        # Write header once
        statsFile.write(
            "parallelism,wSlide,wSize,approach,expfrequency,"
            "outputDuration,TotalJVMHeap,AvgJVMHeap,sourceBackPressure,sinkBackPressure,sourceBusyTime,sinkBusyTime""\n"
        )

        while True:
             prevValues = memory_stats(statsFile, base_url, job_id, parallelism, wSlide,wSize, approach,
                        expFrequency, sinkDeploymentTime, prevValues)
             statsFile.flush()  # Ensure data is written immediately
             time.sleep(1)


def memory_stats(statsFile, base_url, job_id, parallelism, wSlide,wSize, approach,
               expFrequency, sinkDeploymentTime, prevValues):

    response = getJobOverview(base_url, job_id)
    jsonTxt = json.loads(response.text)

    prevEdges = prevValues[0]
    prevNodes = prevValues[1]

    vertices = jsonTxt.get("vertices", [])

    sourceVertex = vertices[1]
    sinkVertex = vertices[-3]

    # initialize variables
    outputDuration = 0

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

    if nodes > prevNodes:
        outputDuration = execDurationSink - sinkDeploymentTime

    metrics = getTaskManagerMetrics(base_url,getTaskManagers(base_url))
    total_heap = sum_heap_used(metrics)
    average_heap = avg_heap_used(metrics)

    row = (f"{parallelism},{wSlide},{wSize},{approach},{expFrequency},{outputDuration},"
           f"{total_heap},{average_heap},{sourceBackPressure},"
           f"{sinkBackPressure},{sourceBusyTime},{sinkBusyTime}")

    statsFile.write(row + "\n")


    return [edges,nodes]
