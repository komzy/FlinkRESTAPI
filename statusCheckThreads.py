
from FlinkRESTAPIMethods import *

def poll_stats_change_daemon(base_url, job_id, outputFilePathAndName, parallelism, windowStep, approach, steps, expFrequency, pipelineDeploymentDelay):
    print("polling source stats for throughput...")

    previous_write_records = 0  # store last value

    with open(outputFilePathAndName, "w") as statsFile:
        # Write header once
        statsFile.write(
            "parallelism,windowStep,approach,steps,expfrequency,"
            "vertexName,execPipelineDuration,withoutDeploymentDelayDuration,"
            "readRecords,writeRecords,inputRateIdleness,outputRateIdleness,inputRate,outputRate\n"
        )

        while True:
            previous_write_records = write_source_stats(statsFile,base_url,job_id,parallelism,windowStep,approach,steps,expFrequency,pipelineDeploymentDelay, previous_write_records)
            statsFile.flush()  # Ensure data is written immediately

def write_source_stats(statsFile, base_url, job_id, parallelism, windowStep, approach,
                       steps, expFrequency,pipelineDeploymentDelay, previousWriteRecords):

    response = getJobOverview(base_url, job_id)
    jsonTxt = json.loads(response.text)

    # check source throughput only
    sourceVertex = jsonTxt.get("vertices", [])[0];
    vertexName = str(sourceVertex.get("name", "")).replace(",", "-")
    execDuration = int(sourceVertex.get("duration", 0))
    readRecords = int(sourceVertex.get("metrics", {}).get("read-records", 0))
    writeRecords = int(sourceVertex.get("metrics", {}).get("write-records", 0))

    if writeRecords <= previousWriteRecords:
        return previousWriteRecords

    dataTransmitDuration = execDuration - pipelineDeploymentDelay;
    # Throughput calculation (records per second)
    throughput = 0
    if dataTransmitDuration > 0:
        throughput = writeRecords / (dataTransmitDuration / 1000)

    row = (f"{parallelism},"f"{windowStep},"f"{approach},"f"{steps},"f"{expFrequency},"
           f"{vertexName},"f"{execDuration},"f"{dataTransmitDuration},"f"{readRecords},"
           f"{writeRecords},"f"{throughput}")

    statsFile.write(row + "\n")

    return writeRecords



def write_sink_stats(statsFile, base_url, job_id, parallelism, windowStep, approach,
                       steps, expFrequency,pipelineDeploymentDelay, previousReadRecords, sourceEvents):

    response = getJobOverview(base_url, job_id)
    jsonTxt = json.loads(response.text)

    # check source throughput only
    LatencySinkVertex = jsonTxt.get("vertices", [])[-2];
    vertexName = str(LatencySinkVertex.get("name", "")).replace(",", "-")
    execDuration = int(LatencySinkVertex.get("duration", 0))
    readRecords = int(LatencySinkVertex.get("metrics", {}).get("read-records", 0))
    writeRecords = int(LatencySinkVertex.get("metrics", {}).get("write-records", 0))

    if readRecords <= previousReadRecords:
        return previousReadRecords

    pipelineDuration = execDuration - pipelineDeploymentDelay;
    # Throughput calculation (records per second)
    throughputSink = 0
    if pipelineDuration > 0:
        throughputSink = writeRecords / (pipelineDuration / 1000)
        throughputOutput = sourceEvents/ (pipelineDuration / 1000)

    row = (f"{parallelism},"f"{windowStep},"f"{approach},"f"{steps},"f"{expFrequency},"
           f"{vertexName},"f"{execDuration},"f"{pipelineDuration},"f"{readRecords},"
           f"{writeRecords},"f"{throughputSink}", f"{sourceEvents}",f"{throughputOutput}")

    statsFile.write(row + "\n")

    return readRecords






# def poll_stats_daemon(base_url, job_id, outputFilePathAndName, parallelism, windowStep, approach, steps, expFrequency, pipelineDeploymentDelay):
#     with open(outputFilePathAndName, "w") as statsFile:
#         # Write header once
#         statsFile.write(
#             "parallelism,windowStep,approach,steps,expfrequency,"
#             "vertexName,execPipelineDuration,dataStartDuration,readRecords,writeRecords,throughput\n"
#         )
#
#         while True:
#             write_source_stats(statsFile,base_url,job_id,parallelism,windowStep,approach,steps,expFrequency,pipelineDeploymentDelay)
#             statsFile.flush()  # Ensure data is written immediately
#
#
#
# def write_stats_all(statsFile, base_url, job_id, parallelism, windowStep, approach, steps, expFrequency,pipelineDeploymentDelay):
#
#     response = getJobOverview(base_url, job_id)
#     jsonTxt = json.loads(response.text)
#
#
#     # write all vertices
#     for vertex in jsonTxt.get("vertices", []):
#         vertexName = str(vertex.get("name", "")).replace(",", "-")
#         execDuration = int(vertex.get("duration", 0))
#         readRecords = int(vertex.get("metrics", {}).get("read-records", 0))
#         writeRecords = int(vertex.get("metrics", {}).get("write-records", 0))
#
#         dataTransmitDuration = execDuration - pipelineDeploymentDelay;
#
#         # Throughput calculation (records per second)
#         throughput = 0
#         if dataTransmitDuration > 0:
#             throughput = writeRecords / (dataTransmitDuration / 1000)
#
#         row = (f"{parallelism},"f"{windowStep},"f"{approach},"f"{steps},"f"{expFrequency},"
#                f"{vertexName},"f"{execDuration},"f"{dataTransmitDuration},"f"{readRecords},"f"{writeRecords},"f"{throughput}")
#
#         statsFile.write(row + "\n")