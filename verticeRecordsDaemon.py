from FlinkRESTAPIMethods import *
import json
import time


def poll_vertices_daemon(base_url, job_id, outputFilePathAndName, parallelism, wSlide,wSize, approach, steps, expFrequency,
                             sourceDeploymentTime, sinkDeploymentTime, idleness, stateExpirationTimer, deploymentTime):
    print("Polling vertex statistics...")

    while True:
        write_vertex_stats(base_url, job_id, outputFilePathAndName)
        time.sleep(1)


def write_vertex_stats(base_url, job_id, outputFilePathAndName):
    response = getJobOverview(base_url, job_id)
    jsonTxt = json.loads(response.text)

    vertices = jsonTxt.get("vertices", [])

    # Overwrite the file every poll
    with open(outputFilePathAndName, "w") as statsFile:
        statsFile.write("vertexName,read-records,write-records\n")

        for vertex in vertices:
            name = vertex.get("name", "")
            metrics = vertex.get("metrics", {})

            read_records = metrics.get("read-records", 0)
            write_records = metrics.get("write-records", 0)

            statsFile.write(f"{name},{read_records},{write_records}\n")
