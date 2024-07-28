import json
import threading
import socket
import logging
import time



logger = logging.getLogger('rt_client_cluster')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('rt_client_cluster.log')
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


HOST = "192.168.1.111"
PORT = 8765


def send_task_request(task, count, repeat, task_data):
    logger.info(f"{task} request {count+1}/{repeat}")
    task_data["task_name"] = f"{task}_{count+1}"
    start = time.time()
    client_socket = socket.socket()
    client_socket.connect((HOST, PORT))

    message = json.dumps(task_data)

    client_socket.send(message.encode())

    response = ""
    while True:
        data = client_socket.recv(1024)
        if data:
            response = data.decode("utf-8")
            break

    client_socket.close()
    end = time.time()
    deadline = task_data["deadline"]
    elapsed_time = end - start
    if "Request Denied" in response:
        logger.warning(f"Request {count+1}/{repeat} for {task} was denied by the scheduler in {elapsed_time:.4f}s" )        
    elif elapsed_time > deadline:
        logger.error(
            f"Elapsed time for {task} request {count+1}/{repeat} was {elapsed_time:.4f}s for {deadline}s deadline with response {response}"
        )
    else:
        logger.info(
            f"Elapsed time for {task} request {count+1}/{repeat} was {elapsed_time:.4f}s for {deadline}s deadline with response {response}"
        )


def task_connecction(thread, task, task_data):
    logger.info(
        f"Thread number: {thread} with task: {task} and task info: {task_data}"
    )
    repeat = task_data["repeat"]
    try:
        for count in range(repeat):
            thread = threading.Timer(
                task_data["period"] * count,
                send_task_request,
                (task, count, repeat, task_data),
            )
            thread.start()

    except Exception as e:
        logger.error(f"Fail to connect to the server for {task}, error: {str(e)}")


def main(file):
    logger.info("Real Time Cluster Client started!")

    with open(file, "r", encoding="utf-8") as f:
        task_set = json.loads(f.read())

    for idx, task in enumerate(task_set):
        thread = threading.Thread(
            target=task_connecction, args=(idx, task, task_set[task])
        )
        thread.start()


if __name__ == "__main__":
    #main("task_set/first_sched.json")
    #main("task_set/second_sched.json")
    #main("task_set/long_schedulable_cluster_taskset.json")
    #main("task_set/schedulable_cluster_taskset.json")
    #main("task_set/schedulable_one_worker.json")
    main("task_set/non_schedulable_cluster_taskset.json")
