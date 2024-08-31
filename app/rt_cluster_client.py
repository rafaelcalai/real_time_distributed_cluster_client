import json
import threading
import socket
import logging
import time
import os
from datetime import datetime
import argparse


logger = logging.getLogger("rt_client_cluster")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = os.path.join(log_dir, f"{current_datetime}.log")

file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

HOST = "192.168.1.120"
PORT = 8765


def send_task_request(task, count, repeat, task_data):
    """
    Sends a task request to the server and logs the response.

    Args:
        task (str): The name of the task.
        count (int): The current count of the request.
        repeat (int): The total number of requests to be sent.
        task_data (dict): The data associated with the task.

    Returns:
        None

    Raises:
        None
    """
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
        logger.warning(
            f"Request {count+1}/{repeat} for {task} was denied by the scheduler in {elapsed_time:.4f}s"
        )
    elif elapsed_time > deadline:
        logger.error(
            f"Elapsed time for {task} request {count+1}/{repeat} was {elapsed_time:.4f}s for {deadline}s deadline with response {response}"
        )
    else:
        logger.info(
            f"Elapsed time for {task} request {count+1}/{repeat} was {elapsed_time:.4f}s for {deadline}s deadline with response {response}"
        )


def task_connecction(thread, task, task_data):
    logger.info(f"Thread number: {thread} with task: {task} and task info: {task_data}")
    repeat = task_data["repeat"]
    try:
        for count in range(repeat):
            delay = 0 if count == 0 else task_data["period"]
            thread = threading.Timer(
                delay,
                send_task_request,
                (task, count, repeat, task_data),
            )
            thread.start()
            time.sleep(task_data["period"])

    except Exception as e:
        logger.error(f"Fail {task}, error: {str(e)}")


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
    parser = argparse.ArgumentParser(description="Real Time Cluster Client")
    parser.add_argument(
        "--taskset",
        type=str,
        choices=[
            "first_sched",
            "second_sched",
            "long_schedulable_cluster_taskset",
            "schedulable_cluster_taskset",
            "schedulable_one_worker",
            "non_schedulable_cluster_taskset",
        ],
        help="Choose a taskset",
    )
    args = parser.parse_args()
    if args.taskset:
        taskset_file = f"task_set/{args.taskset}.json"
        main(taskset_file)
    else:
        parser.print_help()
