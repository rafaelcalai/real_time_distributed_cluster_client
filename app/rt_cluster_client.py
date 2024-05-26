import json
import threading
import socket
import logging
import time


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

HOST = "192.168.1.111"
PORT = 8765


def send_task_request(task, count, repeat, task_data):
    logging.info(f"{task} request {count+1}/{repeat}")
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
    if elapsed_time > deadline:
        logging.error(
            f"Elapsed time for {task} request {count+1}/{repeat} was {elapsed_time:.4f}s for {deadline}s deadline with response {response}"
        )
    else:
        logging.info(
            f"Elapsed time for {task} request {count+1}/{repeat} was {elapsed_time:.4f}s for {deadline}s deadline with response {response}"
        )


def task_connecction(thread, task, task_data):
    logging.info(
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
        logging.error(f"Fail to connect to the server for {task}, error: {str(e)}")


def main(file):
    logging.info("Real Time Cluster Client started!")

    with open(file, "r", encoding="utf-8") as f:
        task_set = json.loads(f.read())

    for idx, task in enumerate(task_set):
        thread = threading.Thread(
            target=task_connecction, args=(idx, task, task_set[task])
        )
        thread.start()


if __name__ == "__main__":
    #main("first_sched.json")
    #main("second_sched.json")
    main("schedulable _cluster_taskset.json")