import json
import threading
import socket
import logging

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

HOST = "127.0.0.1"
PORT = 8765


def task_connecction(thread, task, task_data):
    logging.info(
        f"Thread number: {thread} with task: {task} and task info: {task_data}"
    )
    try:
        client_socket = socket.socket()
        client_socket.connect((HOST, PORT))

        message = json.dumps(task_data)

        client_socket.send(message.encode())
        client_socket.close()

    except Exception as e:
        logging.error(f"Fail to connect to the server for {task}, error: {str(e)}")


def main(file):
    logging.info("\nReal Time Cluster Client started!\n")

    with open(file, "r", encoding="utf-8") as f:
        task_set = json.loads(f.read())

    for idx, task in enumerate(task_set):
        logging.info(task, task_set[task])

        thread = threading.Thread(target=task_connecction, args=(idx, task, task_set))
        thread.start()


if __name__ == "__main__":
    main("first_sched.json")
