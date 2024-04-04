import json


def main(file):
    print("\nReal Time Cluster Client started!\n")

    with open(file, "r", encoding="utf-8") as f:
        task_set = json.loads(f.read())

    for task in task_set:
        print(task, task_set[task])


if __name__ == "__main__":
    main("first_sched.json")
