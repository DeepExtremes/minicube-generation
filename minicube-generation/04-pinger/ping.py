import time

from datetime import datetime


def ping():
    while True:
        print(datetime.now())
        time.sleep(600)


if __name__ == "__main__":
    ping()
