from client import run_client
from threading import Thread
import params

if __name__ == '__main__':
    for _ in range(params.number_of_client_threads):
        thread = Thread(target=run_client)
        thread.start()
