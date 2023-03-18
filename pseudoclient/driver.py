from server import run_server
from client import run_client
from threading import Thread
import params

if __name__ == '__main__':
    thread = Thread(target=run_server)
    thread.start()
    for _ in range(params.number_of_client_threads):
        thread = Thread(target=run_client)
        thread.start()