import socket
import sys
from threading import Thread
import params


def process_msg(message):
    print(sys.stderr, 'received "%s"' % message)


def accept_client(connection, client_address):
    msg_size = params.msg_size
    try:
        print(sys.stderr, 'connection from', client_address)
        # Receive the data
        while True:
            message = connection.recv(msg_size).decode("utf-8")
            if not message:
                break
            process_msg(message)
    finally:
        # Clean up the connection
        connection.close()


def run_server():
    server_address = params.server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind the socket to the port
    print(sys.stderr, 'starting up on %s port %s' % server_address)
    sock.bind(server_address)
    # Listen for incoming connections
    sock.listen(1)
    while True:
        # Wait for a connection
        thread = Thread(target=accept_client, args=sock.accept())
        thread.start()
