import socket
import sys
from threading import Thread
import params


def process_msg(message):
    pass


def accept_client(connection, client_address):
    try:
        print(sys.stderr, 'connection from', client_address)
        # Receive the data
        while True:
            message = connection.recv(msg_size).decode("utf-8")
            if not message:
                print(sys.stderr, 'no more data from', client_address)
                break
            print(sys.stderr, 'received "%s"' % message)
            process_msg(message)
    finally:
        # Clean up the connection
        connection.close()


if __name__ == '__main__':
    server_address = params.server_address
    msg_size = params.msg_size

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind the socket to the port
    print(sys.stderr, 'starting up on %s port %s' % server_address)
    sock.bind(server_address)
    # Listen for incoming connections
    sock.listen(1)
    while True:
        # Wait for a connection
        print(sys.stderr, 'waiting for a connection')
        thread = Thread(target=accept_client, args=sock.accept())
        thread.start()
