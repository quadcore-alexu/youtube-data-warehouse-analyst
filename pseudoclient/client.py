import socket
import sys
import time
import params

if __name__ == '__main__':
    server_address = params.server_address
    msg_size = params.msg_size
    delay = params.client_delay_time
    schema = params.schema
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    print(sys.stderr, 'connecting to %s port %s' % server_address)
    sock.connect(server_address)
    try:
        # Send data
        while True:
            message = schema.format(video_id=1050, country='Egypt')
            if len(message.encode('utf-8')) >= msg_size:
                raise 'message length exceeds specified maximum size'
            print(sys.stderr, 'sending "%s"' % message)
            sock.sendall(message.encode("utf-8"))
            time.sleep(delay)

    finally:
        print(sys.stderr, 'closing socket')
        sock.close()