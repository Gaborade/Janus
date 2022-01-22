import socket
import sys

class CLIException(Exception):
    pass

DATABASE_COMMANDS = ("insert", "retrieve", "update", "delete")

"""HOST, PORT = "localhost", 4000
data = " ".join(sys.argv[1:])

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))
    sock.sendall(bytes(data + "\n", "utf-8"))
    received = str(sock.recv(1024), "utf-8")

print(f'{received}')
"""

def commands_cli():
    global DATABASE_COMMANDS
    HOST, PORT = "localhost", 4000
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((HOST, PORT))
        except ConnectionRefusedError:
            raise ConnectionRefusedError("Cannot connect to host server, perhaps start key_db_server")

        while True:
            command = input("key_db_cli $  ").strip()
            if command == "quit" or command == "exit":
                sys.exit(0)
            else:
                command_split = command.split(" ")
                command_len = len(command_split)
                if command_len < 2:
                    print("Few arguments given", file=sys.stderr)
                    continue
                elif command_len > 3:
                    print("Too many arguments", file=sys.stderr)

                if command_split[0] not in DATABASE_COMMANDS:
                    print("Command not supported", file=sys.stderr)
                else:
                    sock.sendall(bytes(command + "\n", "utf-8"))
                    response = str(sock.recv(1024), "utf-8")
                    print(response)


if __name__ == "__main__":
    commands_cli()

