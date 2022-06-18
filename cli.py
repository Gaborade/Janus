import socket
import sys
import os

DATABASE_COMMANDS = ("insert", "retrieve", "update", "delete")
SYSTEM_EXITS = ("exit", "quit", "q", "Q", "QUIT", "Quit")

class JanusServerNotStartedError(ConnectionRefusedError):
    pass


def commands_cli():
    global DATABASE_COMMANDS
    global SYSTEM_EXITS

    HOST, PORT = "localhost", 4000

    while True:
        command = input("janus_cli $  ").strip()
        if command in SYSTEM_EXITS:
            sys.exit()
        else:
            command_split = command.split()
            command_len = len(command_split)

            if command_len < 2:
                print("Few arguments given", file=sys.stderr)
            elif command_len > 3:
                print("Too many arguments", file=sys.stderr)
            else:

                if command_split[0] not in DATABASE_COMMANDS:
                    print("Command not supported", file=sys.stderr)
                else:
                    # socket is re-initialised everytime
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        try:
                            sock.connect((HOST, PORT))
                        except ConnectionRefusedError:
                            env_name = "python3" if os.name == "posix" else "python"
                            raise JanusServerNotStartedError(
                                "Cannot connect to janus server, start server "
                                f"with {env_name} server.py command"
                            )
                        sock.sendall(bytes(command + "\n", "utf-8"))
                        response = str(sock.recv(1024), "utf-8")
                        print(response)


if __name__ == "__main__":
    try:
        commands_cli()
    except KeyboardInterrupt as e:
        print("Cli abruptly stopped, reason: KeyboardInterrupt, ", e.__doc__)
        sys.exit()

