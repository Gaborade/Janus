import socketserver
import sys
from log import logger
from keys import Key
import os

class TCPServerHandler(socketserver.StreamRequestHandler):

    def handle(self):
        db = Key()
        self.data = self.rfile.readline().strip().decode()
        command_split = self.data.split()

        if len(command_split) == 3:
            db_command, key, value = command_split
            command_len = 3

        elif len(command_split) == 2:
            db_command, key = command_split
            command_len = 2

        if hasattr(db, f"{db_command}"):
            command = getattr(db, f"{db_command}")
            if command_len == 3:
                command(key, value)
                self.wfile.write("0".encode())
            elif command_len == 2:
                get_key = command(key)
                self.wfile.write(f"{get_key}".encode())


if __name__ == "__main__":
    HOST, PORT = "localhost", 4000

    with socketserver.TCPServer((HOST, PORT), TCPServerHandler) as server:
        try:
            process_id = os.getpid()
            logger.info(f"Starting process id {process_id}")
            logger.info(f"Key value db server starting, address={HOST}:{PORT}")
            server.serve_forever()

        except BaseException:
            logger.info(f"Closing process id {process_id}")
            logger.info(f"Key value db server shutting down")
            sys.exit()
