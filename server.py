import socketserver
import sys
from log import logger
from engine import Janus
import os
import threading
import logging

# experiment with tracemalloc
import tracemalloc

tracemalloc.start()
db = Janus()


def memory_trace(limit=10):
    dash = "-" * 30
    print("-" * 74)
    print("-" * 74)
    print("-", "MEMORY USAGE", "-", sep=dash)
    print("-" * 74)
    snapshot = tracemalloc.take_snapshot()
    memory_stats = snapshot.statistics("lineno")
    for stat in memory_stats[:limit]:
        print(stat)


class TCPServerHandler(socketserver.StreamRequestHandler):
    def handle(self):
        global db
        self.data = self.rfile.readline().strip().decode()
        logger.info(
            "Message sent by Janus cli client with address "
            f"{self.client_address[0]}:{self.client_address[1]}"
        )
        command_split = self.data.split()

        if len(command_split) == 3:
            db_command, key, value = command_split
            command_len = 3

        elif len(command_split) == 2:
            db_command, key = command_split
            command_len = 2

        # callable makes sure no key db attributes which are not
        # methods are not called by getattr
        if hasattr(db, f"{db_command}") and callable(getattr(db, f"{db_command}")):
            command = getattr(db, f"{db_command}")
            if command_len == 3:
                query_response = command(key, value)
                self.wfile.write(f"{query_response}".encode())
            elif command_len == 2:
                query_response = command(key)
                self.wfile.write(f"{query_response}".encode())


if __name__ == "__main__":
    sort_keys_thread = threading.Thread(target=db.sort_keys, daemon=True)
    sort_keys_thread.start()
    HOST, PORT = "localhost", 4000
    SERVER_IP_ADDR = "127.0.0.1" if HOST == "localhost" else HOST
    if len(sys.argv) == 2:
        DEBUG = True if sys.argv[1] == "--debug" or sys.argv[1] == "-d" else False
    else:
        DEBUG = False

    db.log_level = logging.DEBUG if DEBUG else logging.INFO

    with socketserver.TCPServer((HOST, PORT), TCPServerHandler) as server:
        try:
            if DEBUG:
                logger.info("DEBUG=ON")
            else:
                logger.info("DEBUG=OFF")
            process_id = os.getpid()
            logger.info(f"Starting process id {process_id}")
            logger.info(f"Janus server starting")
            logger.info(f"Server listening on address {SERVER_IP_ADDR}:{PORT}")
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info(f"Closing process id {process_id}")
            logger.info(f"Janus server shutting down")
            # still doesn't solve socket bind problem
            server.shutdown()
            if DEBUG:
                memory_trace()
            sys.exit()
