import os
import sys
import sys
import re
from log import logger
import threading

# create a write-ahead log?
# use decorator methods to attach to insert, update, delete methods so
# decorator method assess page to see if it's last byte offset is beyond 4096
# if it is split into pages or create a new page
# need to store current file in a file


class Key:
    DB_DIRECTORY_PATH = os.path.join(os.path.dirname(__file__), ".db_logs")
    WRITE_CURRENT_PAGE_PATH = os.path.join(os.path.dirname(__file__), ".curr_page.txt")
    PAGE_MAX_SIZE = 4096

    def __init__(self, use_btrees=False):
        self.use_btrees = use_btrees
        self.lock = threading.Lock()
        # self._binary_tree = BinaryTree() if self._use_btree else None
        if not os.path.exists(self.DB_DIRECTORY_PATH):
            os.mkdir(self.DB_DIRECTORY_PATH)
        os.chdir(self.DB_DIRECTORY_PATH)
        self.root_page = "log_1.txt"

        if not os.path.exists(self.WRITE_CURRENT_PAGE_PATH):
            with open(self.root_page, mode="a") as init_db_file:
                init_db_file.close()
            self.current_page = self.root_page
            self._write_current_page_name(self.current_page)
        else:
            self.current_page = self._read_current_page_name()
            print("it read from curr file")
        if self.use_btrees:
            self.page_table = []

    def _write_current_page_name(self, page_name):
        with open(self.WRITE_CURRENT_PAGE_PATH, mode="w") as file:
            file.write(page_name)

    def _read_current_page_name(self):
        with open(self.WRITE_CURRENT_PAGE_PATH, mode="r") as file:
            page_name = file.read()
        return page_name

    def insert(self, key, value):
        current_file_size = os.stat(self.current_page).st_size
        # 2 for addition of space and newline character
        len_new_entries = len(key) + len(value) + 2
        if current_file_size + len_new_entries > self.PAGE_MAX_SIZE:
            page_num = re.findall(r"\d+", self.current_page)[0]
            self.current_page = self.current_page.replace(
                page_num, str(int(page_num) + 1)
            )
            self._write_current_page_name(self.current_page)

        with open(self.current_page, mode="a") as file:
            try:
                file.write(f"{key} {value}\n")
                logger.info(f"{key} set in db")
                return True
            except Exception:
                logger.error(f"[ATTEMPT-FAIL] {key} could not be set in db")
                return False

    def retrieve(self, key):
        if not self.use_btrees:
            with open("log_1.txt", mode="r") as file:
                data = file.readlines()
                for line in data:
                    if line.startswith(key):
                        key, value = line[:-1].split()
                        return value

    def update(self, key, new_value):
        self.delete(key)
        self.insert(key, new_value)

    def delete(self, key):
        with open("log_1.txt", mode="r") as file:
            data = file.readlines()
        with open("log_1.txt", mode="w") as rewrite_file:
            for line in data:
                if not line.startswith(key):
                    rewrite_file.write(line)
