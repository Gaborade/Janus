import os
import sys
import sys
import re
import time
from log import logger
import threading

# create a write-ahead log?
# use decorator methods to attach to insert, update, delete methods so
# decorator method assess page to see if it's last byte offset is beyond 4096
# if it is split into pages or create a new page
# need to store current file in a file

# store binary tree python object as a pickle and load into memory
# hope overtime the object size doesn't get bigger than RAM
# what of compression when stored pickled


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
            except Exception:
                logger.error(f"[ATTEMPT-FAIL] {key} could not be set in db")
                return "1"
            else:
                return "0"

    def retrieve(self, key):
        if not self.use_btrees:
            with open("log_1.txt", mode="r") as file:
                data = file.readlines()
                for line in data:
                    if line.startswith(key):
                        key, value = line[:-1].split()
                        return value
                return "1"

    def update(self, key, new_value):
        delete_query_response = self.delete(key)
        if delete_query_response == "1":
            return "1"
        elif delete_query_response == "0":
            return self.insert(key, new_value)

    def delete(self, key):
        db_file = open("log_1.txt", mode="r")
        key_value_pairs = (line[:-1].split() for line in db_file)
        keys = [element[0] for element in key_value_pairs]
        if key not in keys:
            db_file.close()
            return "1"
        data = db_file.readlines()
        db_file.close()
        try:
            with open("log_1.txt", mode="w") as rewrite_db_file:
                for line in data:
                    if not line.startswith(key):
                        rewrite_file.write(line)
        except Exception as e:
            logger.error(f"[ATTEMPT-FAIL] {key} could not be deleted, reason: ", e)
            return "1"
        else:
            return "0"

    def sort_keys(self, time_interval=1800):
        # sleep for 30 mins between sort rewrites
        # 60 secs will be used for quick experiments
        # only current page is sorted
        # there should be a threadlock to ensure when this is
        # being done no other operation by any method is also being done
        while True:
            current_file_size = os.stat(self.current_page).st_size
            if current_file_size == 0:
                time.sleep(time_interval)
                continue
            with open(self.current_page, mode="r") as db_file:
                lines = db_file.readlines()
                sorted_lines = sorted(lines)
            if lines == sorted_lines:
                time.sleep(time_interval)
                continue
            else:
                with open(self.current_page, "w") as rewrite_db_file:
                    print("rewriting db file again")
                    for line in sorted_lines:
                        rewrite_db_file.write(line)
