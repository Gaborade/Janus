import os
import sys
import sys
import re
from pathlib import Path
import fcntl
import time
import threading
import logging
from log import logger


class Janus:
    PARENT_DIRECTORY = Path(__file__).resolve(strict=True).parent.parent
    DB_LOGS_DIRECTORY_PATH = PARENT_DIRECTORY / ".db_logs"
    WRITE_CURRENT_PAGE_PATH = PARENT_DIRECTORY / ".curr_page.txt"
    PAGE_MAX_SIZE = 0x1000  # (4096/4kb)

    def __init__(self, use_btrees=False):
        self.use_btrees = use_btrees
        self.log_level = logging.INFO
        self.log = logger
        # use fcntl instead of threading.Lock
        # for file locking and create a context manager for it
        self.lock = threading.Lock()
        # self._binary_tree = BinaryTree() if self._use_btree else None

        if not os.path.exists(self.DB_LOGS_DIRECTORY_PATH):
            os.mkdir(self.DB_LOGS_DIRECTORY_PATH)
        os.chdir(self.DB_LOGS_DIRECTORY_PATH)
        self.root_page = "log_1.bin"
        self._init_db_file()

    def _init_db_file(self):
        if not os.path.exists(self.WRITE_CURRENT_PAGE_PATH):
            with open(self.root_page, mode="ab") as init_db_file:
                init_db_file.close()
            self.current_page = self.root_page
            self._write_current_page_name(self.current_page)
            self.log.info(f"{self.current_page} is the first db_logs page")
        else:
            self.current_page = self._read_current_page_name()

    def _write_current_page_name(self, page_name):
        with open(self.WRITE_CURRENT_PAGE_PATH, mode="w") as file:
            file.write(page_name)

    def _read_current_page_name(self):
        with open(self.WRITE_CURRENT_PAGE_PATH, mode="r") as file:
            page_name = file.read()
        return page_name

    def _log_level_set_to_debug(self):
        return self.log_level == logging.DEBUG

    def insert(self, key, value):
        """
        Inserts key value pairs until file size almost exceeds or is
        equal to PAGE_MAX_SIZE of 4096 (4kb), then inserts into a new file.
        """

        current_file_size = os.stat(self.current_page).st_size
        # 2 for addition of space and newline character
        len_new_entries = len(key) + len(value) + 2

        if current_file_size + len_new_entries > Janus.PAGE_MAX_SIZE:
            page_num = re.findall(r"\d+", self.current_page)[0]
            # TODO: replace number naming files with hexadecimal naming
            self.current_page = self.current_page.replace(
                page_num, str(int(page_num) + 1)
            )
            self._write_current_page_name(self.current_page)
            self.log.info(f"{self.current_page} the new db_logs page for insertions")

        with open(self.current_page, mode="ab") as file:
            try:
                key_in_byte = bytes(key, encoding="utf-8")
                value_in_byte = bytes(value, encoding="utf-8")
                file.write(
                    b"%(key)s %(value)s\n"
                    % {
                        b"key": bytes(key, encoding="utf-8"),
                        b"value": bytes(value, encoding="utf-8"),
                    }
                )
                # file.write(f"{key} {value}\n")  # needs to be in bytes
                self.log.info(f"{key} set in db")
            except Exception as e:
                if self._log_level_set_to_debug():
                    self.log.debug(
                        f"[ATTEMPT-FAIL] {key} could not be set in db, reason: {e}\n"
                        f"Site of error: {self.__class__.__name__}.insert method, {__name__}.py file".strip()
                    )
                return "1"
            else:
                return "0"

    def retrieve(self, key):
        if not self.use_btrees:
            with open(self.root_page, mode="r") as file:
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
        db_file = open(self.root_page, mode="r")
        data = db_file.readlines()
        db_file.close()
        key_value_pairs = (line[:-1].split() for line in data)
        keys = [element[0] for element in key_value_pairs]
        if key not in keys:
            return "1"
        try:
            with open(self.root_page, mode="wb") as rewrite_db_file:
                for line in data:
                    if not line.startswith(key):
                        rewrite_db_file.write(bytes(line, encoding="utf-8"))
        except Exception as e:
            if self._log_level_set_to_debug():
                self.log.debug(
                    f"[ATTEMPT-FAIL] {key} could not be deleted, reason: {e}\n"
                    f"Site of error: {self.__class__.__name__}.delete method, {__name__}.py file".strip()
                )
            return "1"
        else:
            return "0"

    def sort_keys(self, time_interval=1800):
        # probably needs a thread lock since self.current_page is a shared resource

        # reason for the many log messages are due to the fact
        # that sort_keys is run as a thread
        # Threads are unpredictable, thus should be under monitoring
        tag = "[SORT_KEYS_THREAD]"
        self.log.info(
            f"{tag}:Sorting keys within {time_interval} seconds time intervals"
        )

        while True:
            db_file = open(self.current_page, mode="r")
            lines = db_file.readlines()
            db_file.close()
            len_lines = len(lines)
            if len_lines == 0 or len_lines == 1:
                time.sleep(time_interval)
                self.log.info(
                    f"{tag}:{self.current_page} page not sorted, has either one or no entry"
                )
                continue

            sorted_lines = sorted(lines)
            if lines == sorted_lines:
                time.sleep(time_interval)
                self.log.info(
                    f"{tag}:{self.current_page} page current entries already sorted"
                )
                continue

            else:
                with open(self.current_page, mode="wb") as rewrite_db_file:
                    for line in sorted_lines:
                        rewrite_db_file.write(bytes(line, encoding="utf-8"))
                        self.log.info(
                            f"{tag}:{self.current_page} page successfully sorted"
                        )
