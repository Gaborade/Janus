import os
import sys
import re
import socketserver
from log import logger


class Key:
    STORE_KEYS_FILE = os.path.join(os.path.dirname(__file__), "store_keys.txt")

    def __init__(self, use_btrees=False):
        self.use_btrees = use_btrees
        #self._binary_tree = BinaryTree() if self._use_btree else None

    def insert(self, key, value):
        with open(self.STORE_KEYS_FILE, mode="a") as file:
            file.write(f"{key} {value} \n")
            logger.info(f"{key} set in db")
    
    def retrieve(self, key):
        if not self.use_btrees:
            with open(self.STORE_KEYS_FILE, mode="r") as file:
                keys = file.readlines()
                for line in keys:
                    if line.startswith(key):
                        value = re.split(r'(\n|\s)', line)[2]
                        return value

    def update(self, key, new_value):
        if not self.use_btrees:
            with open(self.STORE_KEYS_FILE) as file:
                keys = file.readlines()
                for line in keys:
                    if line.startswith(key):
                        # line.replace
                        pass

