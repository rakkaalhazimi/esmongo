from pathlib import Path
import sys
parent_path = Path.cwd()
sys.path.append(str(parent_path))

import esmongo.constant as const
from esmongo.db_server import MongoDB, ES


def test_connect_mongodb():
    server = MongoDB(host=const.HOST_MONGODB)
    is_connect = server.connect()
    assert is_connect == True

def test_connect_es():
    server = ES(host=const.HOST_ES)
    is_connect = server.connect(username=const.USER_ES, password=const.PWD_ES)
    assert is_connect == True
    