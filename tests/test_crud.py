from pathlib import Path
import sys
parent_path = Path.cwd()
sys.path.append(str(parent_path))

import esmongo.constant as const
from esmongo.db_server import MongoDB, ES