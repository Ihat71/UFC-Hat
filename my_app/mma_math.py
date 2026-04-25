import sqlite3 as sq
import logging
import os
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import numpy as np
import pandas as pd
import math

base_dir= os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
db_path= os.path.join(base_dir, "data", "ufc-hat.db")
logger = logging.getLogger(__name__)


def clustering_fighter_styles():
    ...