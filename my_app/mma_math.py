import sqlite3 as sq
import logging
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import numpy as np
import pandas as pd
import math

db_path = (Path(__file__).parent).parent / "data" / "ufc-hat.db"
logger = logging.getLogger(__name__)


def clustering_fighter_styles():
    ...