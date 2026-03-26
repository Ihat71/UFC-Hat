import sqlite3 as sq
import logging
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import numpy as np
import pandas as pd
import math
import base64
# from sklearn.preprocessing import StandardScaler, MinMaxScaler

logger = logging.getLogger(__name__)
db_path = (Path(__file__).parent).parent / "data" / "testing.db"