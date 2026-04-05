import logging
from db_setup import *
from db_update import *
from scraper import *

logging.basicConfig(
    filemode="w",  #w overwrites, a appends
    filename='main.log',
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) 

logger = logging.getLogger(__name__)

def setup():
    # db_tables_setup()
    # fighters_table_setup()
    #events_table_setup()
    #records_table_setup()
    # advanced_table_setup()
    #fights_table_setup()
    # advanced_espn_setup()
    with sq.connect(db_path) as conn:

        conn.row_factory = sq.Row
        db = conn.cursor()

        rows = db.execute("SELECT event_url as url, event_date as date FROM events").fetchall()
        events_url = [row["url"] for row in rows]
    fight_scraper(events_url)
    ...
def update():
    event_list = update_records_and_fights() #should also run fight_scraper to add the fights for each event yknow
    update_advanced_stats()
    # all_fighters_gctrl()
    update_fighters_aggregate_stats()
    # update_fighters_threaded()
    # update_fighters_threaded(type=2)

    events = [row[0] for row in event_list]
    fight_scraper(events)
    ...

def tests():
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        total_analysis_update(1637, "Anthony Hernandez", "", conn)
    #total_fighting_analysis('career')

def main():
    #setup()
    update()

    


if __name__ == "__main__":
    main()