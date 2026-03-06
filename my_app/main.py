import logging
from db_setup import *
from db_update import *
from scraper import fight_scraper

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
    fight_scraper()
    ...
def update():
    # update_records_and_fights()
    # update_advanced_stats()
    # update_fighters_aggregate_stats()
    #update_fighters_threaded()
    # update_fighters_threaded(type=2)
    # all_fighters_gctrl()
    ...
def main():
    setup()
    


if __name__ == "__main__":
    main()