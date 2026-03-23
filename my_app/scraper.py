from bs4 import BeautifulSoup
import string
import requests
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from utilities import *
import time, random
import re
import json

logger = logging.getLogger(__name__)


db_path = (Path(__file__).parent).parent / "data" / "testing.db"

ip_list = [
    "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
    "2001:0db8:0000:0000:abcd:1234:5678:9abc",
    "2001:0db8:abcd:0012:0000:0000:0000:0001",
    "2001:0db8:dead:beef:0000:0000:cafe:babe",
    "2001:0db8:1111:2222:3333:4444:5555:6666",
    "2001:0db8:aaaa:bbbb:cccc:dddd:eeee:ffff",
    "2001:0db8:abcd:0000:1234:5678:90ab:cdef",
    "2001:0db8:abcd:1234:ffff:0000:1111:2222",
    "2001:0db8:abcd:4321:0000:abcd:ef01:2345",
    "2001:0db8:aaaa:0000:bbbb:0000:cccc:0000",
    "2001:0db8:abcd:5678:1234:abcd:5678:9abc",
    "2001:0db8:ffff:eeee:dddd:cccc:bbbb:aaaa"
]

# stats_url = "http://ufcstats.com/statistics/fighters?char=a&page=all"
def get_ufc_fighters():
    #list of all fighters
    fighters_list = []
    seen = set()

    session = requests.Session()
    for i in list(string.ascii_lowercase):
        page = session.get(f"http://ufcstats.com/statistics/fighters?char={i}&page=all")
        if page.status_code == 200:
            soup = BeautifulSoup(page.text, "html.parser")
            tbody = soup.find("tbody")
            tr = tbody.find_all('tr') 
           
            for x, tag in enumerate(tr):
                # this dictionary should be local so that all the fighters dont point to the same pointer in the dict
                fighters = {}
                if x == 0:
                    continue
                td = tag.find_all('td')
                #this part of the code is to check if the fighter has already been seen so that the database doesnt get filled with redundancy
                try:
                    link = td[0].find("a")["href"]
                except:
                    continue

                if link in seen:
                    continue
                seen.add(link)

                for index, field in enumerate(["first_name", "last_name", "nick_name"]):
                    try:
                        fighters[field] = td[index].find("a").text.strip()
                    except:
                        fighters[field] = ""

                fighters["height"] = td[3].text.strip()
                fighters["weight"] = td[4].text.strip()
                fighters["reach"] = td[5].text.strip()
                fighters["stance"] = td[6].text.strip()
                fighters["wins"] = td[7].text.strip()
                fighters["losses"] = td[8].text.strip()
                fighters["draws"] = td[9].text.strip()
                fighters["url"] = link
                belt = td[10]
                
                for field in ["height", "weight", "reach", "stance"]:
                    if fighters[field] == "--" or fighters[field] == "''":
                        fighters[field] = "Unknown"

                #if there is an image tag then that means the fighter is a champion
                if belt.find('img'):
                    fighters["belt"] = "Champ"
                else:
                    fighters["belt"] = "--"
                fighters_list.append(fighters)
            #time.sleep(random.uniform(1, 3))

    #[{first_name, last_name, nick_name, height, weight, reach, stance, wins, losses, draws, belt, url}, ...]
    return fighters_list

def get_events():
    all_events = []
    url = "http://ufcstats.com/statistics/events/completed?page=all"
  

    session = requests.Session()

    page = session.get(url)
    if page.status_code == 200:
        logger.info("Successfully fethed event data! ")
        soup = BeautifulSoup(page.text, "html.parser")
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        for x, tr in enumerate(tr_list):
            event_data = {}
            if x == 0:
                continue
            elif tr.find('img'):
                continue
            td_list = tr.find_all('td')
            event_data['event_url'] = td_list[0].find('a')['href'].strip()
            event_data['event_name'] = td_list[0].find('a').text.strip()
            event_data['event_date'] = td_list[0].find('span').text.strip()
            event_data['event_location'] = td_list[1].text.strip()
            all_events.append(event_data)

    return all_events
    
        
def get_fighter_records(url):
    fighters_records = []
    session = requests.Session()
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        # urls = cursor.execute("select url from fighters;").fetchall()
        try:
            page = session.get(url)
            page.raise_for_status()
            logger.info(f"Fetched (Records) {url} successfuly")
        except Exception:
            logger.warning(f"Warning (Records): could not connect to {url}")
            return None
        
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
            
        for x, tr in enumerate(tr_list):
            fighter = {}
            if x == 0:
                continue

            td_list = tr.find_all('td')
            win_loss = td_list[0].find_all('i')

            i_tag = "--"
            for tag in win_loss:
                if tag.text.strip().lower() in ['win', 'loss', 'nc', 'draw', 'next']:
                    #I named it i_tag because in the website the information is inside <i>
                    i_tag = tag.text.strip()
                    break
                    
            if i_tag == 'next':
                continue

            fighter['url'] = url
            fighter['win_loss'] = i_tag
            opponents = td_list[1].find_all('p')
            fighter['fighter_1'] = opponents[0].text.strip()
            fighter['fighter_2'] = opponents[1].text.strip()
            fighter['event'] = td_list[6].find('a').text.strip()
            fighter['event_date'] = (td_list[6].find_all('p'))[1].text.strip()
            fighter['weight_class'] = None
            title = td_list[6].find("img", {"src": "http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/belt.png"})
            if title:
                is_title_fight = 'yes'
            else:
                is_title_fight = 'no'
            fighter['is_title_fight'] = is_title_fight
            method = td_list[7].find_all('p')
            fighter['method'] = f"{method[0].text.strip()} ({method[1].text.strip()})" if method[1].text.strip() != '' else method[0].text.strip()
            round_ended = td_list[8].find('p')
            fighter['round'] = round_ended.text.strip()
            time_ended = td_list[9].find('p')
            fighter['time'] = time_ended.text.strip()

            dt = datetime.strptime(fighter.get('event_date'), "%b. %d, %Y")
            usable_date = dt.strftime("%B %d, %Y")
            event_url = cursor.execute('select event_url from events where event_date = ?', (usable_date,)).fetchone()
            if event_url:
                try:    
                    event_page = session.get(event_url[0])
                    event_page.raise_for_status()
                    event_soup = BeautifulSoup(event_page.text, 'html.parser')
                    tbody = event_soup.find('tbody')
                    tr_list = tbody.find_all('tr')
                    for tr in tr_list:
                        td_list = tr.find_all('td')
                        opponents = [p.text.strip() for p in td_list[1].find_all('p')]
                        if fighter.get('fighter_1') in opponents and fighter.get('fighter_2') in opponents:
                            weight_class = td_list[6].find('p').text.strip()
                            fighter['weight_class'] = weight_class
                    logger.info(f'successfully got the event url and weight class {fighter.get("weight_class")} of the fight of fighter: {fighter.get("fighter_1")} vs opponent: {fighter.get("fighter_2")} result: {fighter.get("win_loss")}, method: {fighter.get("method")}, round and time ended: ( {fighter.get("round")} | {fighter.get("time")} ), in event date {usable_date}')
                except Exception as e:
                    logger.warning(f'exception {e} happened when attempting to fetch url {event_url}')
            else:
                logger.warning(f'could not get event url {event_url[0] if event_url else event_url} in date {usable_date}')
            
            fighters_records.append(fighter)
            #time.sleep(random.uniform(1, 3))

    return fighters_records

def get_fighter_records_threaded(max_workers=4):
    fighters_records = []
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        urls = [u[0] for u in cursor.execute("SELECT url FROM fighters;").fetchall()]

    # run threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_fighter_records, url): url for url in urls}
        for future in as_completed(futures):
            try:
                records = future.result()
                fighters_records.extend(records)
            except Exception as e:
                logger.error(f"Error processing {futures[future]}, error: {e}")

    return fighters_records
        

def get_advanced_stats():
    advanced_fighters = []
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        url = cursor.execute("select url from fighters;").fetchall()

    session = requests.Session()
    for fighter_url in url:
        advanced_fighter = {}
        try:
            page = session.get(fighter_url[0])
            page.raise_for_status()
            logger.info(f"Fetched (Advanced_stats) {fighter_url[0]} successfuly")
        except Exception:
            logger.warning(f"Warning (Adcanced_stats): could not connect to {fighter_url[0]}")
            continue
   
        soup = BeautifulSoup(page.text, 'html.parser')
        left_stats_div = soup.find('div', class_='b-list__info-box-left')
        right_stats_div = soup.find('div', class_='b-list__info-box_style-margin-right')

        left_stats = left_stats_div.find_all('li')
        right_stats = right_stats_div.find_all('li')

        advanced_fighter['url'] = fighter_url[0]
        for i in left_stats:
            tag = i.find('i').text.strip()
            advanced_fighter[tag.replace(":", "").replace(".", "").replace(" ", "_").lower()] = i.text.strip().replace(tag, "")
        for j in right_stats:
            tag = j.find('i').text.strip()
            advanced_fighter[tag.replace(":", "").replace(".", "").replace(" ", "_").lower()] = j.text.strip().replace(tag, "")              
        advanced_fighters.append(advanced_fighter)
        
           

    return advanced_fighters

def get_espn_stats(espn_url, name):
    time.sleep(random.uniform(0.2, 0.6))
    #x forwarded only wors for very lazy websites so this was useless but we move
    # ipv6_random = get_random_ip(ip_list)
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.espn.com/",
    }      
    striking_dict, clinching_dict, ground_dict = {}, {}, {}
    status_code = None




    with requests.Session() as session:
        try:
            page = session.get(espn_url.strip(), headers=headers)
            status_code = page.status_code
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')
        except Exception as e:
            logger.info(f'exception {e} happened when trying to access {espn_url}')
            return ({}, {}, {}, status_code)
        #split it into 3 phases: striking clinch and ground

        thead = soup.find_all('thead', class_='Table__THEAD')
        tbody = soup.find_all('tbody', class_='Table__TBODY')

 
        try:
            striking_col = thead[0]
            striking = tbody[0]
            clinching_col = thead[1]
            clinching = tbody[1]
            ground_col = thead[2]
            ground = tbody[2]
        except Exception as e:
            logger.info(f'espn stat getter: no data available, exception: {e}')
            return ({}, {}, {}, status_code)

        striking_fights_list = []
        try:
            for tr in striking.find_all('tr'):
                striking_data = {}
                td_list = tr.find_all('td')
                #but remember that %BODY %HEAD and %LEG stays the same in the dict 
                for index, col in enumerate([parse_espn_stats(i) for i in striking_col.find_all('th')]):
                    if index == 2:
                        continue
                    if td_list[5].text.strip() == '-':
                        continue
                    striking_data[col] = td_list[index].text.strip()
                if striking_data:
                    striking_fights_list.append(striking_data)
            striking_dict[name] = striking_fights_list
            logger.debug(f'successfully got the striking data for {name}')
        except Exception as e:
            logger.error(f'couldnt get the striking data dictionary for {name}, error: {e}')

        clinching_fights_list = []
        try:
            for tr in clinching.find_all('tr'):
                clinching_data = {}
                td_list = tr.find_all('td')
                for index, col in enumerate([parse_espn_stats(i) for i in clinching_col.find_all('th')]):
                    if index == 2:
                        continue
                    if len(td_list) > 4 and td_list[4].text.strip() == '-':
                        continue
                    clinching_data[col] = td_list[index].text.strip()
                if clinching_data:
                    clinching_fights_list.append(clinching_data)
            clinching_dict[name] = clinching_fights_list
            logger.debug(f'successfully got the clinching data for {name}')
        except Exception as e:
            logger.error(f'couldnt get the clinching data for {name}, error: {e}')

        ground_fights_list = []
        try:

            for tr in ground.find_all('tr'):
                ground_data = {}
                td_list = tr.find_all('td')
                for index, col in enumerate([parse_espn_stats(i) for i in ground_col.find_all('th')]):
                    if index == 2:
                        continue
                    if len(td_list) > 4 and td_list[4].text.strip() == '-':
                        continue
                    ground_data[col] = td_list[index].text.strip()
                if ground_data:
                    ground_fights_list.append(ground_data)
            ground_dict[name] = ground_fights_list
            logger.debug(f'successfully got the ground data for {name}')
        except Exception as e:
            logger.error(f'couldnt get the ground data for {name}, error: {e}')

    return (striking_dict, clinching_dict, ground_dict, status_code)
        
def get_espn_ids(seen_ids=None, id_and_name=(None, None)):
    fighter_id, name = id_and_name
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.espn.com/",
    }  
    time.sleep(random.uniform(0.2, 0.6))

    if seen_ids:
        if fighter_id and (fighter_id,) in seen_ids:
            return ('seen', 'seen')
    
    #fun better way:
    # if fighter_id and fighter_id in [*id for id in seen_ids]


    url = f"https://site.api.espn.com/apis/search/v2?query={name.replace(' ', '%20')}"
    # if index == 100:
    #     break
    try:
        resp = requests.get(url, headers=headers)
        data = resp.json()

        # Look for the fighter object in the json gotten from the AJAX 
        for result in data.get("results", []):
            if result["type"] == "player":
                for player in result.get("contents", []):
                    if player.get("description") == "MMA":
                        fighter_url = player["link"]["web"].replace("/mma/fighter/_/", "/mma/fighter/stats/_/")
                        logger.info(f'successfully got the url {fighter_url} for the fighter {name}')
                        return (fighter_url, name)
                    
        return (None, None)

    except Exception as e:
        logger.error(f'error {e} happened when trying to get the url of {name}')



def espn_stats_threaded(max_workers=5):
    # headers = {
    # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    #               "AppleWebKit/537.36 (KHTML, like Gecko) "
    #               "Chrome/128.0.0.0 Safari/537.36",
    # "Accept-Language": "en-US,en;q=0.9",
    # "Referer": "https://www.espn.com/",
    # }  
    striking = []
    clinching = []
    ground_game = []
    # session = requests.Session()
    with sq.connect(db_path) as conn:
        # url_1 = 'https://www.espn.com/search/_/q/'
        cursor = conn.cursor()
        rows = cursor.execute('select fighter_id, name from fighters;').fetchall()
        seen_ids = cursor.execute('select distinct fighter_id from advanced_striking;').fetchall()
        fighter_pairs = []


        # fighter_url_pattern = re.compile(r"/mma/fighter/_/id/\d+/")
        # for row in rows:
        #     try:
        #         name = row[0].lower()
        #         # first_name, last_name = row[0].lower().split(' ')
        #         # search_url = url_1 + f'{first_name}%20{last_name}'
        #         search_url = url_1 + name.replace(' ', "%20").replace('junior', 'jr.')
        #         page = session.get(search_url, headers=headers)
        #         soup = BeautifulSoup(page.text, 'html.parser')
        #         for link in soup.find_all("a", href=True):
        #             href = link["href"]
        #             print(href)
        #             if fighter_url_pattern.search(href):
        #                 fighter_url = href.replace("/mma/fighter/_/", "/mma/fighter/stats/_/")
        #                 fighter_pairs.append((fighter_url, name))
        #                 logger.info(f"Got fighter URL for {name}: {fighter_url}")
        #                 break 
        #         print(search_url)
        #         break
        #     except Exception as e:
        #         logger.error(f'error {e} happened when trying to get the url for {name}')

        #need to do: use api call instead to get all the url ids. Once this is done, I will finally be finished...
        # for index, row in enumerate(rows):
        #     name = row[0].lower()
        #     url = f"https://site.api.espn.com/apis/search/v2?query={name.replace(' ', '%20')}"
        #     # if index == 100:
        #     #     break
        #     try:
        #         resp = requests.get(url, headers=headers)
        #         data = resp.json()

        #         # Look for the fighter object in the json gotten from the AJAX 
        #         for result in data.get("results", []):
        #             if result["type"] == "player":
        #                 for player in result.get("contents", []):
        #                     if player.get("description") == "MMA":
        #                         fighter_url = player["link"]["web"].replace("/mma/fighter/_/", "/mma/fighter/stats/_/")
        #                         logger.info(f'successfully got the url {fighter_url} for the fighter {name}')
        #                         fighter_pairs.append((fighter_url, name))

        #     except Exception as e:
        #         logger.error(f'error {e} happened when trying to get the url of {name}')
    

    # sub_rows = rows[:600]
    #this gets the url and name of fighters to parse their stats
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_espn_ids, seen_ids, row) : row for row in rows}
        for future in as_completed(futures):
            try:
                result = future.result()

                if result != (None, None) and result != ('seen', 'seen'): 
                    fighter_pairs.append(result)
                    # print(strike, clinch, ground)
                    logger.info(f'no errors in getting the url for {futures[future]}')
                    # time.sleep(random.uniform(0.2, 0.6))
                elif result == ('seen', 'seen'):
                    PURPLE = "\033[35m"
                    RESET = "\033[0m"
                    logger.info(f'{PURPLE}Already seen the fighter {futures[future]} before, will be skipping{RESET}')
                else:
                    logger.warning(f'Unfortunately got None for {futures[future]}')
            
            except Exception as e:
                logger.error(f"Error getting the url for {futures[future]}, error: {e}")
                print("result: ", future.result())


    #making a predefined batch size to avoid getting rate limitted        
    batch_size = 150    #we need to look human so that we do not get rate limited
    for i in range(0, len(fighter_pairs), batch_size):
        batch = fighter_pairs[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(get_espn_stats, url, name): (url, name) for url, name in batch}
            for future in as_completed(futures):
                url, name = futures[future]
                try:
                    strike, clinch, ground, status = future.result()
                    if status != 200:
                        logger.warning(f"Website error for {name} ({url}), status {status}")
                        continue
                    if strike or clinch or ground:
                        striking.append(strike)
                        clinching.append(clinch)
                        ground_game.append(ground)
                        logger.info(f"Processed {name} ({url}) successfully")
                        # print(strike, clinch, ground)
                    else:
                        logger.info(f"No fight data found for {name} ({url})")
                except Exception as e:
                    logger.error(f"Error processing {url, name}: {e}")
        logger.info("Batch complete, sleeping 5 minutes to avoid rate limit...")
        time.sleep(300)
    
    logger.info(f"final dict sizes -> striking: {len(striking)}, clinching: {len(clinching)}, ground: {len(ground_game)}")
    return fighter_pairs, striking, clinching, ground_game
            

#need to make a backfilling for what was missed. espn is so extra with their rate limiting. Currently missing 2553

def create_fights_table(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS fights_extended (
            fight_id INTEGER,
            fighter_1 INTEGER,
            fighter_2 INTEGER,
            fight_url TEXT UNIQUE,
            winner TEXT,
            title_fight TEXT,
            bonus TEXT,
            method TEXT,
            round TEXT,
            time TEXT,
            fight_data TEXT,
            FOREIGN KEY (fight_id) REFERENCES fights (fight_id)
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS rounds (
            fight_id INTEGER,
            round integer,
            stat_type TEXT,
            stat TEXT,
            fighter_1 TEXT,
            fighter_2 TEXT,
            FOREIGN KEY (fight_id) REFERENCES fights (fight_id)
        )
    """)


def fight_exists(db, fight_url):
    result = db.execute(
        "SELECT 1 FROM fights WHERE fight_url=? LIMIT 1",
        (fight_url,)
    ).fetchone()

    return result is not None


def save_fight_to_db(db, fight):
    db.execute("""
        INSERT OR IGNORE INTO fights_extended (
            fight_id,
            fighter_1,
            fighter_2,
            fight_url,
            winner,
            title_fight,
            bonus,
            method,
            round,
            time,
            fight_data
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        fight.get('fight_id'),
        fight.get('fighter_1').get('fighter_id'),
        fight.get('fighter_2').get('fighter_id'),
        fight.get("fight_url"),
        fight.get("winner"),
        fight.get("title_fight"),
        fight.get("bonus"),
        fight.get("method"),
        fight.get("round"),
        fight.get("time"),
        json.dumps(fight)
        )
    )


def fight_scraper(do_all=True):
    """if do_all is true it means the function will scrape all events"""
    with sq.connect(db_path) as conn:
        '''this scraper scrapes fight data from individual fights'''
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.espn.com/",
        }

        # NOTE TO SELF:
        # add a way to scrape this fight data and also link it to
        #its fight ID in the fights table. If it is not in there
        # make sure to add an entry for it (DONE)
        # it should also give round data in a table called rounds (NEXT)
        # round data has 5 columns and you simply dump all the round data
        # into each round and then clean up the data later for visualization
        # we can then also use the data in the round data for more accurate
        # fighter data and predictions by deriving trends from various rounds

        conn.row_factory = sq.Row
        db = conn.cursor()

        # create table once
        create_fights_table(db)

        rows = db.execute("SELECT event_url as url, event_date as date FROM events").fetchall()
        events_url = [row["url"] for row in rows]

        with requests.Session() as session:
            for url in events_url:
                try:
                    response = session.get(url, headers=headers)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, "html.parser")

                    fight_date = None

                    li_list = soup.find_all('li')
                    for li in li_list:
                        if "Date:" in li.text:
                            fight_date = li.text.replace("Date:", "").strip()

                    table = soup.find("table")
                    if not table:
                        continue

                    thead = table.find("thead")
                    if not thead:
                        continue

                    th_list = thead.find_all("th")

                    wc_index = None
                    for i, th in enumerate(th_list):
                        if th.text.lower().strip() == "weight class":
                            wc_index = i
                            break

                    if wc_index is None:
                        continue

                    tbody = table.find("tbody")
                    if not tbody:
                        continue

                    tr_list = tbody.find_all("tr")

                    tr_list_helper(
                        tr_list, session, db, wc_index, headers, fight_date
                    )

                    # commit once per event (FAST)
                    db.connection.commit()

                except Exception as e:
                    logger.error(f"There was an exception requesting url: {e}")


def tr_list_helper(tr_list, session, db, wc_index, headers, date):

    for tr in tr_list:

        fight = {
            "fighter_1": {},
            "fighter_2": {},
        }

        date = datetime.strptime(date, r"%B %d, %Y")
        date = date.strftime(r"%b. %d, %Y")
        fight['date'] = date

        title_fight = False
        bonus = None

        td_list = tr.find_all("td")
        if not td_list:
            continue

        # -------------------------
        # fighters
        # -------------------------
        names = td_list[1].find_all("p")
        if len(names) < 2:
            continue

        fight["fighter_1"]["name"] = names[0].text.strip()
        fight["fighter_2"]["name"] = names[1].text.strip()

        row = db.execute(
            "select fighter_id from fighters where name = ?",
            (fight["fighter_1"]["name"],)
        ).fetchone()

        fight['fighter_1']['fighter_id'] = row['fighter_id'] if row else None

        row = db.execute(
            "select fighter_id from fighters where name = ?",
            (fight["fighter_2"]["name"],)
        ).fetchone()

        fight['fighter_2']['fighter_id'] = row['fighter_id'] if row else None

        # winner
        if td_list[0].text.strip().lower() == "win":
            fight["winner"] = fight["fighter_1"]["name"]
        else:
            fight["winner"] = None

        # -------------------------
        # fight URL
        # -------------------------
        fight_url = tr.get("data-link")
        if not fight_url:
            continue

        # fix relative urls
        if fight_url.startswith("/"):
            fight_url = "https://www.ufcstats.com" + fight_url

        # skip already scraped fights
        if fight_exists(db, fight_url):
            logger.info(f"Skipping existing fight: {fight_url}")
            continue

        fight["fight_url"] = fight_url

        # -------------------------
        # bonuses / belts
        # -------------------------
        weight_class = td_list[wc_index]

        for img in weight_class.find_all("img"):
            src = img.get("src", "")

            if "belt.png" in src:
                title_fight = True
            elif "perf.png" in src:
                bonus = "perf"
            elif "sub.png" in src:
                bonus = "sub"
            elif "fight.png" in src:
                bonus = "fight"
            elif "ko.png" in src:
                bonus = "ko"

        fight["title_fight"] = "yes" if title_fight else "no"
        fight["bonus"] = bonus

        fight["method"] = td_list[wc_index + 1].text.strip()
        fight["round"] = td_list[wc_index + 2].text.strip()
        fight["time"] = td_list[wc_index + 3].text.strip()

        # =====================================================
        # SCRAPE FIGHT PAGE
        # =====================================================
        try:
            page = session.get(fight_url, headers=headers)
            page.raise_for_status()

            fight_soup = BeautifulSoup(page.text, "html.parser")

            total_headers = None
            total_stats = None
            sig_headers = None
            sig_stats = None

            for table in fight_soup.find_all("table"):

                thead = table.find("thead")
                if not thead:
                    continue

                ths = thead.find_all("th")
                if len(ths) < 2:
                    continue

                header_name = ths[1].text.strip().lower()

                if header_name == "kd":
                    total_headers = ths
                    total_stats = table.find("tbody").find_all("tr")

                elif header_name == "sig. str":
                    sig_headers = ths
                    sig_stats = table.find("tbody").find_all("tr")

            # TOTAL STATS
            if total_headers and total_stats and len(total_stats) >= 2:

                f1_cols = total_stats[0].find_all("td")
                f2_cols = total_stats[1].find_all("td")

                for i in range(1, len(total_headers)):

                    header = (
                        total_headers[i].text
                        .replace(".", "")
                        .replace("%", "percent")
                        .strip()
                        .replace(" ", "_")
                        .lower()
                    )

                    fight["fighter_1"][header] = f1_cols[i].text.strip()
                    fight["fighter_2"][header] = f2_cols[i].text.strip()

            # SIGNIFICANT STRIKES
            if sig_headers and sig_stats and len(sig_stats) >= 2:

                f1_cols = sig_stats[0].find_all("td")
                f2_cols = sig_stats[1].find_all("td")

                for i in range(1, len(sig_headers)):

                    header = (
                        sig_headers[i].text
                        .replace(".", "")
                        .replace("%", "percent")
                        .strip()
                        .replace(" ", "_")
                        .lower()
                    )

                    fight["fighter_1"][header] = f1_cols[i].text.strip()
                    fight["fighter_2"][header] = f2_cols[i].text.strip()
                        
            fight['fight_id'] = get_fight_id(db, fight)
            rounds = round_data(db, fight, fight_soup)
            parsed_rounds = table_parser(rounds, fight['fight_id'])



                


            # save fight
            save_fight_to_db(db, fight)
            save_to_rounds(db, parsed_rounds)

        except Exception as e:
            logger.error(f"Error processing fight: {e}")

def get_fight_id(db, fight):
    fighter_1 = fight['fighter_1']['fighter_id']
    fighter_2 = fight['fighter_2']['fighter_id']
    winner = fight['winner']
    date = fight['date']
    placeholders = (fighter_1, fighter_2, fighter_2, fighter_1, winner, date)
    row = db.execute('''select fight_id from fights where ((fighter_a = ? and fighter_b = ?)
    or (fighter_b = ? and fighter_a = ?)) and winner = ? and date = ?''', placeholders).fetchone()

    if row:
        return row['fight_id']
    
    return None

def round_data(db, fight, page):
    rounds = {
        'sig_strikes': {
            'headers':['fighter', 'sig_str', 'sig_str_percent', 'head', 'body', 'leg', 
                       'distance', 'clinch', 'ground'],
        },
        'totals': {
            'headers':['fighter', 'kd', 'sig_str', 'sig_str_percent', 'total_str', 'td', 'td_percent',
                    'sub_att', 'rev', 'ctr']
        }
    }
    links = page.find_all('a')
    count = 0
    for link in links:
        if "Per round" in link.text and count < 2:
            parent = link.parent 
            table = parent.find('table')
            if count == 0:
                rounds['totals']['data'] = table
            else:
                rounds['sig_strikes']['data'] = table
            count += 1
    return rounds
    ...

def table_parser(rounds, fight_id):

    parsed = []

    for stat_type in ["totals", "sig_strikes"]:

        table = rounds.get(stat_type, {}).get("data")
        headers = rounds.get(stat_type, {}).get("headers")

        if not table:
            continue

        bodies = table.find_all("tbody")

        round_num = 0
        for body in bodies:
            rows = body.find_all('tr')
            if not rows:
                continue
            round_num += 1
            for r_index, row in enumerate(rows):

                cols = row.find_all("td")

                # # this is what I have to work on
                # fighter_1_vals = cols[0].text.strip().split()
                # fighter_2_vals = cols[1].text.strip().split()
                # # ---------------- ISOLATION

                for i, header in enumerate(headers):

                    parsed.append(
                        {
                            "fight_id": fight_id,
                            "round": round_num,
                            "stat_type": stat_type,
                            "stat": header,
                            "fighter_1": cols[i].find_all('p')[0].text.strip() if i < len(cols) else None,
                            "fighter_2": cols[i].find_all('p')[1].text.strip() if i < len(cols) else None,
                        }
                    )

    return parsed


# ------------------------------------------------
# SAVE ROUND DATA
# ------------------------------------------------

def save_to_rounds(db, rounds):

    for r in rounds:

        db.execute(
            """
            insert into rounds
            (fight_id, round, stat_type, stat, fighter_1, fighter_2)
            values (?, ?, ?, ?, ?, ?)
            """,
            (
                r["fight_id"],
                r["round"],
                r["stat_type"],
                r["stat"],
                r["fighter_1"],
                r["fighter_2"],
            ),
        )
