from bs4 import BeautifulSoup
import requests
from pathlib import Path
import sqlite3 as sq
import math
from datetime import datetime, timedelta
import logging
from elo import get_dates, to_table_date, elo_equation, elo_history_table
from scraper import get_espn_stats, get_espn_ids
from db_setup import get_column_query
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import random, time
from analysis import *
from utilities import easy_espn_fights_getter

db_path = (Path(__file__).parent).parent / "data" / "testing.db"
events_url = "http://ufcstats.com/statistics/events/completed?page=all"

#dont forget traceback.print_exc() using import traceback when you log bossman
logger = logging.getLogger(__name__)

fighters_updated = []


#update events first then the rest become easy
def update_events():
    with sq.connect(db_path) as conn: 
        cursor = conn.cursor()
        #returns a sorted list of all the dates of the events in my events table
        all_ufc_dates = get_dates()
        up_to_date = all_ufc_dates[-1]
        event_list = []
        try:
            page = requests.get(events_url)
            soup = BeautifulSoup(page.text, 'html.parser')
            tbody = soup.find('tbody')
            tr_list = tbody.find_all('tr')
            for tr in tr_list:
                if not tr.text.strip() or tr.find('img'):
                    continue 
                td_list = tr.find_all('td')
                dt = datetime.strptime(td_list[0].find("span").text.strip(), "%B %d, %Y")
                if dt > up_to_date:
                    url = td_list[0].find('a')['href'].strip()
                    name = td_list[0].find('a').text.strip()
                    date = td_list[0].find('span').text.strip()
                    location = td_list[1].text.strip()
                    cursor.execute('insert into events (event_url, event_name, event_date, event_location) values (?, ?, ?, ?)', (url, name, date, location))
                    logger.info(f'successfully inserted into events the event {name}, url {url}')
                    conn.commit()
                    event_list.append((url, dt))
        except Exception as e:
            logger.error(f'error {e} in trying to make the events up to date')
        
        return event_list

def get_fighter(name, conn):
    cursor = conn.cursor()
    first_name, last_name = name.split(" ", 1)
    last_name_letter = last_name[0]
    url = f"http://ufcstats.com/statistics/fighters?char={last_name_letter}&page=all"
    page = requests.get(url)
    seen_ids = cursor.execute('select distinct fighter_id from advanced_striking;').fetchall()

    if page.status_code == 200:
        soup = BeautifulSoup(page.text, "html.parser")
        tbody = soup.find("tbody")
        tr_list = tbody.find_all('tr') 
    
        for x, tag in enumerate(tr_list):
            td = tag.find_all('td')
            if x == 0:
                continue
            if not (td[0].find("a").text.strip() == first_name and td[1].find("a").text.strip() == last_name):
                continue
            try:
                link = td[0].find("a")["href"]
            except:
                continue


            height = td[3].text.strip()
            weight = td[4].text.strip()
            reach = td[5].text.strip()
            stance = td[6].text.strip()
            wins = td[7].text.strip()
            losses = td[8].text.strip()
            draws = td[9].text.strip()
            fighter_url = link
            belt = td[10]
        
            if height == "--" or height == "''":
                height = "Unknown"
            if weight == "--" or height == "''":
                weight = "Unknown"
            if reach == "--" or height == "''":
                reach = "Unknown"
            if stance == "--" or height == "''":
                stance = "Unknown"

            #if there is an image tag then that means the fighter is a champion
            if belt.find('img'):
                belt = "Champ"
            else:
                belt = "--"
            break
        

        cursor.execute("insert into fighters (name, height, weight, reach, stance, wins, losses, draws, champ_status, url) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                            (name, height, 
                                        weight, reach, stance, wins
                                                , losses, draws, belt, fighter_url))

        conn.commit()

        new_id = cursor.execute('select fighter_id from fighters where name = ?', (name,)).fetchone()
        if new_id:
            url, _ = get_espn_ids(seen_ids, (new_id, name))
            return url

#"unexpected ctrl format"
def put_elo(name, conn):
    cursor = conn.cursor()
    fighter_id = cursor.execute('select fighter_id from fighters where name = ?', (name,)).fetchone()
    cursor.execute('insert into elo (fighter_id) values (?)', (fighter_id[0],))
    conn.commit()

    
        
def update_records_and_fights():
    event_list = update_events()
    event_list = sorted(event_list, key=lambda x: x[1])
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for url, date in event_list:
            date = to_table_date(date)
            event_id = cursor.execute('select event_id from events where event_url = ?', (url,)).fetchone()[0]
            # try:
            session = requests.Session()
            page = session.get(url)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')
            tbody = soup.find('tbody')
            tr_list = tbody.find_all('tr')

            for tr in tr_list:
                td_list = tr.find_all('td')
                result = td_list[0].find('p').text.strip().lower()

                fighter_elements = td_list[1].find_all('p')
                if len(fighter_elements) < 2:
                    logger.error("Could not find both fighters, skipping fight")
                    continue

                fighter_a = fighter_elements[0].text.strip()
                fighter_b = fighter_elements[1].text.strip()
                logger.info(f"Processing fight: {fighter_a} vs {fighter_b}")

                new_url_a = None
                new_url_b = None

                if not cursor.execute('select * from fighters where name = ?', (fighter_a,)).fetchone():
                    new_url_a = get_fighter(fighter_a, conn)
                    put_elo(fighter_a, conn)
                    logger.info(f'put into fighters and elo table the new fighter {fighter_a}')
                if not cursor.execute('select * from fighters where name = ?', (fighter_b,)).fetchone():
                    new_url_b = get_fighter(fighter_b, conn)
                    put_elo(fighter_b, conn)
                    logger.info(f'put into fighters and elo table the new fighter {fighter_b}')

                row = cursor.execute('select fighter_id from fighters where name = ?', (fighter_a,)).fetchone()
                id_a = row[0] if row else None

                row = cursor.execute('select fighter_id from fighters where name = ?', (fighter_b,)).fetchone()
                id_b = row[0] if row else None

                swapped = False
                if id_b < id_a:
                    id_a, id_b = id_b, id_a
                    swapped = True
                    # fighter_a, fighter_b = fighter_b, fighter_a

                #note to self: recheck id swap logic

                weight_class = td_list[6].text.strip()
                is_title_fight = 'yes' if td_list[6].find('img', {"src":"http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/belt.png"}) else 'no'
                #a bit confusing but saves energy and time

                p_tags = td_list[7].find_all('p') if td_list[7].find_all('p') else None
                if len(p_tags) >= 2 and p_tags[1].text.strip() and p_tags:
                    method = f"{p_tags[0].text.strip()} ({p_tags[1].text.strip()})"
                elif len(p_tags) >= 1 and p_tags:
                    method = p_tags[0].text.strip()
                else:
                    method = "Unknown"

                row = cursor.execute('select fighter_id from fighters where name = ?', (fighter_a,)).fetchone()
                # winner = row[0] if row  else None
                if 'draw' in result:
                    winner = 0
                elif 'nc' in result or 'DQ' in method:
                    winner = None
                elif 'win' in result:
                    winner = row[0]
                else:
                    winner = 'Unknown'

                round_ended = int(td_list[8].text.strip())
                time_ended = td_list[9].text.strip()
                #inserting the fights for fighter_a and fighter_b
                cursor.execute('insert into fights (event_id, date, fighter_a, fighter_b, winner, weight_class, method, round_ended, time_ended, is_title_fight) values (?,?,?,?,?,?,?,?,?,?)', (event_id, date, id_a, id_b, winner, weight_class, method, round_ended, time_ended, is_title_fight))
                conn.commit()
                logger.info(f'successfully inserted into fights the fight of {id_a} vs {id_b} winner {winner}')
                #inserting records for fighter_a and fighter_b
                #for fighter_a
                if id_a == winner:
                    win_loss_a = 'win'
                    win_loss_b = 'loss'
                elif id_b == winner:
                    win_loss_b = 'win'
                    win_loss_a = 'loss'
                elif winner == 0:
                    win_loss_a = 'draw'
                    win_loss_b = 'draw'
                elif winner == None or 'DQ' in method:
                    win_loss_a = 'nc'
                    win_loss_b = 'nc'

                url_a = cursor.execute('select url from fighters where fighter_id = ?', (id_a,)).fetchone()[0]
                cursor.execute('insert into records (url, event_id, date, fighter_1, fighter_2, result, weight_class, method, round_num, fight_time, is_title_fight) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (url_a, event_id, date, id_a, id_b, win_loss_a, weight_class, method, round_ended, time_ended, is_title_fight))
                logger.info(f'inserted into records table the record for fighter {id_a}')
                #for fighter_b
                url_b = cursor.execute('select url from fighters where fighter_id = ?', (id_b,)).fetchone()[0]
                cursor.execute('insert into records (url, event_id, date, fighter_1, fighter_2, result, weight_class, method, round_num, fight_time, is_title_fight) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (url_b, event_id, date, id_b, id_a, win_loss_b, weight_class, method, round_ended, time_ended, is_title_fight))
                conn.commit()
                logger.info(f'inserted into records table the record for fighter {id_b}')
                #tag
                #getting the new elos:
                rA = cursor.execute('select elo from elo where fighter_id = ?', (id_a,)).fetchone()[0]
                rB = cursor.execute('select elo from elo where fighter_id = ?', (id_b,)).fetchone()[0]
                elo_winner = 'A' if id_a == winner else 'B'
                is_draw = True if winner == 0 else False
                is_nc = True if winner == None else False
                new_rA, new_rB = elo_equation(rA, rB, elo_winner, is_draw, is_nc, method, round_ended, True if is_title_fight == 'yes' else False)
                cursor.execute('update elo set elo = ? where fighter_id = ?', (new_rA, id_a))
                cursor.execute('update elo set elo = ? where fighter_id = ?', (new_rB, id_b))
                conn.commit()
                logger.info(f'changed elo of fighter {id_a} from {rA} to {new_rA}')
                logger.info(f'changed elo of fighter {id_b} from {rB} to {new_rB}')

                ended = f"{round_ended} | {time_ended}"
                cursor.execute('insert into elo_history (fighter_1, fighter_2, winner, weight_class, elo_1, elo_2, new_elo_1, new_elo_2, method, round_time_ended, is_title_fight, date) values (?,?,?,?,?,?,?,?,?,?,?,?)', (id_a, id_b, winner, weight_class, rA, rB, new_rA, new_rB, method, ended, is_title_fight, date))
                logger.info(f'inserted into elo history table the fight of {id_a} and {id_b}')
                conn.commit()

                fighters_updated.append(id_a)
                fighters_updated.append(id_b)

                skip_a = False
                skip_b = False

                #this is where the espn stats updater starts:
                espn_url_a = cursor.execute('select espn_url from advanced_striking where fighter_id = ?;', (id_a,)).fetchone()
                if espn_url_a:
                    espn_url_a = espn_url_a[0]
                else:
                    name = cursor.execute('select name from fighters where fighter_id = ?;', (id_a,)).fetchone()
                    url, _ = get_espn_ids(id_and_name=(id_a, name[0]))
                    espn_url_a = url
                    easy_espn_fights_getter(id_a, espn_url_a, conn)
                    skip_a = True
                    
                espn_url_b = cursor.execute('select espn_url from advanced_striking where fighter_id = ?;', (id_b,)).fetchone()
                if espn_url_b:
                    espn_url_b = espn_url_b[0]
                else:
                    name = cursor.execute('select name from fighters where fighter_id = ?;', (id_b,)).fetchone()
                    url, _ = get_espn_ids(id_and_name=(id_b, name[0]))
                    espn_url_b = url
                    easy_espn_fights_getter(id_b, espn_url_b, conn)
                    skip_b = True

                logger.debug('updating advanced stats now...')
                if swapped:
                    fighter_a, fighter_b = fighter_b, fighter_a
                if not skip_a:
                    espn_update(espn_url_a, id_a, fighter_a, date, conn)
                if not skip_b:
                    espn_update(espn_url_b, id_b, fighter_b, date, conn)




                #note to self: also update the draws and no contests, also add some logger
                #note to self: add elo history then everything is finally complete


            # except Exception as e:
            #     logger.error(f"exception {e} happened when trying to access the urls for records and fights for url {url} in date {date}")


def espn_update(url, id, name, date, conn):
    """
    Updates ESPN stats for a fighter for a given date.
    
    date: str, format e.g. "Feb. 14, 2023" (from your DB)
    """
    cursor = conn.cursor()
    # Convert incoming date to datetime object
    try:
        fight_date = datetime.strptime(date, "%b. %d, %Y")
    except ValueError:
        # fallback if month doesn't have a dot
        fight_date = datetime.strptime(date, "%b %d, %Y")
    
    striking, clinch, ground, status = get_espn_stats(url, name)
    stats_dict = {'advanced_striking': striking, 'advanced_clinch': clinch, 'advanced_ground': ground}

    found = False
    for table, stats in stats_dict.items():
        for fights in stats.values():
            for fight in fights:
                # Convert fight date from ESPN into datetime too
                espn_date_str = fight.get('date')
                if not espn_date_str:
                    continue
                try:
                    espn_date = datetime.strptime(espn_date_str, "%b. %d, %Y")
                except ValueError:
                    try:
                        espn_date = datetime.strptime(espn_date_str, "%b %d, %Y")
                    except ValueError:
                        logger.warning(f"Could not parse ESPN date: {espn_date_str}")
                        continue
                
                if abs(espn_date - fight_date) <= timedelta(days=1):
                    logger.info('fight found')
                    column_query, values = get_column_query(fight)
                    query = f'insert into {table} {column_query} values {values}'
                    # clean up '-' values
                    for key in fight:
                        if fight[key] and fight[key].strip() == '-':
                            fight[key] = None
                    params = (id, url, *fight.values())
                    cursor.execute(query, params)
                    logger.info(f'successfully inserted into {table} the stats {fight} for {name}')
                    found = True
                    break

    if not found:
        logger.info('fight not found')



#yes I plugged in my previous update advanced stats function to cloud sonnet and copied its reviewd version, however
#I ONLY did this to learn the correct handling and logger practices later down the line from this code
def update_advanced_stats():
    # Remove potential duplicates
    unique_fighters = set(fighters_updated)
    logger.info(f"Updating advanced stats for {len(unique_fighters)} fighters")
    
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        
        for fighter in unique_fighters:
            try:
                logger.info(f"Processing advanced stats for fighter ID: {fighter}")
                
                # Get fighter URL with error handling
                url_result = cursor.execute('select url from fighters where fighter_id = ?', (fighter,)).fetchone()
                if not url_result or not url_result[0]:
                    logger.error(f"No URL found for fighter {fighter}")
                    continue
                    
                url = url_result[0]
                logger.info(f"Fetching stats from: {url}")
                
                # Make web request with error handling
                try:
                    page = requests.get(url, timeout=30)
                    page.raise_for_status()  # Raises exception for bad status codes
                except requests.RequestException as e:
                    logger.error(f"Failed to fetch page for fighter {fighter}: {e}")
                    continue
                
                soup = BeautifulSoup(page.text, 'html.parser')
                advanced_stat = {}
                
                # Find all li elements
                li_list = soup.find_all('li')
                logger.info(f"Found {len(li_list)} li elements for fighter {fighter}")
                
                stats_found = 0
                for li in li_list:
                    tag = li.find('i')
                    logger.info("found a tag")
                    if not tag:
                        logger.info('did not find a tag')
                        continue
                        
                    stat_name = tag.text.strip().replace('.', '').replace(' ', '_').replace(':', '').lower()
                    
                    if stat_name in ['slpm', 'str_acc', 'sapm', 'str_def', 'td_avg', 'td_acc', 'td_def', 'sub_avg']:
                        # Extract the stat value more safely
                        try:
                            full_text = li.text.strip()
                            tag_text = tag.text.strip()
                            stat = full_text.replace(tag_text, '').strip()
                            
                            # Basic validation - make sure we got something
                            if stat and stat != '--' and stat != 'N/A':
                                advanced_stat[stat_name] = stat
                                stats_found += 1
                                logger.debug(f"Fighter {fighter}: {stat_name} = {stat}")
                            else:
                                advanced_stat[stat_name] = None
                                
                        except Exception as e:
                            logger.warning(f"Error parsing stat {stat_name} for fighter {fighter}: {e}")
                            advanced_stat[stat_name] = None
                
                logger.info(f"Fighter {fighter}: Found {stats_found} valid stats")
                
                # Check if fighter already exists in advanced_stats - FIXED
                existing = cursor.execute('select fighter_id from advanced_stats where fighter_id = ?', (fighter,)).fetchone()
                
                if existing:
                    # Update existing record
                    cursor.execute('''
                        update advanced_stats 
                        set SLpM=?, str_acc=?, SApM=?, str_def=?, td_avg=?, td_acc=?, td_def=?, sub_avg=? 
                        where fighter_id=?
                    ''', (
                        advanced_stat.get('slpm'), 
                        advanced_stat.get('str_acc'), 
                        advanced_stat.get('sapm'), 
                        advanced_stat.get('str_def'), 
                        advanced_stat.get('td_avg'), 
                        advanced_stat.get('td_acc'), 
                        advanced_stat.get('td_def'), 
                        advanced_stat.get('sub_avg'), 
                        fighter
                    ))
                    logger.info(f'Successfully updated advanced stats for fighter: {fighter}')
                    
                else:
                    # Insert new record - make sure column count matches your table schema
                    cursor.execute('''
                        insert into advanced_stats 
                        (fighter_id, url, SLpM, str_acc, SApM, str_def, td_avg, td_acc, td_def, sub_avg) 
                        values (?,?,?,?,?,?,?,?,?,?)
                    ''', (
                        fighter, 
                        url, 
                        advanced_stat.get('slpm'), 
                        advanced_stat.get('str_acc'), 
                        advanced_stat.get('sapm'), 
                        advanced_stat.get('str_def'), 
                        advanced_stat.get('td_avg'), 
                        advanced_stat.get('td_acc'), 
                        advanced_stat.get('td_def'), 
                        advanced_stat.get('sub_avg')
                    ))
                    logger.info(f'Successfully inserted advanced stats for fighter: {fighter}')
                
            except Exception as e:
                logger.error(f"Error processing fighter {fighter}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                continue  # Continue with next fighter
        
        try:
            conn.commit()
            logger.info("Successfully committed all advanced stats updates")
        except Exception as e:
            logger.error(f"Error committing advanced stats: {e}")
            conn.rollback()

def update_fighters(espn_url, fighter_name, fighter_id):
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/116.0.0.0 Safari/537.36'
    } 
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/116.0.0.0 Safari/537.36')
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(espn_url)
        WebDriverWait(driver, 5).until(lambda d: d.find_elements(By.TAG_NAME, "li"))

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        logger.info(f'Successfully connected to the page for {fighter_name, fighter_id}')
            
        fighter_pic, country, birthdate, team = (None, None, None, None)

        # Extract info from <li> tags
        for li in soup.find_all('li'):
            text = li.text.lower()
            if 'birthdate' in text:
                birthdate = li.text.lower().replace('birthdate', '').strip().split(' (')[0]
            elif 'team' in text:
                team = li.text.lower().replace('team', '').strip()

        imgs = soup.find_all('img')
        for img in imgs:
            src = img.get('src')
            title = img.get('title')
            if title and not country:  # flag
                country = title
            if 'headshot' in src or 'headshots' in src:  # ESPN uses 'headshots' in image URLs
                fighter_pic = src
    except Exception as e:
        logger.error(f'Error occurred in getting the webpage: {e}')
        return (None, None, None, None)
    

    finally:
        driver.quit()

    return (fighter_pic, country, birthdate, team)


def update_fighters_profile_threaded(type):
    '''type 1 does a general update while type 2 does an update only to the fighters that werent covered the first time'''
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        # Check existing columns in SQLite
        cursor.execute("PRAGMA table_info(fighters);")
        existing_cols = [c[1] for c in cursor.fetchall()]
        for col_name, col_type in [('picture', 'TEXT'), ('team', 'VARCHAR(100)'),
                                   ('birthday', 'VARCHAR(100)'), ('country', 'VARCHAR(100)')]:
            if col_name not in existing_cols:
                cursor.execute(f"ALTER TABLE fighters ADD COLUMN {col_name} {col_type}")

        # Fetch unique fighters
        if type == 1:
            fighter_list = cursor.execute(
                '''SELECT DISTINCT a.espn_url, f.name, f.fighter_id FROM fighters f 
                JOIN advanced_striking a ON f.fighter_id = a.fighter_id;'''
            ).fetchall()
        elif type == 2:
            fighter_list = cursor.execute(
                '''
                SELECT DISTINCT a.espn_url, f.name, f.fighter_id FROM fighters f 
                JOIN advanced_striking a ON f.fighter_id = a.fighter_id where (f.picture is null 
                or f.team is null 
                or f.country is null 
                or f.birthday is null
                );
                '''
            ).fetchall()

            non_covered = cursor.execute(
                '''SELECT count(a.espn_url) FROM fighters f 
                JOIN advanced_striking a ON f.fighter_id = a.fighter_id where f.picture is null or 
                f.team is null or 
                f.country is null or 
                f.birthday is null;'''
            ).fetchone()

            logger.info(f'Count of non covered fighters: {non_covered[0] if non_covered else None}')
            # fighter_list = cursor.execute('select distinct a.espn_url, f.name from fighters f join advanced_striking a on f.fighter_id = a.fighter_id where f.fighter_id = 100;').fetchall()
            # print(fighter_list)

    # Threaded processing
    results_list = []
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = {executor.submit(update_fighters, url, name, fighter_id): (url, name, fighter_id) for url, name, fighter_id in fighter_list}
        for future in as_completed(futures):
            try:
                results = future.result()
                if results:
                    # Include fighter name for WHERE clause
                    fighter_pic, country, birthdate, team = results
                    if country and country.strip() == 'Powered by OneTrust Opens in a new Tab':
                        country = None

                    results_list.append((fighter_pic, country, birthdate, team, futures[future][2]))
                    logger.info(f'Successfully fetched data for {futures[future][1]}')
                    logger.info(f'results for {futures[future][1]}: {results}')
            except Exception as e:
                logger.error(f"Error processing {futures[future]}, error: {e}")

    # Commit all updates at once
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for fighter_data in results_list:
            cursor.execute(
                'UPDATE fighters SET picture=?, country=?, birthday=?, team=? WHERE fighter_id=?',
                fighter_data
            )
        conn.commit()


def get_ground_control(url, name, fighter_id):
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/118.0.5993.117 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/"
    }

    control_time = 0
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')

        real_table = None
        for table in tables:
            if 'w/l' in table.find('thead').find('th').text.strip().lower():
                real_table = table
        
        tbody = real_table.find('tbody')
        if not tbody:
            return None
        tr_list = tbody.find_all('tr')
        th_list = soup.find('thead').find_all('th')


        for tr in tr_list:
            new_url = tr.get('data-link')
            if not new_url:
                continue

            response = requests.get(new_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            tbody = soup.find('tbody')
            if not tbody:
                continue
            td_list = tbody.find('tr').find_all('td')

            index = 0 if name.lower() in td_list[0].find('p').text.strip().lower() else 1
            ctrl = (td_list[9].find_all('p'))[index].text.strip()

            if ctrl and ':' in ctrl:
                minute, seconds = ctrl.split(':')
                ctrl_minutes = int(minute) + float(seconds)/60
                control_time += ctrl_minutes
            else:
                # either skip or log warning
                logger.warning(f"Unexpected ctrl format: '{ctrl}' for fighter {name}")
            # time.sleep(random.uniform(0.5, 1))


    except requests.exceptions.RequestException as e:
        logger.info(f"Error fetching the page: {e}")
        return None
    
    return control_time
        
def all_fighters_gctrl():
    '''function to get total ground control for every fighter and put it in advanced_stats'''
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        fighter_list = cursor.execute('select distinct f.url, f.name, f.fighter_id from records r join fighters f on r.fighter_1 = f.fighter_id')

        results_list = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_ground_control, url, name, fighter_id): (url, name, fighter_id) for url, name, fighter_id in fighter_list}
            for future in as_completed(futures):
                try:
                    results = future.result()
                    if results:
                        # Include fighter name for WHERE clause
                        ground_control = results

                        results_list.append((ground_control, futures[future][2]))
                        logger.info(f'Successfully fetched data for {futures[future][1]}')
                        logger.info(f'results for {futures[future][1]}: {results}')
                except Exception as e:
                    logger.error(f"Error processing {futures[future]}, error: {e}")
        

        for fighter_data in results_list:
            cursor.execute(
                'UPDATE advanced_stats SET control_time=? WHERE fighter_id=?',
                fighter_data
            )
        conn.commit()

def update_individual_fighter_aggregate_stats(fighter_id, db, conn):
    name = db.execute('select url, name from fighters where fighter_id = ?', (fighter_id,)).fetchone()
    if not name:
        logger.error(f'no fighter found with ID {fighter_id}')
        return None
    
    control_time = get_ground_control(name['url'], name['name'], fighter_id)
    db.execute(
        'UPDATE advanced_stats SET control_time=? WHERE fighter_id=?',
        (control_time, fighter_id)
    )
    total_analysis_update(fighter_id, name['name'], db, conn)

def update_fighters_aggregate_stats():
    with sq.connect(db_path) as conn:
        conn.row_factory = sq.Row
        db = conn.cursor()

        for fighter_id in fighters_updated:
            try:
                update_individual_fighter_aggregate_stats(fighter_id, db, conn)
            except Exception as e:
                logger.warning(f"Skipping {fighter_id}, Exception raised: {e}")
                continue

def new_fighter_clean_up():
    # for fighters who have a registered fighter id but not an espn url.
    with sq.connect(db_path) as conn:
        conn.row_factory = sq.Row
        db = conn.cursor()

        fighters = db.execute('select * from fighters where fighter_id NOT IN (select fighter_id from advanced_striking)').fetchall()

        for fighter in fighters:
            ...
    


#two things to fix: fetching fighters in update records and also the winner detection
# update_events()
# update_records_and_fights()
# update_advanced_stats()
# update_fighters_aggregate_stats()


