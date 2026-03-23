import random
from flask import redirect, render_template, session
from functools import wraps
import requests
from bs4 import BeautifulSoup
import logging
import uuid
from my_app.plots import *
import time
#from db_setup import get_espn_stats, get_column_query 


logger = logging.getLogger(__name__)


def get_fighter_id(conn, name):
    '''this function is for the db_setup to modularize the getting of fighter ids'''
    cursor = conn.cursor()
    row = cursor.execute('select fighter_id from fighters where LOWER(name) = ?', (name.lower(),)).fetchone()
    if row:
        return row[0]
    else:
        return None

def parse_espn_stats(i):
    '''this function is for the scraper where it is needed to parse the stats strings'''
    return i.text.lower().strip().replace(' ', '_').replace('/', '_').replace('-', '_').replace('.', '')


def replace_last(text, old, new, count=1):
    """Replace the last occurence with a new word"""
    parts = text.rsplit(old, count)
    return new.join(parts)

def get_fighter_pair_url(fighter_pairs, fighter_name):
    '''this returms the url from the fighter_pairs variable I made'''
    for url, name in fighter_pairs:
        if name == fighter_name:
            return url
        
def get_random_ip(ip_list):
    '''retruns a random IP from a list of valid IPs'''
    return random.choice(ip_list)

def login_required(f):
    """
    Decorate routes to require login.

    https://flask.palletsprojects.com/en/latest/patterns/viewdecorators/
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get("user_id") is None:
            return redirect("/login")
        return f(*args, **kwargs)

    return decorated_function

def apology(message, code=400):
    """Render message as an apology to user."""

    def escape(s):
        """
        Escape special characters.

        https://github.com/jacebrowning/memegen#special-characters
        """
        for old, new in [
            ("-", "--"),
            (" ", "-"),
            ("_", "__"),
            ("?", "~q"),
            ("%", "~p"),
            ("#", "~h"),
            ("/", "~s"),
            ('"', "''"),
        ]:
            s = s.replace(old, new)
        return s

    return render_template("apology.html", top=code, bottom=escape(message)), code

def get_upcoming_events_list():
    '''This function is to get the upcoming events from the ufc stats website, returns None if error, returns {event_name:[event_url, event_date, event_location, number]} if no error'''
    url = "http://ufcstats.com/statistics/events/upcoming"
    events_hash = {}
    try:
        page = requests.get(url)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        for i, tr in enumerate(tr_list):
            if i == 0:
                continue
            td_list = tr.find_all('td')
            event_url = td_list[0].find('a')['href'] if td_list else None
            event_name = td_list[0].find('a').text.strip() if td_list else None
            event_date = td_list[0].find('span').text.strip() if td_list else None
            event_location = td_list[1].text.strip() if td_list else None

            events_hash[i] = {
                'event_url':event_url,
                'event_date':event_date,
                'event_location':event_location,
                'event_name':event_name,
                'event_id':get_web_route()
            }

    except Exception as e:
        logger.warning(f'error getting upcoming events: {e}')
        return None

    return events_hash

def get_upcoming_event_info(url):
    '''gets the actual details of any ufc event. returns None if error. Returns {fight_number:{fighter_1, fighter_2, weight_class, is_title}} for upcoming events
    returns {fight_number:{a lot of data}} for completed ones lol'''

    #figure out how to differentiate between completed and upcoming fight
    #figure out the data structure
    try:
        page = requests.get(url)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        fights = {}
        for number, tr in enumerate(tr_list):
            is_title = False
            td_list = tr.find_all('td')
            for i, td in enumerate(td_list):
                #the juicy stuff is only in the 1st and 6th td
                if i == 1:
                    fighter_list = td.find_all('p')
                    fighter_1 = fighter_list[0].text.strip() if fighter_list else None
                    fighter_2 = fighter_list[1].text.strip() if fighter_list else None
                # elif i == 6:
                #     x = td.find('p')
                #     weight_class = x.text.strip() if x else None
                #     if x.find('img'):
                #         is_title = True
                var = td.find_all('p')
                for p in var:
                    if 'weight' in p.text.strip():
                        weight_class = p.text.strip()
                        if p.find('img'):
                            is_title = True
                    

            fights[number + 1] = {
                'fighter_1': fighter_1,
                'fighter_2': fighter_2,
                'weight_class': weight_class,
                'is_title': is_title
            }

    except Exception as e:
        logger.warning(f'exception occuted getting details for event: {e}')
        return None
    
    return fights
    
def get_completed_event_info(url):
    '''
    get the completed events info,
    args:
    plug in an url   
    returns:
    dict: {number:{winner, loser, kd, str, td, sub, weight_class, is_title, is_extra, method, round_time}}'''
    # I guess I do need to fix this later
    try:
        page = requests.get(url)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        fights = {}
        for number, tr in enumerate(tr_list):
            is_title = False
            is_extra = None
            if tr.find_all('td'):
                td_list = tr.find_all('td')
                fighter_list = td_list[1].find_all('p')
                fighter_1 = fighter_list[0].text.strip()
                fighter_2 = fighter_list[1].text.strip()
                kd = (td_list[2].find_all('p')[0].text.strip(), td_list[2].find_all('p')[1].text.strip())
                strikes = (td_list[3].find_all('p')[0].text.strip(), td_list[3].find_all('p')[1].text.strip())
                takedowns = (td_list[4].find_all('p')[0].text.strip(), td_list[4].find_all('p')[1].text.strip())
                sub = (td_list[5].find_all('p')[0].text.strip(), td_list[5].find_all('p')[1].text.strip())
                weight_class = td_list[6].text.strip()
                if td_list[6].find_all('img'):
                    for img in td_list[6].find_all('img'):
                        src = img['src']
                        for i in ['perf', 'fight', 'sub', 'ko']:
                            if i in src:
                                is_extra = i
                        if 'belt' in src:
                            is_title = True
                method = (td_list[7].find_all('p')[0].text.strip(), td_list[7].find_all('p')[1].text.strip())

                round_number = td_list[8].text.strip()
                time = td_list[9].text.strip()
                round_time = f"{round_number} - {time}"


            fights[number + 1] = {
                'winner': fighter_1,
                'loser': fighter_2,
                'kd': kd,
                'str': strikes,
                'td': takedowns,
                'sub':sub,
                'weight_class':weight_class,
                'is_title':is_title,
                'is_extra':is_extra,
                'method':method,
                'round_time':round_time
            }

    except Exception as e:
        logger.warning(f'exception occured getting details for event: {e}')
        return None
    
    return fights

def get_web_route():
    random_id = str(uuid.uuid4())
    return random_id

def get_all_fighters(db):
    fighters = db.execute('select * from fighters').fetchall()

    return fighters

def get_two_fighters(name1, name2, db):
    name1 = name1.lower()
    name2 = name2.lower()

    rows = db.execute('select * from fighters f where lower(f.name) = ? or lower(f.name) = ?', (name1, name2)).fetchall()

    if len(rows) < 2:
        return None
    
    temp_1, temp_2 = rows

    fighters = [temp_1, temp_2]
    fighter_dicts = []

    for f in fighters:
        fighter_dicts.append({
            "id": f["fighter_id"],
            "name":f['name'],
            "nationality": f["country"] or "--",
            "weight": f["weight"] or "--",
            "team": f["team"] or "--",
            "birthday": f["birthday"] or "--",
            'picture': f['picture'] or '--'
        })

    fighter_1, fighter_2 = fighter_dicts

    if fighter_1 and fighter_2:
        return fighter_1, fighter_2
    
def get_fighter_data(id, db):

    row = db.execute('''select * from fighters f 
                     join aggregate_career c on f.fighter_id = c.fighter_id 
                     join aggregate_global g on f.fighter_id = g.fighter_id 
                     where f.fighter_id = ?''', (id,)).fetchone()

    if not row:
        return None
    
    temp = row
    fighter = {}

    feet, inches = temp['height'].replace('\'', '').replace('\"', '').split()
    fighter['elo'] = temp['elo']
    fighter['Peak Elo'] = temp['elo']
    fighter['Height (cm)'] = round(int(feet) * 30.48 + int(inches) * 2.54) if feet and inches else '--'

    reach_str = temp['reach'].replace('\"', '').strip()
    try:
        reach_cm = float(reach_str) * 2.54 if reach_str else None
    except ValueError:
        reach_cm = None

    fighter['Reach (cm)'] = round(reach_cm) if reach_cm is not None else '--'
    fighter['Wins'] = temp['wins']
    fighter['Losses'] = temp['losses']
    fighter['KO/TKO'] = temp['ko_tko']
    fighter['Subs'] = temp['subs']
    fighter['Decisions'] = temp['decisions']
    fighter['Title Fights'] = temp['title_fights']
    fighter['Number of Minutes in Octagon'] = temp['num_of_mins']
    fighter['Highest Win Streak'] = temp['highest_win_streak']

    return fighter

def plot_mergers(fighter1, fighter2, db):
    strike_fig, fig2 = striking_analysis_plot(fighter1, db), striking_analysis_plot(fighter2, db)
    for trace in fig2.data:
        trace.update(line=dict(color="purple"))
        strike_fig.add_trace(trace)

    
    grappling_fig, fig2 = grappling_analysis_plot(fighter1, db), grappling_analysis_plot(fighter2, db)
    for trace in fig2.data:
        trace.update(line=dict(color="purple"))
        grappling_fig.add_trace(trace)
    
    career_fig, fig2 = career_plot(fighter1, db), career_plot(fighter2, db)
    for trace in fig2.data:
        trace.update(line=dict(color="purple"))
        career_fig.add_trace(trace)

    return strike_fig.to_html(full_html=False), grappling_fig.to_html(full_html=False), career_fig.to_html(full_html=False)

    
def compare_career_stats(fighter1, fighter2, db):
    ...

def get_global_score(db, fighter_id):
    score = db.execute('select global_rating, global_rating_scaled from aggregate_global where fighter_id = ?', (fighter_id,)).fetchone()

    return score

def career_data_cleaner(career_data):
    for fighter_name, fighter_stats in career_data.items():

        # Make a list of original keys so we can safely modify dict
        original_keys = list(fighter_stats.keys())

        for old_key in original_keys:
            value = fighter_stats.pop(old_key)

            label = old_key

            label = label.replace('sm', 'submissions')
            label = label.replace("ss", "significant strike")
            label = label.replace("tsl", "total strikes landed")
            label = label.replace("_pm", " / minute")
            label = label.replace("_scaled", "")
            label = label.replace("_", " ")
            label = label.replace("opp", "opponent")

            label = label.title()

            fighter_stats[label] = value

    return career_data

def easy_espn_fights_getter(id, url, conn):
    """gets fighter fights from espn then puts them inside the database easily without hassle"""
    cursor = conn.cursor()
    name = cursor.execute('select name from fighters where fighter_id = ?', (id,)).fetchone()
    name = name[0]

    striking, clinch, ground, _ = get_espn_stats_util(url, name)
    stats_dict = {'advanced_striking': striking, 'advanced_clinch': clinch, 'advanced_ground': ground}
    for table, stats in stats_dict.items():
            for fights in stats.values():
                for fight in fights:
                    # Convert fight date from ESPN into datetime too
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

def get_column_query(fight_dict):
    '''makes a custom query so i dont have to keep writing queries im so done with that'''
    column_query = '(fighter_id,espn_url,'
    values = '(?,?,'
    for key in fight_dict:
        if key.lower() in ['%body', '%leg', '%head']:
            key = f"{key.lower().replace('%', '')}_percentage"
        column_query += f"{key},"
        values += '?,'
    column_query = replace_last(column_query, ",", ")")
    values = replace_last(values, ",", ")")
    return column_query, values

def get_espn_stats_util(espn_url, name):
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


