import sqlite3 as sq
import logging
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import numpy as np
import pandas as pd
import math
import base64
import json
from collections import defaultdict
# from sklearn.preprocessing import StandardScaler, MinMaxScaler

logger = logging.getLogger(__name__)
db_path = (Path(__file__).parent).parent / "data" / "ufc-hat.db"

LEAGUE_AVGS = {
    'ts_acc':0.53,
    'ss_acc':0.45,
    'sdh_acc':0.32,
    'sdb_acc':0.64,
    'sdl_acc':0.8,
    'ko_pm':0.012,
    'kd_pm':0.011,
    'scb_acc':0.70,
    'scl_acc':0.42,
    'sch_acc':0.48,
    'control_time':14,
    'tda':20,
    'sgba':3,
    'sgha':49,
    'num_of_mins':100,
    'gnp_pm':0.25,
    'sub_attempts_pm':0.022,
    'subs_pm':0.007,
    'subs_conceded_pm':0.01,
    'sub_attempts_faced_pm':0.02,
    'sgh_acc':0.58,
    'sgb_acc':0.48,
    'tk_acc':0.25,
    'td_pm':0.055,
    'opp_avg_elo': 1222,
    'control_pm':0.14
}

PRIOR_ATTEMPTS = 50
PRIOR_MINS = 20

features = {
    'striking': [
                'ts_acc', 'ss_acc', 'sdh_acc', 'sdb_acc', 'sdl_acc', 'tsl', 'sdhl', 'sdbl', 'sdll',
                'total_ssl_acc', 'total_ssa_percentage', 'kd_pm', 'tsl_pm',
                'true_ko_power', 'avg_ko_opp_durability', 'avg_ko_opp_elo',
                'SLpM', 'SApM', 'str_def', 'ko_pm',
                'body_percentage', 'head_percentage', 'leg_percentage', 'effective_volume', 'leg_kicks', 'sig_acc'
            ],
    'clinching': [
                'scbl', 'schl', 'scll', 'scb_acc', 'sch_acc', 'scl_acc', 'clinch_strikes_pm', 'effective_accuracy'
            ],
    'grappling': [
                'tdl', 'tk_acc', 'sm', 'td_def', 'sub_avg', 'control_time', 'effective_takedowns',
                'td_pm', 'effective_gnp', 'bjj_advances', 'effective_sub_threat', 'control_pm', 'td_pressure',
                'effective_control', 'opp_avg_elo', 'subs_pm', 'sub_attempts_pm', 'bjj_defence', 'gnp_pm'
            ],
    'global': [
                'wrestling_adj',
                'bjj_adj',
                'gnp_acj',
                'striking_adj',
                'global_rating'
            ],
    'career': [
                'peak_elo',
                'elo',
                'highest_win_streak',
                'min_maxed_peak_elo',
                'min_maxed_win_elo',
                'min_maxed_loss_elo',
                'elo_score',
                'fall_off_rate',
                'title_score',
                'win_score',
                'finish_rate'
            ]
}

art_style_high_low = {
    'striking' : (0.96, 0.04),
    'clinching' : (0.96, 0.04),
    'grappling' : (0.99, 0.01),
    'global' : (0.995, 0.005),
    'career' : (0.9995, 0.0005)
}

def bayesian_shrinkage(
    values,
    minutes,
    prior_mean,
    prior_minutes=120
):
    """
    values: raw metric (per-minute or composite)
    minutes: fight minutes
    prior_mean: league-wide mean of the metric
    prior_minutes: how many minutes before we fully trust the fighter
    """
    return (
        values * minutes + prior_mean * prior_minutes
    ) / (minutes + prior_minutes)


def career_analysis(db, id, cached=False):
    '''analyses career data from the fighter tab in my_app'''

    divisions={
        'strawweight':0,
        'featherweight':0,
        'bantamweight':0,
        'flyweight':0,
        'lightweight':0,
        'welterweight':0,
        'middleweight':0,
        'lightheavyweight':0,
        'heavyweight':0,
        'openweight':0,
        'catchweight':0,
        'superheavyweight':0
    }
    
    records = db.execute('select * from records where fighter_1 = ?', (id,)).fetchall()
    fights = db.execute('select * from fights where fighter_a = ? or fighter_b = ?', (id, id)).fetchall()
    fmts = ['%b. %d, %Y', '%b %d, %Y', '%B %d, %Y']

    def _try_format(s, fmt):
        try:
            datetime.strptime(s, fmt)
            return True
        except ValueError:
            return False
    
    sorted_records = sorted(records, key=lambda x: next(datetime.strptime(x['date'], fmt) for fmt in fmts if _try_format(x['date'], fmt)), reverse=True)
    sorted_fights = sorted(fights, key=lambda x: next(datetime.strptime(x['date'], fmt) for fmt in fmts if _try_format(x['date'], fmt)), reverse=True)


    if not records:
        return None

    career = db.execute('select * from aggregate_career where fighter_id = ?', (id,)).fetchone()
    if career and (cached == True):
        last_5 = sorted_records[0:5] if len(sorted_records) >= 5 else sorted_records
        last_5 = last_5[::-1]
        for i in sorted_records:
            i = dict(i)
            if i['weight_class']:
                divisions[i['weight_class'].lower().replace(' ', '').replace('women\'s', '')] += 1

        career_hash = {
            'fighter_id':id,
            'ufc_fights':career['ufc_fights'],
            'highest_win_streak':career['highest_win_streak'],
            'wins':career['wins'],
            'losses':career['losses'],
            'draws':career['draws'],
            'last_5':last_5,
            'finishes':career['finishes'],
            'debut':career['debut'],
            'last_fight':career['last_fight'],
            'cage_time':career['cage_time'],
            'win_rate': career['win_rate'],
            'finish_rate': career['finish_rate'],
            'average_fight_time': career['average_fight_time'],
            'title_fights': career['title_fights'],
            'subs': career['subs'],
            'ko_tko': career['ko_tko'],
            'decisions': career['decisions'],
            'num_of_mins': career['num_of_mins'],
            'divisions':divisions,
        }

        return career_hash



    fighter = db.execute('select * from fighters where fighter_id = ?;', (id,)).fetchall()
    fighter = dict(fighter[0])
    num_of_mins = get_fighter_minutes(records)
    

    last_5 = sorted_fights[0:5] if len(sorted_fights) >= 5 else sorted_fights
    last_5 = last_5[::-1]
    win_streak = 0
    rounds=0
    last_round_time_seconds=0
    finishes=0
    round_end_times = []
    title_fights = 0
    decisions = 0
    subs = 0
    ko_tko = 0

    win_streaks = []
    current_streak = 0


    for i in sorted_records:
        i = dict(i)
        if i['result'] == 'win':
            current_streak += 1
        else:
            if current_streak > 0:
                win_streaks.append(current_streak)
            current_streak = 0

        if i['method'] and i['result'] == 'win':
            if i['method'].strip() not in ['U-DEC', 'M-DEC', 'S-DEC', 'CNC', "Overturned"]:
                finishes += 1
            else:
                decisions += 1
            if 'SUB' in i['method']:
                subs += 1
            elif 'KO' in i['method']:
                ko_tko += 1
        if i['is_title_fight'] == 'yes':
            title_fights += 1
        if i['weight_class']:
            divisions[i['weight_class'].lower().replace(' ', '').replace('women\'s', '')] += 1
        minute, second = map(int, i['fight_time'].split(':'))
        round_end_times.append(f'{rounds * 5 + minute}:{second}')

    if current_streak > 0:
        win_streaks.append(current_streak)

    win_streak = sorted(win_streaks, reverse=True)[0] if win_streaks else 0
    
    h = int(num_of_mins // 60)
    remaining_minutes = num_of_mins - (h * 60)

    m = int(remaining_minutes)
    s = int(round((remaining_minutes - m) * 60))

    if s == 60:
        m += 1
        s = 0

    if m == 60:
        h += 1
        m = 0


    row = sorted_records[-1]
    debut = dict(row)['date']

    row = sorted_records[0]
    last_fight = dict(row)['date']

    avg_fight_minutes = num_of_mins / len(records) if len(records) != 0 else 0

    avg_m = 0
    avg_s = 0

    if avg_fight_minutes:
        avg_m = int(avg_fight_minutes)
        avg_s = int(round((avg_fight_minutes - avg_m) * (60)))

    if avg_s == 60:
        avg_m += 1
        avg_s = 0

    total_fights = (
        int(fighter['wins']) +
        int(fighter['losses']) +
        int(fighter['draws'])
    )

    win_rate = round(int(fighter['wins']) / total_fights, 2) if total_fights else 0
    finish_rate = round(finishes/len(sorted_records), 2) if sorted_records else 0

    career_hash = {
        'fighter_id':id,
        'ufc_fights':len(sorted_records),
        'highest_win_streak':win_streak,
        'wins':fighter['wins'],
        'losses':fighter['losses'],
        'draws':fighter['draws'],
        'last_5':last_5,
        'finishes':finishes,
        'debut':debut,
        'last_fight':last_fight,
        'cage_time':f'{str(h).zfill(2)}:{str(m).zfill(2)}:{str(s).zfill(2)}',
        'win_rate': win_rate,
        'finish_rate': finish_rate,
        'average_fight_time':f"{str(avg_m).zfill(2)}:{str(avg_s).zfill(2)}",
        'title_fights':title_fights,
        'subs':subs,
        'ko_tko':ko_tko,
        'decisions':decisions,
        'num_of_mins':num_of_mins,
        'divisions':divisions
    }

    return career_hash

def global_rating(conn=None, id=None, career_hash=None):
    '''returns -1 if not found'''
    conn.row_factory = sq.Row
    db = conn.cursor()
    # I want to give some grace to specialists but still give crazy boost to well rounded fighters
    wrestling_grace = 0.5 # there should be a grace for wrestling if the specialist has good power and good tk defense and even good bjj game
    bjj_grace = 0.5 #there should be a grace for bjj if the specialist has good wrestling control, bjj defence and good striking
    striking_grace = 0.5 # there should be a grace for a bad striker if he has tk threat, striking defence and clinch
    gnp_grace = 0.5 # there should be a grace for gnp if the fighter has good sub game and clinch game
    fighter = db.execute('''
    select g.Wrestling, g.BJJ, g.GNP, g.bjj_defence_scaled, g.td_def_scaled, g.opp_avg_elo_scaled, g.opp_avg_elo, g.effective_sub_threat_scaled, g.effective_control_scaled, g.tk_acc_scaled,
        s.KickBoxing, s.Boxing, s.str_def_scaled, s.SApM_scaled, s.true_ko_power_scaled as power,
        c.Muay 
        from aggregate_grappling g
        join aggregate_striking s on g.fighter_id = s.fighter_id
        join aggregate_clinching c on g.fighter_id = c.fighter_id
        where g.fighter_id = ?
    ''', (id,)).fetchone()

    if fighter == None:
        return -1
    
    wrestling_score = fighter['Wrestling']
    bjj_score = fighter['BJJ']
    gnp_score = fighter['GNP']

    striking_score = 0.75 * max(fighter['Boxing'], fighter['KickBoxing']) + 0.15 * min(fighter['Boxing'], fighter['KickBoxing']) + 0.1 * fighter['Muay']
    
    if wrestling_score < 45:
        wrestling_grace += (0.55 * fighter['td_def_scaled'] + 0.25 * fighter['power'] + 0.2 * fighter["effective_sub_threat_scaled"]) / 200

    if bjj_score < 45:
        bjj_grace += (0.55 * fighter['bjj_defence_scaled'] + 0.225 * fighter['effective_control_scaled'] + 0.225 * fighter['power']) / 200
    
    if gnp_score < 45:
        gnp_grace += (0.6 * fighter['effective_sub_threat_scaled'] + 0.4 * fighter['Muay']) / 200    
    if striking_score < 45:
        striking_grace += (0.5 * fighter['str_def_scaled'] + 0.3 * fighter['tk_acc_scaled'] + 0.2 * fighter['Muay']) / 200

    BASE_EXP = 1.0          # normal punishment
    MIN_EXP  = 0.35         # maximum forgiveness

    wrestling_grace = min(wrestling_grace, 1.0)
    bjj_grace = min(bjj_grace, 1.0)
    striking_grace = min(striking_grace, 1.0)
    gnp_grace = min(gnp_grace, 1.0)

    wrestling_k = max(0.35, 1.0 - wrestling_grace)
    bjj_k       = max(0.35, 1.0 - bjj_grace)
    striking_k = max(0.35, 1.0 - striking_grace)
    gnp_k       = max(0.35, 1.0 - gnp_grace)

    PIVOT = 60  # elite threshold

    def adjusted_score(score, k):
        if score >= PIVOT:
            return score
        return (score / PIVOT) ** k * PIVOT


    wrestling_adj = adjusted_score(wrestling_score, wrestling_k)
    bjj_adj       = adjusted_score(bjj_score, bjj_k)
    striking_adj  = adjusted_score(striking_score, striking_k)
    gnp_adj       = adjusted_score(gnp_score, gnp_k)


    weights = {
        "wrestling": 1.0,
        "bjj": 1.0,
        "striking": 1.0,
        "gnp": 0.25 
    }

    numerator = (
        wrestling_adj ** weights["wrestling"] *
        bjj_adj       ** weights["bjj"] *
        striking_adj  ** weights["striking"] *
        gnp_adj       ** weights["gnp"]
    )

    global_rating = numerator ** (1 / sum(weights.values()))

    global_rating *= (1 + 0.25 * fighter['opp_avg_elo_scaled'])

    gl_hash = {
        'fighter_id':[id],
        'wrestling':[wrestling_score],
        'bjj':[bjj_score],
        'striking':[striking_score],
        'boxing':[fighter['Boxing']],
        'kickboxing':[fighter['KickBoxing']],
        'gnp':[gnp_score],
        'wrestling_adj':[wrestling_adj],
        'bjj_adj':[bjj_adj],
        'gnp_acj':[gnp_adj],
        'striking_adj':[striking_adj],
        'global_rating':[global_rating]
    }

    df = pd.DataFrame(gl_hash)
    print(id, '\n', df.to_string())
    return df

def career_ranking_analysis(conn=None, fighter_id=2373):
    conn.row_factory = sq.Row
    db = conn.cursor()

    #based on divisional skill level
    division_boost={
        'strawweight':1.2,
        'flyweight':1.3,
        'bantamweight':1.4,
        'featherweight':1.4,
        'lightweight':1.5,
        'welterweight':1.4,
        'middleweight':1.3,
        'lightheavyweight':1.2,
        'heavyweight':1.1,
        'openweight':1,
        'catchweight':1,
        'superheavyweight':1
    }

    elo_rows = db.execute('select * from elo_history where fighter_1 = ? or fighter_2 = ?', (fighter_id, fighter_id)).fetchall()
    if not elo_rows:
        return -1
    career = career_analysis(db=db, id=fighter_id)

    loss_elo = []
    win_elo = []
    opp = ''
    for row in elo_rows:
        if row['fighter_1'] == fighter_id:
            opp = 'elo_2'
        else:
            opp = 'elo_1' 

        if row['winner'] == fighter_id:
            win_elo.append(row[opp])
        elif row['winner'] != fighter_id:
            loss_elo.append(row[opp])
    

    avg_win_elo = sum(win_elo) / len(win_elo) if len(win_elo) > 0 else 0
    avg_loss_elo = sum(loss_elo) / len(loss_elo) if len(loss_elo) > 0 else 0
    fighter_elo = elo_analysis(fighter_id)
    
    win_rate = career['win_rate']
    finish_rate = career['finish_rate']
    ko_tko = career['ko_tko']
    subs = career['subs']
    title_fights = career['title_fights']
    last_5 = career['last_5']
    highest_win_streak = career['highest_win_streak']
    num_of_fights = len(elo_rows)
    top_division = max(career['divisions'], key=career['divisions'].get)

    df = pd.read_sql_query('select * from elo', conn)

    min_elo = df['elo'].min()
    max_elo = df['elo'].max()

    peak_elo = max(fighter_elo['peak_elo'], fighter_elo['elo'])
    df['peak_elo'] = peak_elo

    min_maxed_peak_elo = (peak_elo - min_elo) / (max_elo - min_elo)
    min_maxed_win_elo = (avg_win_elo - min_elo) / (max_elo - min_elo) if avg_win_elo > 0 else 0
    min_maxed_loss_elo = (avg_loss_elo - min_elo) / (max_elo - min_elo) if avg_loss_elo > 0 else 0

    def safe_div(a, b, default=0):
        return a / b if b != 0 else default

    def clamp(x, low=0, high=1):
        return max(low, min(high, x))


    win_score = (
        (1 + win_rate) *
        (1 + finish_rate) *
        (1 + math.log1p(highest_win_streak))
    )

    title_score = math.log1p(title_fights) ** 0.75

    title_score = title_score * division_boost[top_division] if top_division else title_score


    fall_off_rate = clamp((fighter_elo['peak_elo'] - fighter_elo['elo']) / fighter_elo['peak_elo'])

    elo_score = (
        0.5 * min_maxed_peak_elo +
        0.4 * min_maxed_win_elo -
        0.1 * min_maxed_loss_elo
    )

    elo_score = clamp(elo_score)

    # fall_off_penalty = 1 - (0.3 * fall_off_rate)
    # career_score = (
    #     0.5 * elo_score +
    #     0.3 * win_score +
    #     0.2 * title_score
    # )

    # career_score *= fall_off_penalty
    # career_score = math.log1p(career_score) * 100

    fall_off_penalty = 1 - (0.3 * fall_off_rate)


    for key, value in career.items():
        career[key] = [value]

    df_1 = pd.DataFrame(career)
    df_1 = df_1.drop('last_5', axis=1)
    df_1 = df_1.drop('divisions', axis=1)

    df_2 = pd.DataFrame(
        {
            'fighter_id':[fighter_id],
            'peak_elo':[peak_elo],
            'elo':[fighter_elo['elo']],
            'min_maxed_peak_elo':[min_maxed_peak_elo],
            'min_maxed_win_elo':[min_maxed_win_elo],
            'min_maxed_loss_elo':[min_maxed_loss_elo],
            'elo_score':[elo_score],
            'fall_off_rate':[fall_off_rate],
            'fall_off_penalty':[fall_off_penalty],
            'title_score':[title_score],
            'win_score':[win_score],
        }
    )


    fighter_df = pd.merge(df_1, df_2, on='fighter_id')
    # print(fighter_df)
    # print(fighter_df['highest_win_streak'])

    # print(fighter_df)
    # print(highest_win_streak, min_maxed_loss_elo, finish_rate, win_rate)
    print(fighter_id, '\n', fighter_df.to_string())
    return fighter_df



def elo_analysis(id):
    '''analyses elo data from the fighter tab in my_app'''
    with sq.connect(db_path) as conn:
        conn.row_factory = sq.Row
        db = conn.cursor()
        row = db.execute('select * from elo where fighter_id = ?', (id,)).fetchone()
        elo = row['elo']
        peak_elo = elo
        elo_history = db.execute('select * from elo_history where fighter_1 = ? or fighter_2 = ?', (id, id)).fetchall()
        peak_elos = db.execute('''
                                select date,
                                case 
                                    when fighter_1 = ? then new_elo_1
                                    when fighter_2 = ? then new_elo_2
                                end as elo
                                from elo_history 
                                where fighter_1 = ? or fighter_2 = ?'''
                                , (id, id, id, id)).fetchall()
        
        peak_elo = max([row['elo'] for row in peak_elos])
        for row in peak_elos:
            if row['elo'] == peak_elo:
                peak_fight = row['date']
                

        columns = [desc[0] for desc in db.execute('select * from elo_history').description]


        latest_date = elo_history[-1]['date'] if elo_history else None
        # peak_fight = 0
        # for fight in elo_history:
        #     if float(fight['elo_1']) == peak_elo and fight['fighter_1'] == id:
        #         peak_fight = fight['date']
        #         break
        #     elif float(fight['elo_2']) == peak_elo and fight['fighter_2'] == id:
        #         peak_fight = fight['date']
        #         break

        elo_hash = {
            'elo':elo,
            'peak_elo':peak_elo,
            'latest_fight':latest_date,
            'peak_fight':peak_fight
        }

    return elo_hash


def fight_analysis(db, fight_id):
    fight = db.execute("""
        SELECT e.*, f.date, f.event_id
        FROM fights_extended e 
        JOIN fights f ON e.fight_id = f.fight_id 
        WHERE e.fight_id = ?
    """, (fight_id,)).fetchone()


    if not fight:
        return None
    
    event = db.execute("""
        select * from events where event_id = ?
    """, (fight['event_id'],)).fetchone()

    fight = dict(fight)
    fight_data = fight['fight_data']

    elo_fight = db.execute("""
        SELECT * FROM elo_history 
        WHERE date = ? AND (
            (fighter_1 = ? AND fighter_2 = ?) OR 
            (fighter_1 = ? AND fighter_2 = ?)
        )
    """, (
        fight['date'],
        fight['fighter_1'], fight['fighter_2'],
        fight['fighter_2'], fight['fighter_1']
    )).fetchone()

    elo_fight = dict(elo_fight) if elo_fight else {}

    name_1 = db.execute(
        "SELECT * FROM fighters WHERE fighter_id = ?",
        (fight['fighter_1'],)
    ).fetchone()

    name_2 = db.execute(
        "SELECT * FROM fighters WHERE fighter_id = ?",
        (fight['fighter_2'],)
    ).fetchone()

    event_name = db.execute(
        "SELECT event_name FROM events WHERE event_id = ?",
        (fight['event_id'],)
    ).fetchone()

    fight['fighter_a'] = dict(name_1) if name_1 else {}
    fight['fighter_b'] = dict(name_2) if name_2 else {}

    fight['event_name'] = event_name['event_name'] if event_name else None
    fight['date'] = fight['date'].replace('.', '')

    if elo_fight:
        if elo_fight['fighter_1'] == fight['fighter_1']:
            fight['fighter_a']['elo'] = elo_fight['elo_1']
            fight['fighter_b']['elo'] = elo_fight['elo_2']
            fight['fighter_a']['new_elo'] = elo_fight['new_elo_1']
            fight['fighter_b']['new_elo'] = elo_fight['new_elo_2']
        else:
            fight['fighter_a']['elo'] = elo_fight['elo_2']
            fight['fighter_b']['elo'] = elo_fight['elo_1']
            fight['fighter_a']['new_elo'] = elo_fight['new_elo_2']
            fight['fighter_b']['new_elo'] = elo_fight['new_elo_1']

    parsed = normalize_fight(db, fight_id)
    fighter_a = parsed['fighter_a']
    fighter_b = parsed['fighter_b']

    return fight, event, fighter_a, fighter_b

def s_analysis(db, id):
    '''this function is responsible for making a hash to make striking data very easy to get'''
    striking_data = db.execute('select * from aggregate_striking where fighter_id = ?', (id,)).fetchall()


    ...

'''-------- here starts the functions that are responsible for creating the aggregate analysis tables in the database'''

def fighter_striking_analysis(fighter_id, conn=None):
    '''what is important: knockdowns (KD), sig distance body/head/leg strikes attempts (sdbl/A or sdhl/A or sdll/A), sig strikes attempts SSA, sig strikes landed ssl, 
    total strike attempts TSA and landed TSL, strikes absorbed per min, strike accuracy, strikes landed per min, str def precentag'''
    conn.row_factory = sq.Row
    db = conn.cursor()
    name = db.execute('select name from fighters where fighter_id = ?', (fighter_id,)).fetchone()
    # query_2 = 'select * from advanced_striking'
    rows = db.execute('select * from records where fighter_1 = ?', (fighter_id,)).fetchall()
    num_of_fights = len(rows)
    num_of_mins = get_fighter_minutes(rows)
    # print(num_of_mins)

    stats_df = pd.read_sql_query(
        "SELECT fighter_id, SLpM, SApM, str_def FROM advanced_stats WHERE fighter_id=?",
        conn,
        params=(fighter_id,)
    )

    df = pd.read_sql_query(
        "SELECT * FROM advanced_striking WHERE fighter_id=?",
        conn,
        params=(fighter_id,)
    ) 

    df = df.drop_duplicates()
    df = df.replace({'-': None})
    df = df.map(lambda x: x.replace("\n", "") if isinstance(x, str) else x)
    df = df.dropna(subset=['sdhl_a', 'sdbl_a', 'sdll_a', 'tsl', 'tsa', 'ssl', 'ssa']) 
    try:  
        df[['sdhl', 'sdha']] = df['sdhl_a'].str.split('/', expand=True)
        df[['sdbl', 'sdba']] = df['sdbl_a'].str.split('/', expand=True)
        df[['sdll', 'sdla']] = df['sdll_a'].str.split('/', expand=True)
    except Exception as e:
        logger.error(f'COULDNT GET IT, HIS NAME IS {name['name']}')
    
    df = non_ufc_fight_remover(fighter_id, df, db)



    df[['sdhl', 'sdha', 'sdbl', 'sdba', 'sdll', 'sdla', 'tsl', 'tsa', 'ssl', 'ssa']] = (
        df[['sdhl', 'sdha', 'sdbl', 'sdba', 'sdll', 'sdla', 'tsl', 'tsa', 'ssl', 'ssa']].astype(int)
    )


    df = df.drop(columns=['sdhl_a', 'sdbl_a', 'sdll_a', 'tsl_tsa', 'date', 'opponent', 'espn_url', 'res'])

    percent_cols = ['body_percentage', 'head_percentage', 'leg_percentage']

    for col in percent_cols:
        df[col] = (
            df[col]
            .astype(str)              # force string
            .str.replace('%', '', regex=False)
            .replace('None', None)
        )
        df[col] = pd.to_numeric(df[col], errors='coerce') / 100

    # Step 1: aggregate your numeric stats
    agg_df = df.groupby(['fighter_id']).agg({
        'tsl':'sum',
        'tsa':'sum',
        'ssl':'sum',
        'ssa':'sum',
        'kd':'sum',
        'body_percentage':'mean',
        'head_percentage':'mean',
        'leg_percentage':'mean',
        'sdhl':'sum',
        'sdha':'sum',
        'sdbl':'sum',
        'sdba':'sum',
        'sdll':'sum',
        'sdla':'sum'
    }).reset_index()  # reset_index keeps fighter_id as a column

    # Step 2: merge the SLpM, SApM, str_def from stats_df
    agg_df = agg_df.merge(
        stats_df[['fighter_id', 'SLpM','SApM','str_def']],  # only the columns you need
        on="fighter_id",                
        how='left'                          
    )
    # Step 3: convert str_def to numeric correctly
    agg_df['str_def'] = (
        agg_df['str_def'].astype(str).str.replace('%','', regex=False)
    )
    agg_df['str_def'] = pd.to_numeric(agg_df['str_def'], errors='coerce') / 100

    df = agg_df

    #---- more accruate defense -----
    total_elo = 0
    total_opponents = 0
    for opp in rows:
        x = db.execute('select elo from elo where fighter_id = ?', (opp['fighter_2'],)).fetchone()[0]
        total_elo += x
        total_opponents += 1
    all_avg_opp_elo = total_elo/total_opponents if total_opponents > 0 else 1100
    df['str_def'] = df['str_def'] * (all_avg_opp_elo/1200)
    #---------------------------------

    
    # df['ts_acc'] = df['tsl'] / df['tsa'] #total strikes
    # df['ss_acc'] = df['ssl'] / df['ssa'] #significant strikes
    # df['sdh_acc'] = df['sdhl'] / df['sdha'] #significant distance head strikes
    # df['sdb_acc'] = df['sdbl'] / df['sdba'] #significant distance body strikes
    # df['sdl_acc'] = df['sdll'] / df['sdla'] #significant distance leg strikes

    def bayes_rate(successes, attempts, league_avg, prior_attempts):
        return (successes + league_avg * prior_attempts) / (attempts + prior_attempts)

    df['ts_acc']  = bayes_rate(df['tsl'],  df['tsa'],  LEAGUE_AVGS['ts_acc'], PRIOR_ATTEMPTS)
    df['ss_acc']  = bayes_rate(df['ssl'],  df['ssa'],  LEAGUE_AVGS['ss_acc'], PRIOR_ATTEMPTS)
    df['sdh_acc'] = bayes_rate(df['sdhl'], df['sdha'], LEAGUE_AVGS['sdh_acc'], PRIOR_ATTEMPTS)
    df['sdb_acc'] = bayes_rate(df['sdbl'], df['sdba'], LEAGUE_AVGS['sdb_acc'], PRIOR_ATTEMPTS)
    df['sdl_acc'] = bayes_rate(df['sdll'], df['sdla'], LEAGUE_AVGS['sdl_acc'], PRIOR_ATTEMPTS)


    df['total_ssl_acc'] = df['ssl'] / df['tsa'] #accuracy of significant strikes landed compared to total strikes attempted
    df['total_ssa_percentage'] = df['ssl'] / df['tsl'] #how many of the fighter's landed strikes are significant
    df['tsl_pm'] = df['tsl'] / num_of_mins #measures the true volume of a fighter
    df['kd_pm'] = df['kd'] / num_of_mins

    opp_avg_elo = 0
    true_ko_power = 0
    avg_opp_durability = 0

    if len(rows) > 0:
        ko_hash = ko_power(db, rows, num_of_mins)
        ko_list = ko_hash['ko_list']
        ko_pm = ko_hash['ko_pm']
        df['ko_pm'] = ko_pm
        opp_avg_elo = ko_hash['opponent_average_elo']
        opp_str_def = 0
        opp_sapm = 0

        ko_types = []
        num_of_opps = len(ko_list)
        if num_of_opps > 0:
            for opp in ko_list:
                opp_str_def += opp['opponent_str_def']
                opp_sapm += opp['opponent_sapm']
                ko_types.append(opp['ko_type'])
        print(ko_list, ko_types)

        avg_opp_durability = opp_avg_elo * 0.6 + (opp_str_def/num_of_opps) * 0.3 + ((1/(opp_sapm/num_of_opps)) * 0.1) if (num_of_opps != 0 and opp_sapm != 0) else 0

        def bayes_rate_pm(count, mins, league_avg, prior_mins):
            return (count + league_avg * prior_mins) / (mins + prior_mins)

        ko_pm_adj = bayes_rate_pm(len(ko_list), num_of_mins, LEAGUE_AVGS['ko_pm'], PRIOR_MINS)
        kd_pm_adj = bayes_rate_pm(df['kd'], num_of_mins, LEAGUE_AVGS['kd_pm'], PRIOR_MINS)

        true_ko_power = (0.75 * ko_pm_adj + 0.25 * kd_pm_adj) * avg_opp_durability

    


    df['true_ko_power'] = true_ko_power
    df['avg_ko_opp_durability'] = avg_opp_durability
    df['avg_ko_opp_elo'] = opp_avg_elo

    #------------------- late additions lol
    df['effective_volume'] = df['tsl_pm'] * df['ts_acc']
    df['leg_kicks'] = (df['sdll'] / num_of_mins) * df['sdl_acc']
    df['sig_acc'] = df['ss_acc'] * ((0.5*df['sdh_acc']) + (0.25*df['sdb_acc']) + (0.25*df['sdl_acc']))

    # df = df.round({
    #     'ts_acc':2,
    #     'ss_acc':2,
    #     'sdh_acc':2,
    #     'sdb_acc':2,
    #     'sdl_acc':2,
    #     'total_ssl_acc':2,
    #     'total_ssa_percentage':2,
    #     'kd_pm':2,
    #     'tsl_pm':2,
    #     'true_ko_power':2,
    #     'avg_ko_opp_durability': 2,
    #     'avg_ko_opp_elo': 2,
    #     'effective_volume':2,
    #     'leg_kicks':2,
    #     'sig_acc':2
    # })

    print(fighter_id, '\n', df.to_string())
    return df


def fighter_clinch_analysis(fighter_id, conn=None):
    conn.row_factory = sq.Row
    db = conn.cursor()
    # query_2 = 'select * from advanced_striking'
    rows = db.execute('select * from records where fighter_1 = ?', (fighter_id,)).fetchall()
    num_of_fights = len(rows)
    num_of_mins = get_fighter_minutes(rows)
    # print(num_of_mins)

    df = pd.read_sql_query(
    'select * from advanced_clinch where fighter_id = ?',
    conn,
    params=(fighter_id,)
    )
    
    df = df.drop_duplicates()
    df = df.replace({'-': None})
    df = df.map(lambda x: x.replace("\n", "") if isinstance(x, str) else x)
    # sc stands for significant clinch strikes. h: head, b:body, l:leg
    df = df.dropna(subset=['scbl', 'scba', 'schl', 'scha', 'scll', 'scla'])   

    
    df = non_ufc_fight_remover(fighter_id, df, db)



    df[['scbl', 'scba', 'schl', 'scha', 'scll', 'scla']] = (
        df[['scbl', 'scba', 'schl', 'scha', 'scll', 'scla']].astype(int)
    )

    df = df.drop(columns=['rv', 'sr', 'tdl', 'tda', 'tds', 'tk_acc', 'date', 'opponent', 'espn_url', 'res'])

    if df.empty:
        return pd.DataFrame()


    df = (df.groupby(['fighter_id']).agg({
        'scbl':'sum',
        'scba':'sum',
        'schl':'sum',
        'scha':'sum',
        'scll':'sum',
        'scla':'sum'
    })).reset_index()

    
    # df['ts_acc'] = df['tsl'] / df['tsa'] #total strikes
    # df['ss_acc'] = df['ssl'] / df['ssa'] #significant strikes
    # df['sdh_acc'] = df['sdhl'] / df['sdha'] #significant distance head strikes
    # df['sdb_acc'] = df['sdbl'] / df['sdba'] #significant distance body strikes
    # df['sdl_acc'] = df['sdll'] / df['sdla'] #significant distance leg strikes

    def bayes_rate(successes, attempts, league_avg, prior_attempts):
        return (successes + league_avg * prior_attempts) / (attempts + prior_attempts)

    # df['ts_acc']  = bayes_rate(df['tsl'],  df['tsa'],  LEAGUE_AVGS['ts_acc'], PRIOR_ATTEMPTS)
    # df['ss_acc']  = bayes_rate(df['ssl'],  df['ssa'],  LEAGUE_AVGS['ss_acc'], PRIOR_ATTEMPTS)
    # df['sdh_acc'] = bayes_rate(df['sdhl'], df['sdha'], LEAGUE_AVGS['sdh_acc'], PRIOR_ATTEMPTS)
    # df['sdb_acc'] = bayes_rate(df['sdbl'], df['sdba'], LEAGUE_AVGS['sdb_acc'], PRIOR_ATTEMPTS)
    # df['sdl_acc'] = bayes_rate(df['sdll'], df['sdla'], LEAGUE_AVGS['sdl_acc'], PRIOR_ATTEMPTS)

    prior_attempts = 15

    df['scb_acc'] = df['scbl'] / df['scba'].replace(0, pd.NA)
    df['scl_acc'] = df['scll'] / df['scla'].replace(0, pd.NA)
    df['sch_acc'] = df['schl'] / df['scha'].replace(0, pd.NA)

    df['scb_acc'] = bayes_rate(df['scbl'], df['scba'], LEAGUE_AVGS['scb_acc'], prior_attempts)
    df['scl_acc'] = bayes_rate(df['scll'], df['scla'], LEAGUE_AVGS['scl_acc'], 5)
    df['sch_acc'] = bayes_rate(df['schl'], df['scha'], LEAGUE_AVGS['sch_acc'], prior_attempts)

    df = df.fillna(0)

    df['total_attempted'] = df['scba'] + df['scha'] + df['scla']
    df['total_landed'] = df['scbl'] + df['schl'] + df['scll']

    df['clinch_strikes_pm'] = df['total_landed'] / num_of_mins if num_of_mins != 0 else 0
    df['effective_accuracy'] = 0.45 * df['scb_acc'] + 0.10 * df['scl_acc'] + 0.45 * df['sch_acc'] 
    
    df = df.round({
        'effective_accuracy':4
    })

    print(fighter_id, '\n', df.to_string())
    return df

def fighter_grappling_analysis(name, fighter_id, conn=None):
    conn.row_factory = sq.Row
    db = conn.cursor()
    # query_2 = 'select * from advanced_striking'
    record_rows = db.execute('select * from records where fighter_1 = ?', (fighter_id,)).fetchall()

    elo_rows = db.execute(
        '''
        SELECT
            CASE
                WHEN fighter_1 = ? THEN elo_2
                WHEN fighter_2 = ? THEN elo_1
            END AS opp_elo
        FROM elo_history
        WHERE fighter_1 = ? OR fighter_2 = ?
        ''',
        (fighter_id, fighter_id, fighter_id, fighter_id)
    ).fetchall()

    num_of_fights = len(record_rows)
    num_of_mins = get_fighter_minutes(record_rows)
    opp_avg_elo = sum([int(i['opp_elo']) for i in elo_rows]) / len(elo_rows) if len(elo_rows) > 0 else 0
    num_of_rounds = sum([int(i['round_num']) for i in record_rows])

    opp_ids = [row['fighter_2'] for row in record_rows]

    # print(num_of_mins)

    sub_count = 0
    subs_conceded = 0
    for row in record_rows:
        if 'sub' in row['method'].lower() and 'win' in row['result'].lower():
            sub_count += 1
        elif 'sub' in row['method'].lower() and 'loss' in row['result'].lower():
            subs_conceded += 1

    sub_attempts_faced = 0
    total_opp_control_time = 0
    for opp in opp_ids:
        opp_attempted_subs = db.execute('select sm from advanced_ground where fighter_id = ? and LOWER(opponent) like LOWER(?)', (opp, name)).fetchone()
        ctrl_time = db.execute('select control_time from advanced_stats where fighter_id = ?', (opp,)).fetchone()
        if ctrl_time and ctrl_time['control_time'] is not None:
            total_opp_control_time += ctrl_time['control_time']

        if opp_attempted_subs and opp_attempted_subs['sm'] is not None:
            sub_attempts_faced += opp_attempted_subs['sm']


    opp_avg_ctrl_time = total_opp_control_time / len(opp_ids) if opp_ids else 0

    # Step 1: Only join fight-level tables first
    df = pd.read_sql_query(
        f'''
        select g.*, c.tds, c.tdl, c.tda, c.tk_acc, c.rv, s.control_time, s.sub_avg, s.td_def
        from advanced_ground g 
        join advanced_clinch c on g.fighter_id = c.fighter_id and g.date = c.date
        join advanced_stats s on g.fighter_id = s.fighter_id
        where g.fighter_id = ?
        ''',
        conn,
        params=(fighter_id,)
    )

    # Step 2: Remove non-UFC fights
    df = non_ufc_fight_remover(fighter_id, df, db)

    # Step 3: Now join career stats if needed
    # stats_df = pd.read_sql_query(
    #     'select * from advanced_stats where fighter_id = ?',
    #     conn,
    #     params=(fighter_id,)
    # )
    # # Add career stats to every fight row (broadcast merge)
    # for col in stats_df.columns:
    #     if col != 'fighter_id':
    #         df[col] = stats_df.iloc[0][col]  # same career stats for all rows


    
    df = df.drop_duplicates()
    df = df.replace({'-': None})
    df = df.map(lambda x: x.replace("\n", "") if isinstance(x, str) else x)
    # sc stands for significant clinch strikes. h: head, b:body, l:leg
    df = df.dropna(subset=['tdl', 'tda', 'control_time', 'tds', 'tk_acc', 'rv', 'sgbl', 'sgba', 'sghl', 'sgha', 'sgll', 'sgla', 'sm', 'ad', 'adhg', 'adtb', 'adtm', 'adts'])   


    # df = non_ufc_fight_remover(fighter_id, df, db)


    df[['tdl', 'tda', 'tds', 'rv', 'sgbl', 'control_time', 'sgba', 'sghl', 'sgha', 'sgll', 'sgla', 'sm', 'ad', 'adhg', 'adtb', 'adtm', 'adts']] = (
        df[['tdl', 'tda', 'tds', 'rv', 'sgbl', 'control_time', 'sgba', 'sghl', 'sgha', 'sgll', 'sgla', 'sm', 'ad', 'adhg', 'adtb', 'adtm', 'adts']].astype(float)
    )

    def bayesian_pm(count, minutes, league_pm, prior_minutes):
        return (count + league_pm * prior_minutes) / (minutes + prior_minutes)

    def bayes_rate(successes, attempts, league_avg, prior_attempts):
        return (successes + league_avg * prior_attempts) / (attempts + prior_attempts)
    

    if df.empty:
        return pd.DataFrame()

    df['tk_acc'] = df['tk_acc'].str.replace('%', '').astype(float) / 100
    df['td_def'] = df['td_def'].str.replace('%', '').astype(float) / 100

    # print(df)

    # Step 1: Group only fight-level stats
    fight_cols = ['tdl','tda','tds','tk_acc','rv','sgbl','sgba','sghl','sgha','sgll','sgla','sm','ad','adhg','adtb','adtm','adts']
    df_fight = df.groupby(['fighter_id']).agg({col:'sum' if col != 'tk_acc' else 'mean' for col in fight_cols}).reset_index()

    # Step 2: Add career-level stats back
    career_cols = ['sub_avg','td_def','control_time']
    for col in career_cols:
        if col in df.columns:
            df_fight[col] = df[col].iloc[0]

    df = df_fight


    df['td_pm'] = bayesian_pm(
        df['tdl'],
        num_of_mins,
        LEAGUE_AVGS['td_pm'],
        prior_minutes=120
    )


    # ------------- Ground and Pound ----------------
    
    df['sgh_acc'] = bayes_rate(df['sghl'], df['sgha'], LEAGUE_AVGS['sgh_acc'], 20)
    df['sgb_acc'] = bayes_rate(df['sgbl'], df['sgba'], LEAGUE_AVGS['sgb_acc'], 2)
    df['tk_acc'] = bayes_rate(df['tdl'], df['tda'], LEAGUE_AVGS['tk_acc'], 10)

    df['num_of_mins'] = num_of_mins
    # df['sgh_acc'] = df['sghl'] / df['sgha'].replace(0, np.nan)
    # df['sgb_acc'] = df['sgbl'] / df['sgba'].replace(0, np.nan)
    # df['sgl_acc'] = df['sgll'] / df['sgla'].replace(0, np.nan)

    # GNP per minute
    df['gnp_pm'] = bayesian_pm(
        df['sghl'] + df['sgbl'],
        num_of_mins,
        LEAGUE_AVGS['gnp_pm'],
        prior_minutes=120
    )
    # Compute weighted accuracy
    weighted_acc = 0.85 * df['sgh_acc'] + 0.15 * df['sgb_acc'] 
    df['gnp_pressure'] = np.log1p(df['gnp_pm'] / LEAGUE_AVGS['gnp_pm'])
    # Multiply by strike volume and a base
    df['effective_gnp'] = ((weighted_acc * (df['sghl'] + df['sgbl'])) ** 0.6) * (df['gnp_pressure'] ** 1.3)

    epsilon = 1e-3
    df['effective_gnp'] = (df['effective_gnp'] + epsilon)

    df['td_pressure'] = np.log1p(df['td_pm'] / LEAGUE_AVGS['td_pm'])

    # this one might be useless
    df['bjj_advances'] = (0.15*df['adhg'] + 0.35*df['adtb'] + 0.35*df['adtm'] + 0.15*df['adts']) / num_of_mins

    df['effective_takedowns'] = (
        df['td_pressure'] ** 0.6 *         
        df['tk_acc'] ** 1.3          
    )


    # ------ Effective Subs ------
    df['sub_attempts_pm'] = bayesian_pm(
        df['sm'],
        num_of_mins,
        LEAGUE_AVGS['sub_attempts_pm'],
        prior_minutes=120
    )

    df['subs_pm'] = bayesian_pm(
        sub_count,
        num_of_mins,
        LEAGUE_AVGS['subs_pm'],
        prior_minutes=120
    )

    df['sub_success_rate'] = df['subs_pm'] / df['sub_attempts_pm'].replace(0, np.nan)
    df['sub_success_rate'] = df['sub_success_rate'].fillna(0)

    df['opp_avg_elo'] = opp_avg_elo


    df['effective_sub_threat'] = (df['sub_attempts_pm'] ** 0.75) * (df['sub_success_rate'] ** 1.3) * ((df['opp_avg_elo'] + 1200) / 1200) ** 1.25

    df['control_pm'] = bayesian_pm(
        df['control_time'],
        num_of_mins,
        LEAGUE_AVGS['control_time'] / LEAGUE_AVGS['num_of_mins'],
        prior_minutes=120
    )



    df['effective_control'] = (
        np.log1p(df['control_pm'] / LEAGUE_AVGS['control_pm']) *
        ((df['opp_avg_elo'] + 1200) / 1200) ** 0.75
    )

    # ---------- BJJ defence ---------------
    #eps = 0.1

    df['subs_conceded_pm'] = bayesian_pm(
        subs_conceded,
        num_of_mins,
        LEAGUE_AVGS['subs_conceded_pm'],
        prior_minutes=120
    )

    df['sub_attempts_faced_pm'] = bayesian_pm(
        sub_attempts_faced,
        num_of_mins,
        LEAGUE_AVGS['sub_attempts_faced_pm'],
        prior_minutes=120
    )

    sub_term = np.exp(-1.5 * df['subs_conceded_pm'])
    attempt_term = np.exp(-0.7 * df['sub_attempts_faced_pm'])

    df['opp_avg_control'] = opp_avg_ctrl_time / LEAGUE_AVGS['control_time']
    df['bjj_defence'] = np.log1p(
        sub_term * attempt_term * ((df['opp_avg_elo'] + 1200) / 1200) * df['opp_avg_control'] ** 0.75
    )

    # print(fighter_id, '\n', df.to_string())
    
    df = df.fillna(0)

    print(fighter_id, '\n', df.to_string())
    return df

def total_fighting_analysis(art_style):
    with sq.connect(db_path) as conn:
        conn.row_factory = sq.Row
        db = conn.cursor()
        fighters = db.execute('select DISTINCT a.fighter_id, f.name from advanced_clinch a join fighters f on a.fighter_id = f.fighter_id').fetchall()
        features_to_scale = features[art_style]


        if art_style == 'striking':
            fighters_striking = []
            for fighter in fighters:
                strike_df = fighter_striking_analysis(fighter['fighter_id'], conn=conn)
                fighters_striking.append(strike_df)      
            
            fighters_df = pd.concat(fighters_striking).reset_index()

            # league_avgs = {
            #     'ts_acc': fighters_df['tsl'].sum() / fighters_df['tsa'].sum(),
            #     'ss_acc': fighters_df['ssl'].sum() / fighters_df['ssa'].sum(),
            #     'sdh_acc': fighters_df['sdhl'].sum() / fighters_df['sdha'].sum(),
            #     'sdb_acc': fighters_df['sdbl'].sum() / fighters_df['sdba'].sum(),
            #     'sdl_acc': fighters_df['sdll'].sum() / fighters_df['sdla'].sum(),
            # }

            # print(league_avgs)

            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.04, up=0.96)

                    
            fighters_df['SApM_scaled'] = 100 - fighters_df['SApM_scaled']

            fighters_df['Boxing'] = 0.35 * fighters_df['true_ko_power_scaled'] + 0.15 * fighters_df['effective_volume_scaled'] + 0.25 * fighters_df['str_def_scaled'] + 0.25 * fighters_df['SApM_scaled']
            fighters_df['KickBoxing'] = 0.25 * fighters_df['true_ko_power_scaled'] + 0.15 * fighters_df['effective_volume_scaled'] + 0.20 * fighters_df['str_def_scaled'] + 0.10 * fighters_df['SApM_scaled'] + 0.30 * fighters_df['leg_kicks_scaled']

        elif art_style == 'clinching':
            fighters_clinching = []
            for fighter in fighters:
                clinch_df = fighter_clinch_analysis(fighter['fighter_id'], conn=conn)
                fighters_clinching.append(clinch_df)      
            
            fighters_df = pd.concat(fighters_clinching).reset_index()

            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.04, up=0.96)

            fighters_df["Muay"] = 0.2 * fighters_df['clinch_strikes_pm_scaled'] + 0.8 * fighters_df['effective_accuracy_scaled']

        elif art_style == 'grappling':
            fighters_grappling = []
            for fighter in fighters:
                grappling_df = fighter_grappling_analysis(fighter['name'], fighter['fighter_id'], conn=conn)
                fighters_grappling.append(grappling_df)      
            
            fighters_df = pd.concat(fighters_grappling).reset_index()

            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.01, up=0.99)

            fighters_df["Wrestling"] = 0.34 * fighters_df['effective_takedowns_scaled'] + 0.33 * fighters_df['effective_control_scaled'] + 0.33 * fighters_df['td_def_scaled']
            fighters_df["BJJ"] = 0.7 * fighters_df['effective_sub_threat_scaled'] + 0.3 * fighters_df['bjj_defence_scaled']
            fighters_df['GNP'] = fighters_df['effective_gnp_scaled']
        
        elif art_style == "global":
            global_fighters = []
            for fighter in fighters:
                global_df = global_rating(id=fighter['fighter_id'], conn=conn)
                if isinstance(global_df, int):
                    continue
                global_fighters.append(global_df)      
            
            fighters_df = pd.concat(global_fighters).reset_index()

            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.005, up=0.995)

            # elite_stats = max(fighters_df['wrestling'], fighters_df['bjj'], fighters_df['striking'])

            # specialist_floor = elite_stats * 0.7
            # fighters_df['global_rating_scaled'] = max(fighters_df['global_rating_scaled'], specialist_floor)

        elif art_style == "career":
            fighter_career = []
            for fighter in fighters:
                career_df = career_ranking_analysis(conn, fighter['fighter_id'])
                if isinstance(career_df, int):
                    continue
                fighter_career.append(career_df)      
            
            fighters_df = pd.concat(fighter_career, ignore_index=True)

            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.0005, up=0.9995)

            fighters_df['career_score'] = 0.65 * fighters_df['elo_score_scaled'] + 0.35 * fighters_df['win_score_scaled']

            fighters_df['career_score'] *= fighters_df['fall_off_penalty']

            mask = fighters_df['title_score'] > 0.8

            fighters_df.loc[mask, 'career_score'] *= ( 1 + (fighters_df.loc[mask, 'title_score'] - 0.8) / ((3 - 0.8) * 3))

            fighters_df['career_score'] = fighters_df['career_score'].clip(upper=100)




        
        # fighters_df.head()
        # fighters_df.info()
        # fighters_df.shape
        # fighters_df.describe(include='all')

        fighters_df.to_sql(
            name=f"aggregate_{art_style}",
            con=conn,
            if_exists="replace",
            index=False
        )

        # print(striking_fighters_dict)


'''Here starts the analysis utitlity functions'''

def ko_power(db, fights, mins):
    '''KO power is the most abstract so it needs more effort to get an accurate measure of power
        How hard somebody hits relative to time, opportunity and the opponent's durability
    '''
    ko_num = 0
    opponents_average_elo = 0
    ko_list = []
    for fight in fights:
        if 'KO' in fight['method'] and 'win' in fight['result']:
            ko_dict = {}
            # ko_type = fight['method'].split(' ', 1)[1] if len(fight['method'].split(' ', 1)) > 1 else ''
            ko_type = fight['method']
            ko_dict['weight_class'] = (fight['weight_class'])
            ko_dict['ko_type'] = ko_type.replace('(', '').replace(')', '')
            ko_num += 1
            ko_dict['opponent'] = fight['fighter_2']
            ko_dict['date'] = fight['date']
            opponents_average_elo += int(db.execute(f"select elo from elo where fighter_id = {fight['fighter_2']}").fetchone()[0])
            ko_dict['opponent_str_def'] = float(db.execute(f"select str_def from advanced_stats where fighter_id = {fight['fighter_2']}").fetchone()[0].replace('%', '')) / 100
            ko_dict['opponent_sapm'] = db.execute(f"select SApM from advanced_stats where fighter_id = {fight['fighter_2']}").fetchone()[0]
            ko_list.append(ko_dict)

    opponents_average_elo = opponents_average_elo/ko_num if ko_num > 0 else 1100
    ko_rate = ko_num / len(fights)
    ko_pm = ko_num / mins

    ko_hash = {
        'ko_list':ko_list, #this is for contextualizing the KOs
        'ko_rate':ko_rate, #average of KOs per fight
        'ko_pm':ko_pm, #average of KO per cage minute time
        'opponent_average_elo':opponents_average_elo,
    }

    return ko_hash
    

def non_ufc_fight_remover(id, df, db):
    espn_dates = df['date'].apply(parse_date)

    rows = db.execute(
        'SELECT date FROM records WHERE fighter_1 = ?',
        (id,)
    ).fetchall()

    ufc_dates = [parse_date(r['date']) for r in rows]

    return df[espn_dates.isin(ufc_dates)]


def parse_date(d):
    # If it's a Series or tuple, take the first element
    if isinstance(d, (tuple, pd.Series)):
        d = d[0] if isinstance(d, tuple) else str(d)[1:]
    
    d = str(d).strip()  # ensure it's a string

    # known formats
    formats = ["%b. %d, %Y", "%b %d, %Y"]

    for fmt in formats:
        try:
            return datetime.strptime(d, fmt)
        except:
            continue

    raise ValueError(f"Unknown date format: {d}")



def get_fighter_minutes(fights):
    seconds = 0
    for row in fights:
        seconds += int(row['round_num']) * 5 * 60 + (int((row['fight_time'].split(':'))[0])) * 60
    return seconds // 60

def safe_split(col):
    return col.fillna("0/0").replace("-", "0/0").str.split("/", expand=True)

def get_hash_data(db, art_type, fighter_id):
    """ get the data of a specific fighter in a specific art type when given the db (sq.Row), art type (striking, clinching, grappling) and fighter id. Returns -1 if fail"""
    if art_type not in ['striking', 'clinching', 'grappling', 'global']:
        return -1
    if art_type != 'global':
        row = db.execute(f"select * from aggregate_{art_type.lower()} where fighter_id = ?", (fighter_id,)).fetchone()
    else:
        row = db.execute(f"select * from aggregate_global g join aggregate_career c on g.fighter_id = c.fighter_id where g.fighter_id = ?", (fighter_id,)).fetchone()
    if row:
        return row
    else:
        return -1


def get_z_score(features_to_scale=None, df=None, low=None, up=None):
    ''' features_to_scale: put the columns you want to change, fighters_df: the pandas df, low: the lower end of the z_score you want to clip, up: upper end of the clip'''
    '''low + up should be 1.0'''
    for col in features_to_scale:
        series = df[col]

        #z score
        median = series.median()
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1

        if iqr == 0:
            # this is why title score scaled is 50 for all fighters
            df[col + '_scaled'] = 50
            continue

        z = (series - median) / iqr

        # --- Clip extreme z-scores (prevents outliers from dominating) ---
        z_clipped = z.clip(lower=z.quantile(low), upper=z.quantile(up))

        # --- Map to 1–100 ---
        scaled = ((z_clipped - z_clipped.min()) / (z_clipped.max() - z_clipped.min())) * 99 + 1

        df[col + '_scaled'] = scaled.round(2)

    return df

def get_scaled_attributes(best=True, db=None, fighter_id = 2373, quantity=0):
    '''best (t or f), db, fighter_id'''
    row = db.execute('''select * from aggregate_grappling g 
                        join aggregate_striking s on g.fighter_id = s.fighter_id 
                        join aggregate_clinching c on g.fighter_id = c.fighter_id 
                        where g.fighter_id = ?''', (fighter_id,)).fetchone()
    if not row:
        return {}
    
    stats = dict(row)

    scaled = {
        k: v for k, v in stats.items()
        if k.endswith('_scaled')
    }

    sorted_dict = dict(
        sorted(scaled.items(), key=lambda item: item[1], reverse=best)[:quantity]
    )

    return sorted_dict

def total_analysis_update(fighter_id, name, db, conn):

    df_list = [
        'striking', 
        'clinching', 
        'grappling', 
        'global', 
        'career'
    ]

    # for key, df in df_list.items():
    #     if 'fighter_id' not in df.columns:
    #         df['fighter_id'] = fighter_id

    for key in df_list:
        query = f'select * from aggregate_{key}'
        total_df = pd.read_sql_query(query, con=conn)
        for col in total_df.columns:
            if total_df[col].dtype == object:
                try:
                    total_df["file_data"] = total_df["file_data"].apply(lambda x: base64.b64encode(x).decode() if isinstance(x, (bytes, bytearray)) else x)
                except:
                    pass
        features_to_scale = features[key]
        high, low = art_style_high_low[key]
        if key == 'striking':
            fighter_df = fighter_striking_analysis(fighter_id, conn)
            fighter_df = get_individual_z_score(features_to_scale, fighter_df, total_df, low, high)
            fighter_df['SApM_scaled'] = 100 - fighter_df['SApM_scaled']

            fighter_df['Boxing'] = 0.35 * fighter_df['true_ko_power_scaled'] + 0.15 * fighter_df['effective_volume_scaled'] + 0.25 * fighter_df['str_def_scaled'] + 0.25 * fighter_df['SApM_scaled']
            fighter_df['KickBoxing'] = 0.25 * fighter_df['true_ko_power_scaled'] + 0.15 * fighter_df['effective_volume_scaled'] + 0.20 * fighter_df['str_def_scaled'] + 0.10 * fighter_df['SApM_scaled'] + 0.30 * fighter_df['leg_kicks_scaled']
        elif key == 'clinching':
            fighter_df = fighter_clinch_analysis(fighter_id=fighter_id, conn=conn)
            fighter_df = get_individual_z_score(features_to_scale, fighter_df, total_df, low, high)
            fighter_df["Muay"] = 0.2 * fighter_df['clinch_strikes_pm_scaled'] + 0.8 * fighter_df['effective_accuracy_scaled']
        elif key == 'grappling':
            fighter_df = fighter_grappling_analysis(fighter_id=fighter_id, name=name, conn=conn)
            fighter_df = get_individual_z_score(features_to_scale, fighter_df, total_df, low, high)

            fighter_df["Wrestling"] = 0.34 * fighter_df['effective_takedowns_scaled'] + 0.33 * fighter_df['effective_control_scaled'] + 0.33 * fighter_df['td_def_scaled']
            fighter_df["BJJ"] = 0.7 * fighter_df['effective_sub_threat_scaled'] + 0.3 * fighter_df['bjj_defence_scaled']
            fighter_df['GNP'] = fighter_df['effective_gnp_scaled'] 

        elif key == 'global':
            fighter_df = global_rating(conn, fighter_id)
            fighter_df = get_individual_z_score(features_to_scale, fighter_df, total_df, low, high)
            print(total_df.columns)
            print(fighter_df.columns)

        elif key == 'career':
            fighter_df = career_ranking_analysis(conn, fighter_id)
            #backup_list = [fighter_df['debut'], fighter_df['last_fight'], fighter_df['cage_time'], fighter_df['average_fight_time']] 
            # for col in fighter_df.columns: 
            #     if fighter_df[col].dtype == object: 
            #         try: 
            #             fighter_df['file_data'] = fighter_df['file_data'].apply(lambda x: base64.b64encode(x).decode() 
            #             if isinstance(x, (bytes, bytearray)) else x) 
            #         except: pass 
            
            # fighter_df = fighter_df.map( lambda x: x.item() if hasattr(x, "item") else x ) 
        

            # for col in fighter_df.columns: 
            #     fighter_df[col] = pd.to_numeric(fighter_df[col], errors='coerce')
            
            # fighter_df['debut'] = backup_list[0]

            fighter_df['fighter_id'] = fighter_id
            fighter_df = get_individual_z_score(features_to_scale, fighter_df, total_df, low, high)
            print(total_df.columns)
            print(fighter_df.columns)
            try:
                fighter_df['career_score'] = 0.65 * fighter_df['elo_score_scaled'] + 0.35 * fighter_df['win_score_scaled']

                fighter_df['career_score'] *= fighter_df['fall_off_penalty']
            except Exception as e:
                fighter_df['career_score'] = 50
                print(f'exception occued: {e} in career func')


            mask = fighter_df['title_score'] > 0.8

            fighter_df.loc[mask, 'career_score'] *= ( 1 + (fighter_df.loc[mask, 'title_score'] - 0.8) / ((3 - 0.8) * 3))

            fighter_df['career_score'] = fighter_df['career_score'].clip(upper=100)


        
        logger.info(f'successfully updated the aggregate data for {fighter_id}')

        # update_sql_table_dynamic(fighter_df, f'aggregate_{key}', fighter_id, conn)

        conn.cursor().execute(
            f"DELETE FROM aggregate_{key} WHERE fighter_id = ?",
            (fighter_id,)
        )

        fighter_df.to_sql(
            f"aggregate_{key}",
            conn,
            if_exists="append",
            index=False
        )

def get_individual_z_score(
    features_to_scale=None,
    fighter_df=None,
    total_df=None,
    low=None,
    up=None
):
    """
    Compute scaled z-scores for individual fighters using
    population statistics.

    features_to_scale : columns to scale
    fighter_df        : dataframe containing fighter(s) to transform
    total_df          : full population dataframe (reference distribution)
    low               : lower quantile clip (ex: 0.02)
    up                : upper quantile clip (ex: 0.98)
    """
    for col in features_to_scale:
        try:
            series = total_df[col].astype('float')

            median = series.median()
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1

            if iqr == 0:
                fighter_df[col + "_scaled"] = 50
                continue

            z = (fighter_df[col] - median) / iqr

            # --- Compute clipping bounds from POPULATION ---
            pop_z = (series - median) / iqr

            lower_bound = pop_z.quantile(low)
            upper_bound = pop_z.quantile(up)

            z_clipped = z.clip(lower=lower_bound, upper=upper_bound)

            # --- Map using population scale ---
            scaled = (
                (z_clipped - lower_bound)
                / (upper_bound - lower_bound)
            ) * 99 + 1

            fighter_df[col + "_scaled"] = scaled.round(2)
        except Exception as e:
            fighter_df[col + "_scaled"] = 50
            print(f"there was an error trying to find a z score for the column: {col}")

    return fighter_df

def update_sql_table_dynamic(df, table_name, fighter_id, conn):
    """
    Updates or inserts a row in a SQL table dynamically from a dataframe.

    df        : DataFrame containing one row (must include id_col)
    table_name: SQL table to update
    id_col    : column used as the row identifier
    conn      : sqlite3 connection
    """
    
    cursor = conn.cursor()
    row = df.iloc[0]

    # All columns except the ID
    columns = [c for c in df.columns if c != 'fighter_id']

    # Check if row exists
    exists = cursor.execute(
        f"SELECT fighter_id FROM {table_name} WHERE fighter_id = ?",
        (fighter_id,)
    ).fetchone()

    if exists:
        # -------- UPDATE --------
        set_clause = ", ".join([f"{col} = ?" for col in columns])

        query = f"""
        UPDATE {table_name}
        SET {set_clause}
        WHERE fighter_id = ?
        """

        values = tuple(row[col] for col in columns) + (fighter_id,)

    else:
        # -------- INSERT --------
        all_cols = df.columns.tolist()
        col_string = ", ".join(all_cols)
        placeholders = ", ".join(["?"] * len(all_cols))

        query = f"""
        INSERT INTO {table_name} ({col_string})
        VALUES ({placeholders})
        """

        values = tuple(row[col] for col in all_cols)

    cursor.execute(query, values)
    conn.commit()
        


# with sq.connect(db_path) as conn:

#     total_analysis_update(1637, "Anthony Hernandez", "", conn)

# total_fighting_analysis('career')

# I have to do this since I cant import utilities. THIS SUCKS

# def clean_split(value):
#     if not value:
#         return 0, 0

#     # Split on newlines and remove empty junk
#     parts = [p.strip() for p in value.split('\n') if p.strip()]

#     if len(parts) >= 2:
#         return parts[0], parts[1]
    
#     return parts[0] if parts else 0, 0


def parse_of_stat(s):
    if not s or 'of' not in s:
        return {"landed": 0, "attempted": 0}

    try:
        landed, attempted = s.split('of')
        return {
            "landed": int(landed.strip()),
            "attempted": int(attempted.strip())
        }
    except:
        return {"landed": 0, "attempted": 0}


def parse_percent(s):
    if not s or s == '---':
        return 0
    return float(s.replace('%', '').strip())


def parse_time(s):
    if not s or ':' not in s:
        return 0
    
    try:
        minutes, seconds = s.split(':')
        return int(minutes) * 60 + int(seconds)
    except:
        return 0


def parse_int(s):
    if not s or s == '---':
        return 0
    try:
        return int(s)
    except:
        return 0

def normalize_fight(db, fight_id):
    #make normal df for analysis and then aggregate_df for presentation
    totals = db.execute('select * from rounds where fight_id = ? and stat_type = ?', (fight_id, "totals")).fetchall()
    sig_strikes = db.execute('select * from rounds where fight_id = ? and stat_type = ? and stat not in (?, ?)  ', (fight_id, "sig_strikes", "sig_str", "sig_str_percent")).fetchall()

    f1, f2 = round_aggregation(db, totals, sig_strikes)

    # fighter_a = {}
    # fighter_b = {}

    # for key in f1:
    #     # keep identity fields
    #     if key in ['name', 'fighter_id']:
    #         fighter_a[key] = f1[key]
    #         fighter_b[key] = f2[key]
    #         continue

    #     raw_value = f1[key]

    #     a_val, b_val = clean_split(raw_value)

    #     # ---------- TYPE HANDLING ----------
    #     if key in ['sig_str', 'total_str', 'head', 'body', 'leg', 'distance', 'clinch', 'ground']:
    #         fighter_a[key] = parse_of_stat(a_val)
    #         fighter_b[key] = parse_of_stat(b_val)

    #     elif key in ['sig_str_percent', 'td_percent']:
    #         fighter_a[key] = parse_percent(a_val)
    #         fighter_b[key] = parse_percent(b_val)

    #     elif key == 'ctrl':
    #         fighter_a[key] = parse_time(a_val)
    #         fighter_b[key] = parse_time(b_val)

    #     else:  # kd, sub_att, rev, etc.
    #         fighter_a[key] = parse_int(a_val)
    #         fighter_b[key] = parse_int(b_val)

    # ---------- FINAL STRUCTURE ----------
    return {
        "fighter_a": f1,
        "fighter_b": f2
    }

def round_aggregation(db, totals, sig_strikes):
    f1 = defaultdict(int)
    f2 = defaultdict(int)

    f1['name'] = totals[0]['fighter_1']
    f2['name'] = totals[0]['fighter_2']
    print(f1['name'])

    id_1 = db.execute('select fighter_id from fighters where name = ?', (f1['name'],)).fetchone()
    id_2 = db.execute('select fighter_id from fighters where name = ?', (f2['name'],)).fetchone()

    f1['fighter_id'] = id_1['fighter_id']
    f2['fighter_id'] = id_2['fighter_id']


    for total in totals:
        if total['fighter_1'] == f1['name']:
            continue

        stat = total['stat']
        if "percent" in stat:
            continue
        if 'of' in total['fighter_1']:
            parsed_f1 = parse_of_stat(total['fighter_1'])
            f1[f'{stat}_landed'] += parsed_f1['landed'] 
            f1[f'{stat}_attempted'] += parsed_f1['attempted'] 
            parsed_f2 = parse_of_stat(total['fighter_2'])
            f2[f'{stat}_landed'] += parsed_f2['landed'] 
            f2[f'{stat}_attempted'] += parsed_f2['attempted'] 
        elif stat == 'ctr':
            f1[stat] += parse_time(total['fighter_1'])
            f2[stat] += parse_time(total['fighter_2'])
        else:
            f1[stat] += parse_int(total['fighter_1'])
            f2[stat] += parse_int(total['fighter_2'])


    f1['sig_str_percent'] = round(f1['sig_str_landed'] / f1['sig_str_attempted'], 2) * 100 if f1['sig_str_attempted'] != 0 else 0
    f2['sig_str_percent'] = round(f2['sig_str_landed'] / f2['sig_str_attempted'], 2) * 100 if f2['sig_str_attempted'] != 0 else 0

    f1['td_percent'] = round(f1['td_landed'] / f1['td_attempted'], 2) * 100 if f1['td_attempted'] != 0 else 0
    f2['td_percent'] = round(f2['td_landed'] / f2['td_attempted'], 2) * 100 if f2['td_attempted'] != 0 else 0


    for sig in sig_strikes:
        stat = sig['stat']
        parsed_f1 = parse_of_stat(sig['fighter_1'])
        f1[f'{stat}_landed'] += parsed_f1['landed']
        f1[f'{stat}_attempted'] += parsed_f1['attempted']
        parsed_f2 = parse_of_stat(sig['fighter_2'])
        f2[f'{stat}_landed'] += parsed_f2['landed']
        f2[f'{stat}_attempted'] += parsed_f2['attempted']

    
    return f1, f2