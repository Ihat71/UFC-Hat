import numpy
import logging
from pathlib import Path
from datetime import datetime
import sqlite3 as sq


logger = logging.getLogger(__name__)


db_path = (Path(__file__).parent).parent / "data" / "ufc-hat.db"

def get_dates():
    ordered_ufc_event_dates = []
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        dates = cursor.execute('select event_date from events;').fetchall()
    for date in dates:  
        date = date[0]
        month_name, day_num, year_num = date.replace(",", "").replace(".", "").split(" ")
        month_num = datetime.strptime(month_name, "%B").month
        #new datetime object that can easily be parsed
        new_date = datetime(int(year_num), int(month_num), int(day_num))
        ordered_ufc_event_dates.append(new_date)
    #we can do sorted on these dates because of the datetime objects
    return sorted(ordered_ufc_event_dates)

def to_table_date(standard_date):
    #this function turns a standard date used in the data library to my table's date
    table_date = standard_date.strftime("%b. %d, %Y")
    return table_date

def get_elo():
    #changes the datetime objects into the date format my tables use 
    event_dates = list(map(to_table_date, get_dates()))
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        # print(event_dates)
        for date in event_dates:
            fights = cursor.execute('select fighter_a, fighter_b, winner, weight_class, method, round_ended, time_ended, is_title_fight from fights where date = ?', (date,)).fetchall()
            for fight in fights:
                fighter_a = fight[0]
                fighter_b = fight[1]
                if fight[2] == fighter_a:
                    winner = 'A'
                elif fight[2] == fighter_b:
                    winner = 'B'
                else:
                    winner = None
                weight_class = fight[3] if fight[3] else 'Unknown'
                method = fight[4]
                round_ended = fight[5]
                time_ended = fight[6]
                is_title_fight = True if fight[7] == 'yes' else 'no'

                is_draw = False if winner != 0 else True
                is_nc = False if winner != None else True
                rA = cursor.execute('select elo from elo where fighter_id = ?', (fighter_a,)).fetchone()
                rB = cursor.execute('select elo from elo where fighter_id = ?', (fighter_b,)).fetchone()
                new_rA, new_rB = elo_equation(rA=rA[0], rB=rB[0], winner=winner, draw=is_draw, nc=is_nc, method=method, round_ended=round_ended, is_title_fight=is_title_fight)
                cursor.execute('update elo set elo = ? where fighter_id = ?', (new_rA, fighter_a))
                logging.info(f'{fighter_a} changed elo from {rA[0]} to {new_rA} after fighting {fighter_b}')
                cursor.execute('update elo set elo = ? where fighter_id = ?', (new_rB, fighter_b))
                logging.info(f'{fighter_b} changed elo from {rB[0]} to {new_rB} after fighting {fighter_a}')
                ended = f"{round_ended} | {time_ended}"
                cursor.execute('insert into elo_history (fighter_1, fighter_2, winner, weight_class, elo_1, elo_2, new_elo_1, new_elo_2, method, round_time_ended, is_title_fight, date) values (?,?,?,?,?,?,?,?,?,?,?,?)', (fighter_a, fighter_b, fighter_a if winner == 'A' else fighter_b, weight_class, rA[0], rB[0], new_rA, new_rB, method, ended, fight[7], date))
            conn.commit()

            
def elo_equation(rA, rB, winner=None, draw=False, nc=False, method=None, round_ended=None, is_title_fight=False):
    #rA is the rating of A, and rB is the rating of B
    #these are the probabilities of A or B winning based on their respective elo rankings

    #should add extra elo for title bouts
    K = 32
    extra_k = 0
    title_k = 0
    if 'KO/TKO' in method or 'SUB' in method:
        extra_k += 2
    elif 'U-DEC' in method:
        extra_k += 1
    elif 'S-DEC' in method:
        extra_k -= 1
    if round_ended is not None and round_ended < 3:
        extra_k += 1
    if is_title_fight:
        title_k += 4

    pA = 1 / (1 + 10**((rB - rA)/400))
    pB = 1 - pA
    
    if nc or 'DQ' in method:
        return (rA, rB)
    
    if not draw:
        #equation for new elo ranking: 1 means win
        if winner == 'A':
            new_rA = rA + (K+extra_k+title_k)*(1 - pA)
            new_rB = rB + (K+extra_k)*(0-pB)
        elif winner == 'B':
            new_rA = rA + (K+extra_k)*(0 - pA)
            new_rB = rB + (K+extra_k+title_k)*(1-pB)  
        else:
            logging.error('Wrong parameters for function')
    elif draw:
        new_rA = rA + (K+extra_k)*(0.5 - pA)
        new_rB = rB + (K+extra_k)*(0.5 - pB)
    
    return (round(new_rA), round(new_rB))       
    

def make_elo_table():
    with sq.connect(db_path) as conn:
        cursor = conn.cursor() 
        cursor.execute('''CREATE TABLE if not exists elo(
                       fighter_id integer,
                       elo integer default 1200,
                       foreign key (fighter_id) references fighters(fighter_id)
                       );
                    ''')
        
        cursor.execute('''CREATE TABLE if not exists elo_history(
                       fighter_1 integer,
                       fighter_2 integer,
                       winner integer,
                       weight_class varchar(100),
                       elo_1 integer,
                       elo_2 integer,
                       new_elo_1 integer,
                       new_elo_2 integer,
                       method varchar(100),
                       round_time_ended varchar(100),
                       is_title_fight varchar(100),
                       date varchar(100),
                       foreign key (fighter_1) references fighters(fighters_id),
                       foreign key (fighter_2) references fighters(fighters_id)
                       );''')
        
        fighters = cursor.execute('select fighter_id from fighters;').fetchall()
        for fighter in fighters:
            cursor.execute('insert into elo (fighter_id) values (?);', (fighter[0],))

        conn.commit()

def elo_history_table(fighter_ids, initial_ratings, new_ratings, winner, method, round_ended, time_ended, is_title_fight):
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        
        fighter_1, fighter_2 = fighter_ids
        elo_1, elo_2 = initial_ratings
        new_elo_1, new_elo_2 = new_ratings
        ended = f"{round_ended} | {time_ended}"
        cursor.execute('insert into elo_history (fighter_1, fighter_2, winner, elo_1, elo_2, new_elo_1, new_elo_2, method, round_time_ended, is_title_fight) values (?,?,?,?,?,?,?,?,?)', (fighter_1, fighter_2, winner, elo_1, elo_2, new_elo_1, new_elo_2, method, ended, is_title_fight))
        conn.commit()



# def main():
#     make_elo_table()
#     get_elo()

# if __name__ == "__main__":
#     main()
# import os
# print(os.environ.get("SECRET_KEY"))