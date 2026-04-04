from flask import Flask, flash, redirect, render_template, request, session, g
from flask_session import Session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
import logging
import os
from pathlib import Path
from my_app.utilities import login_required, apology
import sqlite3 as sq
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import datetime, date
from my_app.utilities import *
from my_app.plots import *
# from my_app.analysis import elo_analysis, career_analysis, fight_analysis
from my_app.analysis import *
from dotenv import load_dotenv

#this is going to be the file for my website
load_dotenv()

logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates')

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    # SESSION_COOKIE_SECURE=True,    
    SESSION_COOKIE_SAMESITE="Lax"
)

app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

if not app.config["SECRET_KEY"]:
    raise RuntimeError('SECRET_KEY not set!')

limiter = Limiter(get_remote_address, app=app)
limiter.init_app(app)

csrf = CSRFProtect(app)
# Custom filter
# app.jinja_env.filters["usd"] = usd

# Configure session to use filesystem (instead of signed cookies)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

db_path = (Path(__file__).parent).parent / "data" / "testing.db"

weight_hash = {
    'p4p':None,
    "Flyweight":'125 lbs.',
    'Bantamweight':'135 lbs.',
    'Featherweight':'145 lbs.',
    'Lightweight': '155 lbs.',
    'Welterweight': '170 lbs.',
    'Middleweight': '185 lbs.',
    'LightHeavyweight':'205 lbs.',
    'Heavyweight':'205 lbs.'
}

def get_db():
    if "db" not in g:
        conn = sq.connect(db_path)
        conn.row_factory = sq.Row
        g.conn = conn
        g.db = conn.cursor()
    return g.conn, g.db


def get_db_no_row():
    if "conn" not in g:
        g.conn = sq.connect(db_path)
    return g.conn

@app.teardown_appcontext
def close_db(error):
    conn = g.pop("conn", None)
    if conn is not None:
        conn.close()

@app.after_request
def after_request(response):
    """Ensure responses aren't cached"""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/roster', methods=['GET', 'POST'])
@login_required
def roster():
    default_weight, default_country, default_team = ('', '', '')
    conn, db = get_db()
    countries = [row[0] for row in db.execute('select distinct country from fighters').fetchall()]
    teams = [row[0] for row in db.execute('select distinct team from fighters').fetchall()]
    fighters = []

    if request.method == "POST":
        weight = request.form.get('weight_class')
        country = request.form.get('country')
        team = request.form.get('team')

        if len(weight) == 1 and weight[0] == default_weight:
            weight = None
        if country == default_country:
            country = None
        if team == default_team:
            team = None

        query = 'select * from fighters'
        filters = []
        answers = []

        if weight:
            if weight != "All" and weight != "Heavyweight":
                placeholders = ', '.join(['?'])
                filters.append(f"weight IN ({placeholders})")
                answers.extend([weight_hash[weight]])
            elif weight == "All":
                #error here broski
                # placeholders = ', '.join(['?'] * 8)
                # filters.append(f'weight in ({placeholders})')
                # answers.extend([weight for weight_class, weight in weight_hash.items()])
                pass
            elif weight == "Heavyweight":
                placeholders = '?'
                filters.append(f"cast(replace(weight, ' lbs.', '') as integer) > {placeholders}")
                answers.extend([207])

        if country and country != 'None':
            filters.append("country = ?")
            answers.append(country)
        if team and team != 'None':
            filters.append("team = ?")
            answers.append(team)

        if filters:
            query += " where " + " and ".join(filters)

        fighters = db.execute(query, tuple(answers)).fetchall()
        print("QUERY:", query)
        print("PARAMS:", answers)
        print("number of fighters:", len(fighters))


    return render_template('roster.html', countries=countries, teams=teams, fighters=fighters)


@app.route('/fights/<sub>/', methods=['GET', 'POST'])
@login_required
def fights(sub):
    conn, db = get_db()
    rows = []
    upcoming_events = []
    fights = []
    event_query = 'select * from events;'
    if sub == 'upcoming':
        upcoming = get_upcoming_events_list()
        if upcoming:
            for i in range(len(upcoming.keys())):
                upcoming_events.append(upcoming[i+1])
                print(upcoming_events)
        session['fights_upcoming'] = upcoming_events
    elif sub == 'completed':
        rows = db.execute(event_query).fetchall()
        rows = sorted(rows, key=lambda x: datetime.strptime(x['event_date'], "%B %d, %Y"), reverse=True)
    else:
        if sub.isnumeric() or type(sub) == int:
            cursor = db.execute('select * from events where event_id = ?', (sub,)).fetchall()
            fights = get_completed_event_info(url=cursor[0]['event_url'])
            print('fights: ', fights)
        else:
            upcoming_session = session.get('fights_upcoming', [])
            for event in session['fights_upcoming']:
                if event['event_id'] == sub:
                    fights = get_upcoming_event_info(url=event['event_url'])



    return render_template('fights.html', events=rows, upcoming_events=upcoming_events, sub=sub, fights=fights)

@app.route('/match-ups', methods=["GET", "POST"])
@login_required
def match_ups():
    # I ALREADY MADE ALL OF THIS BUT I JUST USED AI TO CLEAN IT UP
    conn, db = get_db()
    all_fighters = get_all_fighters(db)
    fighter_names = [row['name'] for row in all_fighters]

    # Default fighters
    default_fighter1 = 'khabib nurmagomedov'
    default_fighter2 = 'conor mcgregor'

    # Function to get all the data for a given pair of fighters
    def get_fight_data(fighter1_name, fighter2_name):
        fighter_bio_1, fighter_bio_2 = get_two_fighters(fighter1_name, fighter2_name, db)
        fighter_bio_1['team'], fighter_bio_2['team'] = fighter_bio_1['team'].title() if fighter_bio_1['team'] else None, fighter_bio_2['team'].title() if fighter_bio_2['team'] else None
        fighter_data_1 = get_fighter_data(fighter_bio_1['id'], db)
        fighter_data_2 = get_fighter_data(fighter_bio_2['id'], db)

        strike_fig, grappling_fig, career_fig = plot_mergers(fighter_bio_1['id'], fighter_bio_2['id'], db)
        heat1, heat2 = (
            strike_heatmap(fighter_bio_1['id'], db).to_html(full_html=False),
            strike_heatmap(fighter_bio_2['id'], db).to_html(full_html=False)
        )

        comparison_strike_plot, _ = comparison_plot(fighter_bio_1['id'], fighter_bio_2['id'], db, compare_type="striking")
        comparison_grappling_plot, _ = comparison_plot(fighter_bio_1['id'], fighter_bio_2['id'], db, compare_type="grappling")
        comparison_career_plot, career_data  = comparison_plot(fighter_bio_1['id'], fighter_bio_2['id'], db, compare_type="career")

        compare_plots = [
            comparison_strike_plot.to_html(full_html=False),
            comparison_grappling_plot.to_html(full_html=False),
            comparison_career_plot.to_html(full_html=False)
        ]

        global_scores = [
            get_global_score(db, fighter_bio_1['id']),
            get_global_score(db, fighter_bio_2['id'])
        ]

        career_data = career_data_cleaner(career_data)

        return (
            fighter_bio_1, fighter_bio_2,
            fighter_data_1, fighter_data_2,
            strike_fig, grappling_fig, career_fig,
            heat1, heat2,
            compare_plots, career_data,
            global_scores
        )

    # Start with default matchup
    fighter_bio_1, fighter_bio_2, fighter_data_1, fighter_data_2, strike_fig, grappling_fig, career_fig, \
    heat1, heat2, compare_plots, career_data, global_scores = get_fight_data(default_fighter1, default_fighter2)

    # Try POST data if available
    if request.method == 'POST':
        try:
            fighter1 = request.form.get("fighter1", default_fighter1)
            fighter2 = request.form.get("fighter2", default_fighter2)
            fighter_bio_1, fighter_bio_2, fighter_data_1, fighter_data_2, strike_fig, grappling_fig, career_fig, \
            heat1, heat2, compare_plots, career_data, global_scores = get_fight_data(fighter1, fighter2)
        except Exception as e:
            # If any exception occurs, fallback to default fighters (already loaded above)
            print(e)
            flash("This fighter either doesn't have registered stats in espn or he/she doesn't exist")

    return render_template(
        'match_ups.html', names=fighter_names,
        fighter_1=fighter_bio_1, fighter_2=fighter_bio_2,
        fighter_data_1=fighter_data_1, fighter_data_2=fighter_data_2,
        strike_fig=strike_fig, grappling_fig=grappling_fig, career_fig=career_fig,
        heat1=heat1, heat2=heat2, compare_plots=compare_plots,
        career_data=career_data, global_scores=global_scores
    )



@app.route('/predictions')
@login_required
def predictions():
    return render_template('predictions.html')

@app.route("/login", methods=["GET", "POST"])
@limiter.limit('5 per minute')
def login():
    """Log user in"""
    conn, db = get_db()
    # Forget any user_id
    session.clear()

    # User reached route via POST (as by submitting a form via POST)
    if request.method == "POST":
        # Ensure username was submitted
        if not request.form.get("username"):
            return apology("Invalid Credentials", 403)

        # Ensure password was submitted
        elif not request.form.get("password"):
            return apology("Invalid Credentials", 403)
        
        elif len(request.form.get("password")) < 8:
            return apology("Password must be at least 8 characters long", 403)

        # Query database for username
        rows = db.execute(
            "SELECT * FROM users WHERE username = ?", (request.form.get("username"),)
        ).fetchall()

        # Ensure username exists and password is correct
        if len(rows) != 1 or not check_password_hash(
            rows[0]["hash"], request.form.get("password")
        ):
            return apology("Invalid Credentials", 403)

        # Remember which user has logged in
        session["user_id"] = rows[0]["id"]
        # session["username"] = request.form.get("username")

        # Redirect user to home page
        return redirect("/")

    # User reached route via GET (as by clicking a link or via redirect)
    else:
        return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    conn, db = get_db()

    username = request.form.get("username")
    password = request.form.get("password")
    confirmation = request.form.get("confirmation")

    if request.method == "POST":
        if not username:
            return apology("Write a username!")
        elif not password or not confirmation:
            return apology("Write a password!")
        elif password != confirmation:
            return apology("Wrong confirmation")
        elif len(password) < 8:
            return apology("Password should be at least 8 characters")

        try:
            db.execute("INSERT INTO users (username, hash) VALUES (?, ?)", (username, generate_password_hash(password)))
            conn.commit()
        except sq.IntegrityError:
            return apology("Already registered!")
    return render_template("register.html")

@app.route('/search/', methods=['GET', 'POST'])
@login_required
def search():
    conn, db = get_db()
    fighters_list = []
    fighter_search = request.args.get('query', '')
    fighter_search_first_name = fighter_search + "%"
    fighter_search_last_name = "%" + fighter_search
    match_1 = db.execute('select * from fighters where name like ?', (fighter_search_first_name.lower().title().strip(),)).fetchall()
    match_2 = db.execute('select * from fighters where name like ?', (fighter_search_last_name.lower().title().strip(),)).fetchall()

    if len(fighter_search) != 1:
        if match_1 and not match_2:
            fighters_list = match_1
        elif match_2 and not match_1:
            fighters_list = match_2
        elif match_2 and match_1:
            if len(match_1) > len(match_2):
                fighters_list = match_1
            elif len(match_2)>=len(match_1):
                fighters_list = match_2
    else:
        # this is to make sure that if the search is only 1 letter long the searches are first name (starts from the first letter only) searches
        if match_1:
            fighters_list = match_1

    matches = len(fighters_list)

    return render_template('search.html', matched_number = matches, fighters_list=fighters_list)

@app.route('/rankings', methods=['GET', 'POST'])
@login_required
def rankings():
    conn, db = get_db()
    fighters = []
    action = None
    # chosen_class = None
    original_query = 'Select f.fighter_id, f.name, f.picture, e.elo from fighters f join elo e on f.fighter_id = e.fighter_id'
    query = 'Select f.fighter_id, f.name, f.picture, e.elo from fighters f join elo e on f.fighter_id = e.fighter_id'
    answers = []
    if request.method == "POST":
        action = request.form.get('action')
        if action in weight_hash.keys():
            if action == 'Heavyweight':
                query += " where cast(replace(f.weight, ' lbs.', '') as integer) > ?"
                answers.append(207)
            elif action == 'p4p':
                query = original_query #ik it is redundant
            else:
                query += " where f.weight = ?"
                answers.append(weight_hash[action])
        
        query += " order by e.elo desc;"
        print("class:", action)
        print('query', query)
        fighters = db.execute(query, tuple(answers)).fetchall()

    return render_template('rankings.html', fighters=fighters, chosen_class=action)

@app.route('/fighter/<id>', methods=['GET', 'POST'])
@login_required
def fighter(id):
    selection = 'career'
    quantity = 1
    conn, db = get_db()
    fighter = db.execute('select * from fighters where fighter_id = ?', (id,)).fetchall()
    if fighter is None:
        return apology('fighter not found')
    fighter = dict(fighter[0])
    if fighter['birthday']:
        fighter['birthday'] = date.today().year - datetime.strptime(fighter['birthday'], '%m/%d/%Y').year if fighter['birthday'] != None else None

    fighter['weight'] = fighter['weight'].replace('.', '')
    if fighter['team']:
        fighter['team'] = fighter['team'].title()

    plot = elo_history_plot(id).to_html(full_html=False)
    #random initial assignment for heat map
    heat_map = strike_heatmap(2373, db)
    weaknesses = {}
    strengths = {}

    elo_hash = elo_analysis(id)
    career_hash = career_analysis(db=db, id=id, cached=False) #return to true once ur done
    career_hash['finish_rate'] = f'{career_hash['finish_rate'] * 100 : .1f}%'
    career_hash['win_rate'] = f'{career_hash['win_rate'] * 100 : .1f}%'
    data_hash = career_hash
    fights = get_career_fights()
    #print(fights)
    last_5 = data_hash['last_5']
    if request.method == 'POST':
        quantity = int(request.form.get("num", 5))
        selection = request.form.get('action')
        try:
            if selection == "striking":
                plot = striking_analysis_plot(id, db).to_html(full_html=False)
                heat_map = strike_heatmap(id, db).to_html(full_html=False)
                data_hash = get_hash_data(db, 'striking', id)
            elif selection == "clinch":
                plot = clinching_analysis_plot(id, db).to_html(full_html=False)
                data_hash = get_hash_data(db, 'clinching', id)
            elif selection == "grappling":
                plot = grappling_analysis_plot(id, db).to_html(full_html=False)
                data_hash = get_hash_data(db, 'grappling', id)
            elif selection == "overall":
                plot = career_plot(id, db).to_html(full_html=False)
                data_hash = get_hash_data(db, 'global', id)
                weaknesses = get_scaled_attributes(best=False, db=db, fighter_id=id, quantity=quantity)
                strengths = get_scaled_attributes(best=True, db=db, fighter_id=id, quantity=quantity)
            elif selection == "record":
                fights = get_career_fights(fighter_id=id)
            else:  
                weaknesses = get_scaled_attributes(best=False, db=db, fighter_id=id, quantity=5)
                strengths = get_scaled_attributes(best=True, db=db, fighter_id=id, quantity=5)
        except Exception as e:
            return apology('Could not find this fighter! He probably does not have registered fight stats in espn')

    return render_template('fighter.html', id=id, fighter=fighter, elo_data_hash=elo_hash, selection=selection, plot=plot, data_hash=data_hash, last_5=last_5, last_fight=career_hash['last_fight'], heat_map=heat_map, weaknesses=weaknesses, 
                           strengths=strengths, quantity=quantity, fights=fights)

@app.route('/versus/<fight_id>/', methods=['GET', 'POST'])
@login_required
def versus(fight_id):
    conn, db = get_db()

    #gotta fix fighter_a and fighter_b to give accurate data from rounds table and that's about it
    fight, event, fighter_a, fighter_b = fight_analysis(db, fight_id)
    fight['fighter_a']['elo_diff'] = fight['fighter_a']['new_elo'] - fight['fighter_a']['elo']
    fight['fighter_b']['elo_diff'] = fight['fighter_b']['new_elo'] - fight['fighter_b']['elo']

    striking = {
        "fighter_a": {
            "name": fighter_a['name'],
            "total_strikes_landed": fighter_a['total_str_landed'],
            "total_strikes_attempted": fighter_a['total_str_attempted'],
            "total_significant_strikes_landed": fighter_a['sig_str_landed'],
            "total_significant_strikes_attempted": fighter_a['sig_str_attempted'],
            "significant_strike_percent":fighter_a['sig_str_percent'],
            "knock-downs": fighter_a['kd']
        },
        "fighter_b": {
            "name": fighter_b['name'],
            "total_strikes_landed": fighter_b['total_str_landed'],
            "total_strikes_attempted": fighter_b['total_str_attempted'],
            "total_significant_strikes_landed": fighter_b['sig_str_landed'],
            "total_significant_strikes_attempted": fighter_b['sig_str_attempted'],
            "significant_strike_percent":fighter_b['sig_str_percent'],
            "knock-downs": fighter_b['kd']
        }
    }

    #no td in normal table for some reason, add them later from rounds
    grappling = {
        "fighter_a": {
            # "takedowns":fighter_a['td'],
            "name": fighter_a['name'],
            "takedowns":fighter_a['td_landed'],
            "take_down_percent": fighter_a['td_percent'],
            "sub_attempts": fighter_a['sub_att'],
            "reversals": fighter_a['rev'],
            "control_time": fighter_a['ctr']
        },
        "fighter_b": {
            # "takedowns":fighter_b['td'],
            "name": fighter_b['name'],
            "takedowns":fighter_b['td_landed'],
            "take_down_percent": fighter_b['td_percent'],
            "sub_attempts": fighter_b['sub_att'],
            "reversals": fighter_b['rev'],
            "control_time": fighter_b['ctr']
        }
    }

    print(fighter_a)
    print(fighter_b)

    return render_template('versus.html', fight = fight, event = event, striking = striking, grappling = grappling)


@app.route('/logout')
@login_required
def logout():
    session.clear()
    return redirect('/login')


