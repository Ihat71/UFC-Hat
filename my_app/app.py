from flask import Flask, flash, redirect, render_template, request, session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_session import Session
from flask_wtf.csrf import CSRFProtect
from dotenv import load_dotenv
import logging

from my_app.config import Config
from my_app.logging_config import setup_logging
from my_app.repositories.api_repository import ApiRepository
from my_app.services.api_service import ApiService
from my_app.utilities import apology, login_required

load_dotenv()
setup_logging(Config)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates")
app.config.from_object(Config)

app.secret_key = app.config["SECRET_KEY"]
if not app.config["SECRET_KEY"]:
    raise RuntimeError("SECRET_KEY not set!")

limiter = Limiter(get_remote_address, app=app)
limiter.init_app(app)
CSRFProtect(app)
Session(app)

db_path = app.config["DB_PATH"]

weight_hash = {
    "p4p": None,
    "Flyweight": "125 lbs.",
    "Bantamweight": "135 lbs.",
    "Featherweight": "145 lbs.",
    "Lightweight": "155 lbs.",
    "Welterweight": "170 lbs.",
    "Middleweight": "185 lbs.",
    "LightHeavyweight": "205 lbs.",
    "Heavyweight": "205 lbs.",
}

repository = ApiRepository(db_path=db_path)
service = ApiService(repository=repository, weight_hash=weight_hash)


@app.after_request
def after_request(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/roster", methods=["GET", "POST"])
@login_required
def roster():
    payload = service.roster_data(request.form if request.method == "POST" else None, request.method)
    return render_template("roster.html", **payload)


@app.route("/fights/<sub>/", methods=["GET", "POST"])
@login_required
def fights(sub):
    payload = service.fights_data(sub=sub, fights_upcoming_session=session.get("fights_upcoming", []))
    if payload["session_upcoming_events"] is not None:
        session["fights_upcoming"] = payload["session_upcoming_events"]
    return render_template(
        "fights.html",
        events=payload["events"],
        upcoming_events=payload["upcoming_events"],
        sub=sub,
        fights=payload["fights"],
    )


@app.route("/match-ups", methods=["GET", "POST"])
@login_required
def match_ups():
    try:
        payload = service.matchup_data(request.form if request.method == "POST" else None)
    except Exception:
        flash("This fighter either doesn't have registered stats in espn or he/she doesn't exist")
        payload = service.matchup_data(None)
    return render_template("match_ups.html", **payload)


@app.route("/predictions")
@login_required
def predictions():
    return render_template("predictions.html")


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("5 per minute")
def login():
    session.clear()
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if not username:
            return apology("Invalid Credentials", 403)
        if not password:
            return apology("Invalid Credentials", 403)
        if len(password) < 8:
            return apology("Password must be at least 8 characters long", 403)

        user_id = service.authenticate(username=username, password=password)
        if user_id is None:
            return apology("Invalid Credentials", 403)
        session["user_id"] = user_id
        return redirect("/")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        confirmation = request.form.get("confirmation")
        if not username:
            return apology("Write a username!")
        if not password or not confirmation:
            return apology("Write a password!")
        if password != confirmation:
            return apology("Wrong confirmation")
        if len(password) < 8:
            return apology("Password should be at least 8 characters")

        ok, message = service.register_user(username=username, password=password)
        if not ok:
            return apology(message)
    return render_template("register.html")


@app.route("/search/", methods=["GET", "POST"])
@login_required
def search():
    query = request.args.get("query", "")
    payload = service.search_data(query)
    return render_template("search.html", **payload)


@app.route("/rankings", methods=["GET", "POST"])
@login_required
def rankings():
    action = request.form.get("action") if request.method == "POST" else None
    payload = service.rankings_data(action)
    return render_template("rankings.html", **payload)


@app.route("/fighter/<id>", methods=["GET", "POST"])
@login_required
def fighter(id):
    try:
        payload = service.fighter_data(id, request.form if request.method == "POST" else None)
    except Exception:
        return apology("Could not find this fighter! He probably does not have registered fight stats in espn")
    if payload is None:
        return apology("fighter not found")
    return render_template("fighter.html", **payload)


@app.route("/versus/<fight_id>/", methods=["GET", "POST"])
@login_required
def versus(fight_id):
    payload = service.versus_data(fight_id)
    return render_template("versus.html", **payload)


@app.route("/logout")
@login_required
def logout():
    session.clear()
    return redirect("/login")




