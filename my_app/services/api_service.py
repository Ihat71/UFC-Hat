from datetime import date, datetime
from typing import Any

import sqlite3 as sq
from werkzeug.security import check_password_hash, generate_password_hash

from my_app.analysis import (
    career_analysis,
    elo_analysis,
    fight_analysis,
    get_hash_data,
    get_scaled_attributes,
)
from my_app.plots import *

from my_app.repositories.api_repository import ApiRepository
from my_app.utilities import *



class ApiService:
    def __init__(self, repository: ApiRepository, weight_hash: dict[str, Any]) -> None:
        self.repository = repository
        self.weight_hash = weight_hash

    def roster_data(self, form: dict[str, str] | None, method: str) -> dict[str, Any]:
        countries, teams = self.repository.get_filter_options()
        fighters: list[Any] = []
        if method == "POST" and form is not None:
            weight = form.get("weight_class")
            country = form.get("country")
            team = form.get("team")
            if weight == "":
                weight = None
            if country == "":
                country = None
            if team == "":
                team = None
            fighters = self.repository.get_fighters_filtered(weight, country, team, self.weight_hash)
        return {"countries": countries, "teams": teams, "fighters": fighters}

    def fights_data(self, sub: str, fights_upcoming_session: list[dict[str, Any]] | None) -> dict[str, Any]:
        rows: list[Any] = []
        upcoming_events: list[dict[str, Any]] = []
        fights: list[Any] = []
        session_payload: list[dict[str, Any]] | None = None

        if sub == "upcoming":
            upcoming = get_upcoming_events_list()
            if upcoming:
                for i in range(len(upcoming.keys())):
                    upcoming_events.append(upcoming[i + 1])
            session_payload = upcoming_events
        elif sub == "completed":
            rows = self.repository.get_events()
            rows = sorted(rows, key=lambda x: datetime.strptime(x["event_date"], "%B %d, %Y"), reverse=True)
        else:
            if sub.isnumeric():
                event_row = self.repository.get_event_by_id(sub)
                if event_row:
                    fights = get_completed_event_info(url=event_row["event_url"])
            else:
                source = fights_upcoming_session or []
                for event in source:
                    if event["event_id"] == sub:
                        fights = get_upcoming_event_info(url=event["event_url"])
                        break

        return {
            "events": rows,
            "upcoming_events": upcoming_events,
            "fights": fights,
            "session_upcoming_events": session_payload,
        }

    def matchup_data(self, form: dict[str, str] | None) -> dict[str, Any]:
        conn, db = self.repository.cursor()
        try:
            all_fighters = get_all_fighters(db)
            fighter_names = [row["name"] for row in all_fighters]
            default_fighter1 = "khabib nurmagomedov"
            default_fighter2 = "conor mcgregor"
            fighter1 = form.get("fighter1", default_fighter1) if form else default_fighter1
            fighter2 = form.get("fighter2", default_fighter2) if form else default_fighter2

            (
                fighter_bio_1,
                fighter_bio_2,
                fighter_data_1,
                fighter_data_2,
                strike_fig,
                grappling_fig,
                career_fig,
                heat1,
                heat2,
                compare_plots,
                career_data,
                global_scores,
            ) = self._build_matchup_payload(fighter1, fighter2, db)

            return {
                "names": fighter_names,
                "fighter_1": fighter_bio_1,
                "fighter_2": fighter_bio_2,
                "fighter_data_1": fighter_data_1,
                "fighter_data_2": fighter_data_2,
                "strike_fig": strike_fig,
                "grappling_fig": grappling_fig,
                "career_fig": career_fig,
                "heat1": heat1,
                "heat2": heat2,
                "compare_plots": compare_plots,
                "career_data": career_data,
                "global_scores": global_scores,
            }
        finally:
            conn.close()

    def _build_matchup_payload(self, fighter1_name: str, fighter2_name: str, db: sq.Cursor):
        fighter_bio_1, fighter_bio_2 = get_two_fighters(fighter1_name, fighter2_name, db)
        fighter_bio_1["team"] = fighter_bio_1["team"].title() if fighter_bio_1["team"] else None
        fighter_bio_2["team"] = fighter_bio_2["team"].title() if fighter_bio_2["team"] else None

        fighter_data_1 = get_fighter_data(fighter_bio_1["id"], db)
        fighter_data_2 = get_fighter_data(fighter_bio_2["id"], db)

        strike_fig, grappling_fig, career_fig = plot_mergers(fighter_bio_1["id"], fighter_bio_2["id"], db)
        heat1 = strike_heatmap(fighter_bio_1["id"], db).to_html(full_html=False)
        heat2 = strike_heatmap(fighter_bio_2["id"], db).to_html(full_html=False)

        comparison_strike_plot, _ = comparison_plot(
            fighter_bio_1["id"], fighter_bio_2["id"], db, compare_type="striking"
        )
        comparison_grappling_plot, _ = comparison_plot(
            fighter_bio_1["id"], fighter_bio_2["id"], db, compare_type="grappling"
        )
        comparison_career_plot, career_data = comparison_plot(
            fighter_bio_1["id"], fighter_bio_2["id"], db, compare_type="career"
        )
        compare_plots = [
            comparison_strike_plot.to_html(full_html=False),
            comparison_grappling_plot.to_html(full_html=False),
            comparison_career_plot.to_html(full_html=False),
        ]
        global_scores = [get_global_score(db, fighter_bio_1["id"]), get_global_score(db, fighter_bio_2["id"])]
        career_data = career_data_cleaner(career_data)

        return (
            fighter_bio_1,
            fighter_bio_2,
            fighter_data_1,
            fighter_data_2,
            strike_fig,
            grappling_fig,
            career_fig,
            heat1,
            heat2,
            compare_plots,
            career_data,
            global_scores,
        )

    def authenticate(self, username: str, password: str) -> int | None:
        rows = self.repository.get_user_by_username(username)
        if len(rows) != 1:
            return None
        if not check_password_hash(rows[0]["hash"], password):
            return None
        return rows[0]["id"]

    def register_user(self, username: str, password: str) -> tuple[bool, str]:
        try:
            self.repository.create_user(username, generate_password_hash(password))
            return True, ""
        except sq.IntegrityError:
            return False, "Already registered!"

    def search_data(self, query: str) -> dict[str, Any]:
        fighters_list: list[Any] = []
        match_1, match_2 = self.repository.search_fighters(query)
        if len(query) != 1:
            if match_1 and not match_2:
                fighters_list = match_1
            elif match_2 and not match_1:
                fighters_list = match_2
            elif match_2 and match_1:
                fighters_list = match_1 if len(match_1) > len(match_2) else match_2
        elif match_1:
            fighters_list = match_1
        return {"matched_number": len(fighters_list), "fighters_list": fighters_list}

    def rankings_data(self, action: str | None) -> dict[str, Any]:
        fighters = self.repository.get_rankings(action, self.weight_hash) if action else []
        return {"fighters": fighters, "chosen_class": action}

    def fighter_data(self, fighter_id: str, form: dict[str, str] | None) -> dict[str, Any] | None:
        conn, db = self.repository.cursor()
        try:
            fighter = self.repository.get_fighter_by_id(fighter_id)
            if fighter is None:
                return None
            fighter_obj = dict(fighter)
            if fighter_obj["birthday"]:
                fighter_obj["birthday"] = (
                    date.today().year - datetime.strptime(fighter_obj["birthday"], "%m/%d/%Y").year
                )
            fighter_obj["weight"] = fighter_obj["weight"].replace(".", "")
            if fighter_obj["team"]:
                fighter_obj["team"] = fighter_obj["team"].title()

            selection = "career"
            quantity = 1
            plot = elo_history_plot(fighter_id).to_html(full_html=False)
            heat_map = strike_heatmap(2373, db)
            weaknesses: dict[str, Any] = {}
            strengths: dict[str, Any] = {}
            elo_hash = elo_analysis(fighter_id)
            career_hash = career_analysis(db=db, id=fighter_id, cached=False)
            career_hash["finish_rate"] = f"{career_hash['finish_rate'] * 100 : .1f}%"
            career_hash["win_rate"] = f"{career_hash['win_rate'] * 100 : .1f}%"
            data_hash = career_hash
            fights = get_career_fights()
            last_5 = data_hash["last_5"]

            if form:
                quantity = int(form.get("num", 5))
                selection = form.get("action", "career")
                if selection == "striking":
                    plot = striking_analysis_plot(fighter_id, db).to_html(full_html=False)
                    heat_map = strike_heatmap(fighter_id, db).to_html(full_html=False)
                    data_hash = get_hash_data(db, "striking", fighter_id)
                elif selection == "clinch":
                    plot = clinching_analysis_plot(fighter_id, db).to_html(full_html=False)
                    data_hash = get_hash_data(db, "clinching", fighter_id)
                elif selection == "grappling":
                    plot = grappling_analysis_plot(fighter_id, db).to_html(full_html=False)
                    data_hash = get_hash_data(db, "grappling", fighter_id)
                elif selection == "overall":
                    plot = career_plot(fighter_id, db).to_html(full_html=False)
                    data_hash = get_hash_data(db, "global", fighter_id)
                    weaknesses = get_scaled_attributes(best=False, db=db, fighter_id=fighter_id, quantity=quantity)
                    strengths = get_scaled_attributes(best=True, db=db, fighter_id=fighter_id, quantity=quantity)
                elif selection == "record":
                    fights = get_career_fights(fighter_id=fighter_id)
                else:
                    weaknesses = get_scaled_attributes(best=False, db=db, fighter_id=fighter_id, quantity=5)
                    strengths = get_scaled_attributes(best=True, db=db, fighter_id=fighter_id, quantity=5)

            return {
                "id": fighter_id,
                "fighter": fighter_obj,
                "elo_data_hash": elo_hash,
                "selection": selection,
                "plot": plot,
                "data_hash": data_hash,
                "last_5": last_5,
                "last_fight": career_hash["last_fight"],
                "heat_map": heat_map,
                "weaknesses": weaknesses,
                "strengths": strengths,
                "quantity": quantity,
                "fights": fights,
            }
        finally:
            conn.close()

    def versus_data(self, fight_id: str) -> dict[str, Any]:
        conn, db = self.repository.cursor()
        try:
            fight, event, fighter_a, fighter_b = fight_analysis(db, fight_id)
        finally:
            conn.close()

        fight["fighter_a"]["elo_diff"] = fight["fighter_a"]["new_elo"] - fight["fighter_a"]["elo"]
        fight["fighter_b"]["elo_diff"] = fight["fighter_b"]["new_elo"] - fight["fighter_b"]["elo"]
        striking = {
            "fighter_a": {
                "name": fighter_a["name"],
                "total_strikes_landed": fighter_a["total_str_landed"],
                "total_strikes_attempted": fighter_a["total_str_attempted"],
                "total_significant_strikes_landed": fighter_a["sig_str_landed"],
                "total_significant_strikes_attempted": fighter_a["sig_str_attempted"],
                "significant_strike_percent": fighter_a["sig_str_percent"],
                "knock-downs": fighter_a["kd"],
            },
            "fighter_b": {
                "name": fighter_b["name"],
                "total_strikes_landed": fighter_b["total_str_landed"],
                "total_strikes_attempted": fighter_b["total_str_attempted"],
                "total_significant_strikes_landed": fighter_b["sig_str_landed"],
                "total_significant_strikes_attempted": fighter_b["sig_str_attempted"],
                "significant_strike_percent": fighter_b["sig_str_percent"],
                "knock-downs": fighter_b["kd"],
            },
        }
        grappling = {
            "fighter_a": {
                "name": fighter_a["name"],
                "takedowns": fighter_a["td_landed"],
                "take_down_percent": fighter_a["td_percent"],
                "sub_attempts": fighter_a["sub_att"],
                "reversals": fighter_a["rev"],
                "control_time": fighter_a["ctr"],
            },
            "fighter_b": {
                "name": fighter_b["name"],
                "takedowns": fighter_b["td_landed"],
                "take_down_percent": fighter_b["td_percent"],
                "sub_attempts": fighter_b["sub_att"],
                "reversals": fighter_b["rev"],
                "control_time": fighter_b["ctr"],
            },
        }
        return {"fight": fight, "event": event, "striking": striking, "grappling": grappling}
