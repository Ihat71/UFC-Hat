import sqlite3 as sq
from typing import Any


class ApiRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self, row_factory: bool = True) -> sq.Connection:
        conn = sq.connect(self.db_path)
        if row_factory:
            conn.row_factory = sq.Row
        return conn

    def get_filter_options(self) -> tuple[list[str], list[str]]:
        with self._connect() as conn:
            db = conn.cursor()
            countries = [row[0] for row in db.execute("select distinct country from fighters").fetchall()]
            teams = [row[0] for row in db.execute("select distinct team from fighters").fetchall()]
        return countries, teams

    def get_fighters_filtered(
        self,
        weight: str | None,
        country: str | None,
        team: str | None,
        weight_hash: dict[str, Any],
    ) -> list[sq.Row]:
        query = "select * from fighters"
        filters: list[str] = []
        answers: list[Any] = []

        if weight:
            if weight not in ("All", "Heavyweight"):
                filters.append("weight IN (?)")
                answers.append(weight_hash[weight])
            elif weight == "Heavyweight":
                filters.append("cast(replace(weight, ' lbs.', '') as integer) > ?")
                answers.append(207)

        if country and country != "None":
            filters.append("country = ?")
            answers.append(country)
        if team and team != "None":
            filters.append("team = ?")
            answers.append(team)

        if filters:
            query += " where " + " and ".join(filters)

        with self._connect() as conn:
            db = conn.cursor()
            return db.execute(query, tuple(answers)).fetchall()

    def get_events(self) -> list[sq.Row]:
        with self._connect() as conn:
            db = conn.cursor()
            return db.execute("select * from events;").fetchall()

    def get_event_by_id(self, event_id: str) -> sq.Row | None:
        with self._connect() as conn:
            db = conn.cursor()
            rows = db.execute("select * from events where event_id = ?", (event_id,)).fetchall()
            return rows[0] if rows else None

    def get_all_fighters(self) -> list[sq.Row]:
        with self._connect() as conn:
            db = conn.cursor()
            return db.execute("select * from fighters").fetchall()

    def get_user_by_username(self, username: str) -> list[sq.Row]:
        with self._connect() as conn:
            db = conn.cursor()
            return db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchall()

    def create_user(self, username: str, password_hash: str) -> None:
        with self._connect(row_factory=False) as conn:
            db = conn.cursor()
            db.execute("INSERT INTO users (username, hash) VALUES (?, ?)", (username, password_hash))
            conn.commit()

    def search_fighters(self, query_text: str) -> tuple[list[sq.Row], list[sq.Row]]:
        first_name_like = query_text + "%"
        last_name_like = "%" + query_text
        with self._connect() as conn:
            db = conn.cursor()
            match_1 = db.execute(
                "select * from fighters where name like ?",
                (first_name_like.lower().title().strip(),),
            ).fetchall()
            match_2 = db.execute(
                "select * from fighters where name like ?",
                (last_name_like.lower().title().strip(),),
            ).fetchall()
        return match_1, match_2

    def get_rankings(self, action: str | None, weight_hash: dict[str, Any]) -> list[sq.Row]:
        original_query = (
            "Select f.fighter_id, f.name, f.picture, e.elo "
            "from fighters f join elo e on f.fighter_id = e.fighter_id"
        )
        query = original_query
        answers: list[Any] = []
        if action in weight_hash:
            if action == "Heavyweight":
                query += " where cast(replace(f.weight, ' lbs.', '') as integer) > ?"
                answers.append(207)
            elif action != "p4p":
                query += " where f.weight = ?"
                answers.append(weight_hash[action])
        query += " order by e.elo desc;"

        with self._connect() as conn:
            db = conn.cursor()
            return db.execute(query, tuple(answers)).fetchall()

    def get_fighter_by_id(self, fighter_id: str) -> sq.Row | None:
        with self._connect() as conn:
            db = conn.cursor()
            rows = db.execute("select * from fighters where fighter_id = ?", (fighter_id,)).fetchall()
            return rows[0] if rows else None

    def cursor(self) -> tuple[sq.Connection, sq.Cursor]:
        conn = self._connect()
        return conn, conn.cursor()
