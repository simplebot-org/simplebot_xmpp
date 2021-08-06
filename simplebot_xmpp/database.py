import sqlite3
from typing import Generator, Optional


class DBManager:
    def __init__(self, db_path: str) -> None:
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS channels
                (id INTEGER PRIMARY KEY,
                name TEXT NOT NULL)"""
            )
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS cchats
                (id INTEGER PRIMARY KEY,
                channel INTEGER NOT NULL)"""
            )
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS nicks
                (addr TEXT PRIMARY KEY,
                nick TEXT NOT NULL)"""
            )
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS whitelist
                (channel INTEGER PRIMARY KEY)"""
            )

    def execute(self, statement: str, args=()) -> sqlite3.Cursor:
        return self.db.execute(statement, args)

    def commit(self, statement, args=()) -> sqlite3.Cursor:
        with self.db:
            return self.db.execute(statement, args)

    def close(self) -> None:
        self.db.close()

    # ===== channels =======

    def channel_exists(self, channel: str) -> bool:
        channel = channel.lower()
        r = self.execute("SELECT * FROM channels WHERE name=?", (channel,)).fetchone()
        return r is not None

    def get_channel_by_gid(self, gid: int) -> Optional[str]:
        r = self.db.execute("SELECT channel FROM cchats WHERE id=?", (gid,)).fetchone()
        if r:
            r = self.db.execute("SELECT name FROM channels WHERE id=?", (r[0],))
            r = r.fetchone()
        return r and r[0]

    def get_channels(self) -> Generator:
        for r in self.db.execute("SELECT name FROM channels"):
            yield r[0]

    def add_channel(self, channel: str) -> None:
        channel = channel.lower()
        self.commit("INSERT INTO channels VALUES (?,?)", (None, channel))

    def remove_channel(self, channel: str) -> None:
        channel = channel.lower()
        self.commit("DELETE FROM channels WHERE name=?", (channel,))

    # ===== cchats =======

    def get_cchats(self, channel: str) -> Generator:
        channel = channel.lower()
        r = self.db.execute("SELECT id FROM channels WHERE name=?", (channel,))
        row = r.fetchone()
        if row is not None:
            rows = self.db.execute("SELECT id FROM cchats WHERE channel=?", (row[0],))
            for row in rows:
                yield row[0]

    def add_cchat(self, gid: int, channel: str) -> None:
        channel = channel.lower()
        r = self.db.execute("SELECT id FROM channels WHERE name=?", (channel,))
        channel = r.fetchone()[0]
        self.commit("INSERT INTO cchats VALUES (?,?)", (gid, channel))

    def remove_cchat(self, gid: int) -> None:
        self.commit("DELETE FROM cchats WHERE id=?", (gid,))

    # ===== nicks =======

    def get_nick(self, addr: str) -> str:
        r = self.execute("SELECT nick from nicks WHERE addr=?", (addr,)).fetchone()
        if r:
            return r[0]
        i = 1
        while True:
            nick = "User{}".format(i)
            if not self.get_addr(nick):
                self.set_nick(addr, nick)
                break
            i += 1
        return nick

    def set_nick(self, addr: str, nick: str) -> None:
        self.commit("REPLACE INTO nicks VALUES (?,?)", (addr, nick))

    def get_addr(self, nick: str) -> str:
        r = self.execute("SELECT addr FROM nicks WHERE nick=?", (nick,)).fetchone()
        return r and r[0]

    # ===== whitelist =======

    def is_whitelisted(self, channel: str) -> bool:
        channel = channel.lower()
        rows = self.execute("SELECT channel FROM whitelist").fetchall()
        if not rows:
            return True
        for row in rows:
            r = self.db.execute("SELECT name FROM channels WHERE id=?", (row[0],))
            row = r.fetchone()
            if row and row[0] == channel:
                return True
        return False

    def add_to_whitelist(self, channel: str) -> None:
        channel = channel.lower()
        r = self.db.execute(
            "SELECT id FROM channels WHERE name=?", (channel,)
        ).fetchone()
        if r is None:
            self.add_channel(channel)
            r = self.db.execute("SELECT id FROM channels WHERE name=?", (channel,))
            channel = r.fetchone()[0]
        else:
            channel = r[0]
        self.commit("INSERT INTO whitelist VALUES (?)", (channel,))

    def remove_from_whitelist(self, channel: str) -> None:
        channel = channel.lower()
        r = self.db.execute(
            "SELECT id FROM channels WHERE name=?", (channel,)
        ).fetchone()
        self.commit("DELETE FROM whitelist WHERE channel=?", (r[0],))
