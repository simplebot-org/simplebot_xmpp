import logging
from typing import Generator

from simplebot import DeltaBot
from slixmpp import ClientXMPP

from .database import DBManager

# from slixmpp.exceptions import IqError, IqTimeout


logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(message)s")


class XMPPBot(ClientXMPP):
    def __init__(
        self, jid: str, password: str, nick: str, db: DBManager, dbot: DeltaBot
    ) -> None:
        ClientXMPP.__init__(self, jid, password)
        self.nick = nick
        self.db = db
        self.dbot = dbot
        self.add_event_handler("session_start", self.on_session_start)
        self.add_event_handler("message", self.on_message)
        self.add_event_handler("disconnected", self.on_disconnected)

        self.register_plugin("xep_0045")  # Multi-User Chat
        self.register_plugin("xep_0054")  # vcard-temp
        self.register_plugin("xep_0363")  # HTTP File Upload
        self.register_plugin("xep_0128")  # Service Discovery Extensions
        # self.register_plugin('xep_0071')  # XHTML-IM

    def on_session_start(self, event) -> None:
        self.dbot.logger.debug("XMPP session started")
        self.send_presence(pstatus="Open source DeltaChat <--> XMPP bridge")
        self.get_roster()

        for jid in self.db.get_channels():
            self.join_channel(jid)

    def on_message(self, msg) -> None:
        nick = msg["mucnick"]
        if nick == self.nick:
            return

        if msg["type"] == "groupchat":
            self.dbot.logger.debug("Incoming XMPP message: %r", msg)
            for gid in self.db.get_cchats(msg["mucroom"]):
                self.dbot.get_chat(gid).send_text(
                    "{}[xmpp]:\n{}".format(nick, msg["body"])
                )

    def on_disconnected(self, event) -> None:
        self.dbot.logger.debug("XMPP bridge disconnected")
        self.abort()

    def join_channel(self, jid: str) -> None:
        self.dbot.logger.debug("Joining XMPP channel: %s", jid)
        self["xep_0045"].join_muc(jid, self.nick)

    def leave_channel(self, jid: str) -> None:
        self.dbot.logger.debug("Leaving XMPP channel: %s", jid)
        self["xep_0045"].leave_muc(jid, self.nick)

    def get_members(self, jid: str) -> Generator:
        for u in self["xep_0045"].get_roster(jid):
            if u:
                yield u
