import asyncio
import os
import re
from threading import Event, Thread
from typing import Generator

import simplebot
from deltachat import Chat, Contact, Message
from pkg_resources import DistributionNotFound, get_distribution
from simplebot.bot import DeltaBot, Replies

from .database import DBManager
from .xmpp import XMPPBot

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    __version__ = "0.0.0.dev0-unknown"
nick_re = re.compile(r"[a-zA-Z0-9]{1,30}$")
db: DBManager
xmpp_bridge: XMPPBot


@simplebot.hookimpl
def deltabot_init(bot: DeltaBot) -> None:
    global db
    db = _get_db(bot)

    _getdefault(bot, "nick", "DC-Bridge")
    _getdefault(bot, "max_group_size", "20")
    if _getdefault(bot, "allow_bridging", "1") == "1":
        bot.commands.register("/xmpp_bridge", cmd_bridge)


@simplebot.hookimpl
def deltabot_start(bot: DeltaBot) -> None:
    jid = bot.get("jid", scope=__name__)
    password = bot.get("password", scope=__name__)
    nick = _getdefault(bot, "nick")

    assert jid is not None, 'Missing "{}/jid" setting'.format(__name__)
    assert password is not None, 'Missing "{}/password" setting'.format(__name__)

    bridge_init = Event()
    Thread(
        target=_listen_to_xmpp,
        args=(bot, jid, password, nick, bridge_init),
        daemon=True,
    ).start()
    bridge_init.wait()


@simplebot.hookimpl
def deltabot_member_removed(bot: DeltaBot, chat: Chat, contact: Contact) -> None:
    me = bot.self_contact
    if me == contact or len(chat.get_contacts()) <= 1:
        channel = db.get_channel_by_gid(chat.id)
        if channel:
            db.remove_cchat(chat.id)
            if next(db.get_cchats(channel), None) is None:
                db.remove_channel(channel)
                xmpp_bridge.leave_channel(channel)


@simplebot.filter(name=__name__)
def filter_messages(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Process messages sent to XMPP channels."""
    chan = db.get_channel_by_gid(message.chat.id)
    if not chan:
        return

    if not message.text or message.filename:
        replies.add(text="Unsupported message")
        return

    nick = db.get_nick(message.get_sender_contact().addr)
    text = "{}[dc]:\n{}".format(nick, message.text)

    bot.logger.debug("Sending message to XMPP: %r", text)
    xmpp_bridge.send_message(chan, text, mtype="groupchat")
    for g in _get_cchats(bot, chan):
        if g.id != message.chat.id:
            replies.add(text=text, chat=g)


@simplebot.command
def xmpp_members(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Show list of XMPP channel members."""
    me = bot.self_contact

    chan = db.get_channel_by_gid(message.chat.id)
    if not chan:
        replies.add(text="This is not an XMPP channel")
        return

    members = "Members:\n"
    for g in _get_cchats(bot, chan):
        for c in g.get_contacts():
            if c != me:
                members += "• {}[dc]\n".format(db.get_nick(c.addr))

    for m in xmpp_bridge.get_members(chan):
        if m != xmpp_bridge.nick:
            members += "• {}[xmpp]\n".format(m)

    replies.add(text=members)


@simplebot.command
def xmpp_nick(args: list, message: Message, replies: Replies) -> None:
    """Set your XMPP nick or display your current nick if no new nick is given."""
    addr = message.get_sender_contact().addr
    new_nick = " ".join(args)
    if new_nick:
        if not nick_re.match(new_nick):
            replies.add(
                text="** Invalid nick, only letters and numbers are"
                " allowed, and nick should be less than 30 characters"
            )
        elif db.get_addr(new_nick):
            replies.add(text="** Nick already taken")
        else:
            db.set_nick(addr, new_nick)
            replies.add(text="** Nick: {}".format(new_nick))
    else:
        replies.add(text="** Nick: {}".format(db.get_nick(addr)))


@simplebot.command
def xmpp_join(bot: DeltaBot, payload: str, message: Message, replies: Replies) -> None:
    """Join the given XMPP channel."""
    sender = message.get_sender_contact()
    if not payload:
        replies.add(text="Wrong syntax")
        return
    if not db.is_whitelisted(payload):
        replies.add(text="That channel isn't in the whitelist")
        return

    if not db.channel_exists(payload):
        xmpp_bridge.join_channel(payload)
        db.add_channel(payload)

    chats = _get_cchats(bot, payload)
    g = None
    gsize = int(_getdefault(bot, "max_group_size"))
    for group in chats:
        contacts = group.get_contacts()
        if sender in contacts:
            replies.add(text="You are already a member of this channel", chat=group)
            return
        if len(contacts) < gsize:
            g = group
            gsize = len(contacts)
    if g is None:
        g = bot.create_group(payload, [sender])
        db.add_cchat(g.id, payload)
    else:
        _add_contact(g, sender)

    nick = db.get_nick(sender.addr)
    replies.add(text="** You joined {} as {}".format(payload, nick))


def cmd_bridge(payload: str, message: Message, replies: Replies) -> None:
    """Bridge current group to the given XMPP channel."""
    if not payload:
        replies.add(text="Wrong syntax")
        return
    if not db.is_whitelisted(payload):
        replies.add(text="That channel isn't in the whitelist")
        return
    channel = db.get_channel_by_gid(message.chat.id)
    if channel:
        replies.add(
            text="This chat is already bridged with channel: {}".format(channel)
        )
        return

    if not db.channel_exists(payload):
        db.add_channel(payload)
        xmpp_bridge.join_channel(payload)

    db.add_cchat(message.chat.id, payload)
    text = "** This chat is now bridged with XMPP channel: {}".format(payload)
    replies.add(text=text)


@simplebot.command
def xmpp_remove(
    bot: DeltaBot, payload: str, message: Message, replies: Replies
) -> None:
    """Remove the DC member with the given nick from the XMPP channel, if no nick is given remove yourself."""
    sender = message.get_sender_contact()

    channel = db.get_channel_by_gid(message.chat.id)
    if not channel:
        args = payload.split(maxsplit=1)
        channel = args[0]
        payload = args[1] if len(args) == 2 else ""
        for g in _get_cchats(bot, channel):
            if sender in g.get_contacts():
                break
        else:
            replies.add(text="You are not a member of that channel")
            return

    if not payload:
        payload = sender.addr
    if "@" not in payload:
        t = db.get_addr(payload)
        if not t:
            replies.add(text="Unknow user: {}".format(payload))
            return
        text = t

    for g in _get_cchats(bot, channel):
        for c in g.get_contacts():
            if c.addr == text:
                g.remove_contact(c)
                if c == sender:
                    return
                s_nick = db.get_nick(sender.addr)
                nick = db.get_nick(c.addr)
                text = "** {} removed by {}".format(nick, s_nick)
                for cchat in _get_cchats(bot, channel):
                    replies.add(text=text, chat=cchat)
                text = "Removed from {} by {}".format(channel, s_nick)
                bot.get_chat(c).send_text(text)
                return


def _listen_to_xmpp(
    bot: DeltaBot, jid: str, password: str, nick: str, bridge_initialized: Event
) -> None:
    global xmpp_bridge
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    xmpp_bridge = XMPPBot(jid, password, nick, db, bot)
    bridge_initialized.set()
    while True:
        try:
            bot.logger.info("Starting XMPP bridge")
            xmpp_bridge.connect()
            xmpp_bridge.process(forever=False)
        except Exception as ex:
            bot.logger.exception(ex)


def _get_cchats(bot: DeltaBot, channel: str) -> Generator:
    for gid in db.get_cchats(channel):
        yield bot.get_chat(gid)


def _getdefault(bot: DeltaBot, key: str, value: str = None) -> str:
    val = bot.get(key, scope=__name__)
    if val is None and value is not None:
        bot.set(key, value, scope=__name__)
        val = value
    return val


def _get_db(bot: DeltaBot) -> DBManager:
    path = os.path.join(os.path.dirname(bot.account.db_path), __name__)
    if not os.path.exists(path):
        os.makedirs(path)
    return DBManager(os.path.join(path, "sqlite.db"))


def _add_contact(chat: Chat, contact: Contact) -> None:
    img_path = chat.get_profile_image()
    if img_path and not os.path.exists(img_path):
        chat.remove_profile_image()
    chat.add_contact(contact)
