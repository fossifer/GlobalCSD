import re
import json
import sqlite3
import toolforge
from enums import CSDCATS, RMCATCMT, ADDCATCMT
from sseclient import SSEClient as EventSource
from phpserialize import unserialize

toolforge.set_user_agent('GlobalCSD/0.1 (https://tools.wmflabs.org/globalcsd/; https://meta.wikimedia.org/wiki/User_talk:WhitePhosphorus) Python/3.5 update.py/0.1')

conn = sqlite3.connect('pages.db')
cur = conn.cursor()

# get title from the summaries like "[[:title]] removed from category"
add_title_re = re.compile(r'|'.join([s.replace('[[:$1]]', r'\[\[:(.+)\]\]')
    for s in ADDCATCMT.values()]))
rm_title_re = re.compile(r'|'.join([s.replace('[[:$1]]', r'\[\[:(.+)\]\]')
    for s in RMCATCMT.values()]))

# load admin list to memory
cur.execute('''SELECT site, GROUP_CONCAT(username, '|') FROM admin
    GROUP BY site''')
all_admins = {wiki: users.split('|') for (wiki, users) in cur.fetchall()}


def tools_conn():
    # a cache of connections to toolforge databases
    _toolforge_conns = dict()
    def _fetch(wiki):
        if wiki not in _toolforge_conns:
            _toolforge_conns[wiki] = toolforge.connect(wiki)
        return _toolforge_conns[wiki]
    return _fetch
tools_conns = tools_conn()


url = 'https://stream.wikimedia.org/v2/stream/recentchange'
for event in EventSource(url):
    if event.event == 'message':
        try:
            change = json.loads(event.data)

            if change['user'] in all_admins.get(change['wiki'], []):
                # check admin expiry
                cur.execute('''SELECT expiry FROM admin
                    WHERE site = ? AND username = ?''',
                    (change['wiki'], change['user']))
                expiry = cur.fetchone()
                if expiry and expiry < change['timestamp']:
                    cur.execute('''DELETE FROM admin
                        WHERE site = ? AND username = ?''',
                        (change['wiki'], change['user']))
                    all_admins[change['wiki']] = [
                        a for a in all_admins[change['wiki']]
                        if a != change['user']]
                else:
                    # update admin action time
                    cur.execute('''UPDATE admin
                        SET action = ?
                        WHERE site = ? AND username = ?''',
                        (change['timestamp'], change['wiki'], change['user']))
                conn.commit()

            if change['type'] == 'categorize':
                if change['title'] != CSDCATS.get(change['wiki']):
                    continue
                add_match = add_title_re.search(change['comment'])
                title = None
                if add_match:
                    title = next(t for t in add_match.groups() if t)
                    cur.execute('''INSERT INTO entry
                        (site, siteurl, title, requester, bot, ts) VALUES
                        (?,?,?,?,?,?)''',
                        (change['wiki'], change['server_url'], title,
                        change['user'], change['bot'], change['timestamp']))
                    cur.execute('''INSERT INTO log
                        (site, siteurl, title, user, type, bot, ts) VALUES
                        (?,?,?,?,?,?,?)''',
                        (change['wiki'], change['server_url'], title,
                        change['user'], 'add', change['bot'],
                        change['timestamp']))
                    conn.commit()
                else:
                    rm_match = rm_title_re.search(change['comment'])
                    title = next(t for t in rm_match.groups() if t)
                    cur.execute("SELECT * FROM entry WHERE title = ?",
                        (title,))
                    if cur.fetchone():
                        cur.execute("DELETE FROM entry WHERE title = ?",
                            (title,))
                        cur.execute('''INSERT INTO log
                        (site, siteurl, title, user, type, bot, ts) VALUES
                        (?,?,?,?,?,?,?)''',
                        (change['wiki'], change['server_url'], title,
                        change['user'], 'rm', change['bot'],
                        change['timestamp']))
                        conn.commit()

            elif change['type'] == 'log':
                if change['log_type'] == 'rights':
                    params = unserialize(bytes(change['log_params']))
                    user, wiki = change['log_title'], change['wiki']
                    if '@' in user:
                        # user@wiki
                        wiki = user[user.index('@')+1:]
                        user = user[:user.index('@')]
                    try:
                        oldgroups = params[b'4::oldgroups']
                        oldmetadata = params[b'oldmetadata']
                        newgroups = params[b'5::newgroups']
                        newmetadata = params[b'newmetadata']
                        ogl = list(oldgroups.values())
                        ngl = list(newgroups.values())
                        osi = ogl.index(b'sysop') if b'sysop' in ogl else -1
                        nsi = ngl.index(b'sysop') if b'sysop' in ngl else -1
                        if osi == -1 and nsi == -1:
                            continue
                        if osi == -1:
                            # new sysop
                            newexpiry = newmetadata[nsi][b'expiry']
                            with tools_conns(wiki).cursor() as tool_cur:
                                tool_cur.execute('''
                                    SELECT actor_user, MAX(log_timestamp), MAX(rev_timestamp)
                                    FROM actor
                                    JOIN logging ON log_actor = actor_id
                                    JOIN revision_actor_temp ON revactor_actor = actor_id
                                    JOIN revision ON rev_id = revactor_rev
                                    WHERE actor_name = '?';''', user)
                                (uid, lts, rts) = tool_cur.fetchone()
                                cur.execute('''INSERT INTO admin
                                    (site, userid, username, expiry, action)
                                    VALUES (?,?,?,?,?)''',
                                    (wiki, uid, user, newexpiry, max(lts, rts)))
                                conn.commit()
                        elif nsi == -1:
                            # desysop
                            cur.execute('''DELETE FROM admin WHERE
                                user = ? AND wiki = ?''', (user, wiki))
                            conn.commit()
                        else:
                            # expiry changed, or other permissions changed
                            oldexpiry = oldmetadata[osi][b'expiry']
                            newexpiry = newmetadata[nsi][b'expiry']
                            if oldexpiry == newexpiry:
                                continue
                            # so it is expiry changed
                            cur.execute('''UPDATE admin
                                SET expiry = ? WHERE user = ? AND wiki = ?''',
                                (newexpiry, user, wiki))
                            conn.commit()
                    except KeyError:
                        pass
                    continue
                elif change['log_type'] != 'delete' or change['log_action'] != 'delete':
                    continue
                cur.execute("SELECT * FROM entry WHERE title = ?",
                    (change['title'],))
                if cur.fetchone():
                    cur.execute("DELETE FROM entry WHERE title = ?", (title,))
                    cur.execute('''INSERT INTO log
                        (site, siteurl, title, user, type, bot, ts) VALUES
                        (?,?,?,?,?,?,?)''',
                    (change['wiki'], change['server_url'], change['title'],
                    change['user'], 'del', change['bot'],
                    change['timestamp']))
                    conn.commit()
        except (ValueError, KeyError):
            # change parse error
            pass
        except AttributeError:
            # summary parse error
            pass
        else:
            pass


def main():
    pass


if __name__ == '__main__':
    main()
