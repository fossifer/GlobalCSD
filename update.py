import re
import json
import sqlite3
from enums import CSDCATS, RMCATCMT, ADDCATCMT
from sseclient import SSEClient as EventSource

conn = sqlite3.connect('pages.db')
cur = conn.cursor()

# get title from the summaries like "[[:title]] removed from category"
add_title_re = re.compile(r'|'.join([s.replace('[[:$1]]', r'\[\[:(.+)\]\]')
    for s in ADDCATCMT.values()]))
rm_title_re = re.compile(r'|'.join([s.replace('[[:$1]]', r'\[\[:(.+)\]\]')
    for s in RMCATCMT.values()]))

url = 'https://stream.wikimedia.org/v2/stream/recentchange'
for event in EventSource(url):
    if event.event == 'message':
        try:
            change = json.loads(event.data)

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
                if change['log_type'] != 'delete':
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
