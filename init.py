import sqlite3
import toolforge
from time import time
from enums import CSDCATS
from datetime import datetime as dt

toolforge.set_user_agent('GlobalCSD/0.1 (https://tools.wmflabs.org/globalcsd/; https://meta.wikimedia.org/wiki/User_talk:WhitePhosphorus) Python/3.5 init.py/0.1')

local_conn = sqlite3.connect('db.sqlite3', timeout=60)
local_cur = local_conn.cursor()

# Fetch admins of each site
local_cur.execute('''SELECT name FROM showcsd_wiki''')
wikis = local_cur.fetchall()
local_cur.execute('''SELECT name FROM showcsd_admin JOIN showcsd_wiki ON site_id = showcsd_wiki.id GROUP BY site_id''')
exclude_wikis = local_cur.fetchall()
wikis = set([w[0] for w in wikis]) - set([w[0] for w in exclude_wikis])
print(len(wikis), 'wikis')
for wiki in wikis:
    print(wiki)
    conn = toolforge.connect(wiki)
    with conn.cursor() as cur:
        starttime = time()
        cur.execute('''SELECT user_id, user_name, UNIX_TIMESTAMP(ug_expiry)
            FROM user_groups JOIN user ON ug_user = user_id
            WHERE ug_group LIKE "%sysop%";''')
        admins = cur.fetchall()
        print(len(admins), 'admins')
        if not admins:
            continue
        admin_ids = tuple(a[0] for a in admins)
        cur.execute('''SELECT actor_id FROM actor WHERE actor_user
            IN (%s)''' % ','.join(['%s'] * len(admins)), admin_ids)
        actor_ids = cur.fetchall()
        actor_ids = tuple(a[0] for a in actor_ids)
        admins_d = {actor_ids[i]: admins[i] for i in range(len(admins))}
        cur.execute('''SELECT rev_actor, UNIX_TIMESTAMP(MAX(rev_timestamp))
            FROM revision_userindex WHERE rev_actor
            IN (%s) GROUP BY rev_actor''' % ','.join(['%s'] * len(admins)),
            actor_ids)
        last_edit_time = cur.fetchall()
        cur.execute('''SELECT log_actor, UNIX_TIMESTAMP(MAX(log_timestamp))
            FROM logging_userindex WHERE log_actor
            IN (%s) GROUP BY log_actor''' % ','.join(['%s'] * len(admins)),
            actor_ids)
        last_log_time = cur.fetchall()
        last_edit_time_dict = {e[0]: e[1] for e in last_edit_time}
        last_log_time_dict = {e[0]: e[1] for e in last_log_time}
        last_action_time_dict = {k: max(
            last_edit_time_dict.get(k, -1), last_log_time_dict.get(k, -1))
            for k in admins_d.keys()}
        rst = tuple((wiki, admins_d[k][0], admins_d[k][1].decode('utf-8'),
                dt.utcfromtimestamp(admins_d[k][2]) \
                    if admins_d[k][2] else None,
                dt.utcfromtimestamp(last_action_time_dict[k]) \
                    if last_action_time_dict[k] != -1 else None)
            for k in admins_d.keys())
        print('query time:', time()-starttime)
        local_cur.executemany('''INSERT INTO showcsd_admin
            (site_id, userid, username, expiry, action)
            VALUES ((SELECT id FROM showcsd_wiki WHERE name = ?),?,?,?,?)''',
            rst)
        local_conn.commit()
