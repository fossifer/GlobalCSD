import sqlite3
import toolforge
from enum import CSDCATS

toolforge.set_user_agent('GlobalCSD/0.1 (https://tools.wmflabs.org/globalcsd/; https://meta.wikimedia.org/wiki/User_talk:WhitePhosphorus) Python/3.5 init.py/0.1')

local_conn = sqlite3.connect('pages.db')
local_cur = local_conn.cursor()

for wiki in CSDCATS.keys():
    conn = toolforge.connect(wiki)
    with conn.cursor() as cur:
        # admin info
        cur.execute('''SELECT user_id, user_name, ug_expiry
            FROM user_groups JOIN user ON ug_user = user_id
            WHERE ug_group LIKE "%sysop%";''')
        admins = cur.fetchall()
        admin_ids = (a[0] for a in admins)
        admins = {a[0]: (a[1], a[2]) for a in admins}
        # get last edit time
        # https://stackoverflow.com/a/28090544
        cur.execute('''SELECT ra.actor_user, r.rev_timestamp
            FROM revision r
            JOIN revision_actor_temp rat ON rat.revactor_rev = r.rev_id
            JOIN actor ra ON rat.revactor_actor = ra.actor_id
            LEFT JOIN revision b
            JOIN revision_actor_temp bat ON bat.revactor_rev = b.rev_id
            JOIN actor ba ON bat.revactor_actor = ba.actor_id
            ON ra.actor_user = ba.actor_user
                AND r.rev_timestamp < b.rev_timestamp
            WHERE ra.actor_user IN (%s) AND b.rev_timestamp IS NULL
            ''' % ','.join(['%s'] * len(admins)), admin_ids)
        last_edit_time = dict(cur.fetchall())
        # get last log time
        cur.execute('''SELECT ra.actor_user, r.log_timestamp
            FROM logging r
            JOIN actor ra ON r.log_actor = ra.actor_id
            LEFT JOIN logging b
            JOIN actor ba ON b.log_actor = ba.actor_id
            ON ra.actor_user = ba.actor_user
                AND r.log_timestamp < b.log_timestamp
            WHERE ra.actor_user IN (%s) AND b.log_timestamp IS NULL
            ''' % ','.join(['%s'] * len(admins)), admin_ids)
        last_log_time = dict(cur.fetchall())
        # get maximum of both timestamps,
        # and construct the tuple to insert
        rst = ((wiki,
                a,
                admins[a][0],
                admins[a][1],
                max(last_edit_time.get(a, 0), last_log_time.get(a, 0)))
            for a in admin_ids)
        local_cur.executemany('''INSERT INTO admin
            (site, userid, username, expiry, action) VALUES (?,?,?,?,?)''',
            rst)
        