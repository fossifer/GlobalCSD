import sqlite3

conn = sqlite3.connect('pages.db')
c = conn.cursor()

# entry table lists the current speedy deletion candidates
c.execute('''CREATE TABLE entry
            (id integer primary key, site text, siteurl text, title text,
             requester text, bot integer, ts integer)''')
# log table includes who requested for SD and who deleted a candidate, etc.
# add: added to CSD category, e.g. added speedy templates
# rm: removed from CSD category, e.g. removed speedy templates
# del: deleted the related page
c.execute('''CREATE TABLE log
            (id integer primary key, site text, siteurl text, title text,
             user text, type text check( type in ('add', 'rm', 'del') ),
             bot integer, ts integer)''')
# admin table includes admin info for each wiki, they are
# sysop list and last action time.
c.execute('''CREATE TABLE admin
            (id integer primary key, site text, userid integer, username text,
             expiry integer, action integer)''')

conn.commit()
conn.close()