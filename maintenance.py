import time
import json
import argparse
import requests

parser = argparse.ArgumentParser(description='Do some maintenance jobs to keep database up-to-date.')
parser.add_argument('--enum', help='Generate enums.py', action='store_true')
parser.add_argument('--sdentry', help='Update speedy deletion cacndidates', action='store_true')
parser.add_argument('--admin', help='Updatea admin info', action='store_true')
args = parser.parse_args()


start_time = time.time()
if args.enum:
    # generate enums.py
    GSWIKILINK = 'https://meta.wikimedia.org/w/api.php?action=query&format=json&list=wikisets&utf8=1&wsfrom=Opted-out%20of%20global%20sysop%20wikis&wsprop=wikisincluded&wslimit=max'
    CSDCATSLINK = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=Q5964&props=sitelinks&utf8=1'
    RMCATCMTLINK = 'https://translatewiki.net/w/api.php?action=query&format=json&meta=messagetranslations&utf8=1&mttitle=MediaWiki%3ARecentchanges-page-removed-from-category'
    ADDCATCMTLINK = 'https://translatewiki.net/w/api.php?action=query&format=json&meta=messagetranslations&utf8=1&mttitle=MediaWiki%3ARecentchanges-page-added-to-category'
    GSWIKI = None
    CSDCATS = None
    RMCATCMT = None
    ADDCATCMT = None
    
    # get these enumerations
    r = requests.get(GSWIKILINK)
    try:
        s = r.json()
        wikis = s['query']['wikisets'][0]['wikisincluded']
        GSWIKI = wikis.values()
    except (json.decoder.JSONDecodeError, KeyError):
        print(time.time(), 'cannot fetch GSWIKI')
    r = requests.get(CSDCATSLINK)
    try:
        s = r.json()
        links = s['entities']['Q5964']['sitelinks']
        CSDCATS = {k: v['title'] for (k,v) in links.items()}
    except (json.decoder.JSONDecodeError, KeyError):
        print(time.time(), 'cannot fetch CSDCATS')
    
    r = requests.get(RMCATCMTLINK)
    try:
        s = r.json()
        translations = s['query']['messagetranslations']
        RMCATCMT = {m['language']: m['*'] for m in translations}
        # we don't need qqq (doc for translation) and it may cause issues in regex
        if 'qqq' in RMCATCMT:
            del RMCATCMT['qqq']
    except (json.decoder.JSONDecodeError, KeyError):
        print(time.time(), 'cannot fetch RMCATCMT')
    
    r = requests.get(ADDCATCMTLINK)
    try:
        s = r.json()
        translations = s['query']['messagetranslations']
        ADDCATCMT = {m['language']: m['*'] for m in translations}
        if 'qqq' in ADDCATCMT:
            del ADDCATCMT['qqq']
    except (json.decoder.JSONDecodeError, KeyError):
        print(time.time(), 'cannot fetch ADDCATCMT')
    
    # write to file
    with open('./enums.py', 'w', encoding='utf-8') as f:
        f.write('# AUTO GENERATED BY maintenance.py\n')
        f.write('# ' + GSWIKILINK + '\n')
        f.write("GSWIKI = {\n    '" + "',\n    '".join(GSWIKI))
        f.write("'\n}\n\n")
        f.write('# ' + CSDCATSLINK + '\n')
        f.write('CSDCATS = {\n')
        for k, v in CSDCATS.items():
            f.write('    "' + k + '": "' + v.replace('"', r'\"') + '",\n')
        f.write('}\n\n')
        f.write('# ' + RMCATCMTLINK + '\n')
        f.write('RMCATCMT = {\n')
        for k, v in RMCATCMT.items():
            f.write('    "' + k + '": "' + v.replace('"', r'\"') + '",\n')
        f.write('}\n\n')
        f.write('# ' + ADDCATCMTLINK + '\n')
        f.write('ADDCATCMT = {\n')
        for k, v in ADDCATCMT.items():
            f.write('    "' + k + '": "' + v.replace('"', r'\"') + '",\n')
        f.write('}\n')

    print('enum done:', time.time()-start_time, 'seconds')
    start_time = time.time()


if args.sdentry:
    import sqlite3
    import toolforge
    from enums import CSDCATS
    from datetime import datetime as dt
    toolforge.set_user_agent('GlobalCSD/0.1 (https://tools.wmflabs.org/globalcsd/; https://meta.wikimedia.org/wiki/User_talk:WhitePhosphorus) Python/3.5 maintenance.py/0.1')
    local_conn = sqlite3.connect('db.sqlite3', timeout=60)
    local_cur = local_conn.cursor()
    local_cur.execute('''SELECT name, url FROM showcsd_wiki''')
    wikis = local_cur.fetchall()
    print(len(wikis), 'wikis')
    for wiki in wikis:
        wiki, url = wiki
        if not CSDCATS.get(wiki): continue
        print(wiki)
        if url[-1] == '/': url = url[:-1]
        r = requests.get(url+'/w/api.php?action=query&format=json&meta=siteinfo&utf8=1&siprop=namespaces')
        allns = dict()
        try:
            allns = r.json()
            allns = allns['query']['namespaces']
        except (json.decoder.JSONDecodeError, KeyError):
            print(time.time(), 'cannot fetch ns info for', wiki)
            continue
        conn = toolforge.connect(wiki)
        with conn.cursor() as cur:
            cur.execute('''SELECT cl_from FROM categorylinks
                WHERE cl_to = %s AND cl_type IN ('file', 'page')''',
                (CSDCATS[wiki][CSDCATS[wiki].index(':')+1:].replace(' ', '_'),))
            pageids = cur.fetchall()
            if not pageids:
                print('rm all')
                continue
                # no csd for this wiki
                local_cur.execute('''DELETE FROM showcsd_sdentry
                    WHERE site_id =
                    (SELECT id FROM showcsd_wiki WHERE name = ?)''', wiki)
                local_conn.commit()
                continue
            pageids = tuple(p[0] for p in pageids)
            cur.execute('''SELECT page_id, page_namespace,
                page_title, page_latest FROM page
                WHERE page_id IN (%s)''' % ','.join(['%s'] * len(pageids)),
                pageids)
            pages = cur.fetchall()
            if not pages:
                print('cannot fetch page info (id:', pageids, '@', wiki)
                continue
            pagesdict = {pageid: [ns,
                ((allns[str(ns)]['*']+':') if allns[str(ns)]['*'] else '')+
                title.decode('utf-8').replace('_', ' '), rev]
                for (pageid, ns, title, rev) in pages}
            cur.execute('''SELECT rev_page, comment_text,
                UNIX_TIMESTAMP(rev_timestamp), actor_name FROM revision
                JOIN comment ON rev_comment_id = comment_id
                JOIN actor ON rev_actor = actor_id
                WHERE rev_id IN (%s)''' % ','.join(['%s'] * len(pagesdict)),
                [p[2] for p in pagesdict.values()])
            revs = cur.fetchall()
            for rev in revs:
                pagesdict[rev[0]].extend([rev[1].decode('utf-8'),
                    dt.utcfromtimestamp(rev[2]), rev[3].decode('utf-8')])
            local_cur.execute('''SELECT title FROM showcsd_sdentry
                JOIN showcsd_wiki ON site_id = showcsd_wiki.id
                WHERE name = ?''', (wiki,))
            cur_titles = set([a[0] for a in local_cur.fetchall()])
            new_titles = set([p[1] for p in pagesdict.values()])
            toadd = tuple(new_titles - cur_titles)
            torm = tuple(cur_titles - new_titles)
            toaddpages = list()
            for t in toadd:
                # TODO: this is ugly
                tid, tp = [(pid, p) for pid, p in pagesdict.items() if p[1] == t][0]
                toaddpages.append((wiki, t, tp[5], False, tp[4], tp[0], tp[3], tp[2]))
            print('add', len(toadd), 'pages')
            print('rm', len(torm), 'pages')

            local_cur.execute('''
                DELETE FROM showcsd_sdentry
                WHERE title IN (%s) AND site_id =
                (SELECT id FROM showcsd_wiki WHERE name = ?)
                ''' % ','.join(['?'] * len(torm)),
                torm+(wiki,))
            local_cur.executemany('''
                INSERT INTO showcsd_sdentry
                (site_id, title, requester, bot, ts, namespace, comment, diff)
                VALUES ((SELECT id FROM showcsd_wiki WHERE name = ?),
                ?,?,?,?,?,?,?)''',
                tuple(toaddpages))
            local_conn.commit()


    print('sdentry done:', time.time()-start_time, 'seconds')
    start_time = time.time()


if args.admin:
    print('admin done:', time.time()-start_time, 'seconds')
    start_time = time.time()
