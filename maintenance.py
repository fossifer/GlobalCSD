import time
import json
import requests

# generate enums.py
CSDCATSLINK = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=Q5964&props=sitelinks&utf8=1'
RMCATCMTLINK = 'https://translatewiki.net/w/api.php?action=query&format=json&meta=messagetranslations&utf8=1&mttitle=MediaWiki%3ARecentchanges-page-removed-from-category'
ADDCATCMTLINK = 'https://translatewiki.net/w/api.php?action=query&format=json&meta=messagetranslations&utf8=1&mttitle=MediaWiki%3ARecentchanges-page-added-to-category'
CSDCATS = None
RMCATCMT = None
ADDCATCMT = None

# get these enumerations
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
with open('./enums.py', 'w') as f:
    f.write('# AUTO GENERATED BY maintenance.py\n')
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
