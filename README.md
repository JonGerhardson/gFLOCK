# gFLOCK
scrape flock transparency portals, organize scrapes into a database, de-identify UUIDs 

Quick start

```
git clone https://github.com/JonGerhardson/gFLOCK
```
```
cd gFlock
```
```
python FLOCK-audit.py
```
It is optimal to run this once every 30 days, as that is the data retention period for these files. 

This will use the urls.csv in this repo to begin scraping Flock transparency portals. It will save what it finds in a folder named scraped_data organized by State > Agency > Date of scrape. There will be a file named page_content.html for each agency, and, if you're lucky they have also published a file named search_audit.csv. The file name conventions are identical for every agency, but we're going to create a sql database from what we've gathered, so that won't matter so much. 

Once you've finished running FLOCK-audit.py . . . 

```
python db.py
```
This will create a file named agency_data.db. You can use an app like DB Browser for SQLite to explore this data in a convienet way. 

One problem with the transcparency.flock audit logs however, is they replace the name of the person who performed the search for a license plate, and their orginization, with UUIDs. This is different than the full Network audit files youn can obtain by public records request. BUT, if you happen to have one of those more detailed logs (obtainable from your friendly local police department RAO, and there's some on muckrock too; we can re-identify this data using a database join. 

To do this import your longform network audit csv to your database, and then export it (sounds stupid but apparently nessecary). Then also export the table search_audit that was created when setting up the .db. 

In the same directory as all of this run
```
python join.py
```
You'll be given a new file called uuid_name_mapping.csv, and it will link the UUID to the name of the officer and agency that is absent from the public portal logs. For convienence I have included this file already prepared in this repo. 

Based on the Network Audit files I had access to when doing this, I've been able to re-identify 312 unique Flock users across 116 orginizations. Yet the files I used to do this contain rows for over 800,000 license plate searches from May to July. sum of This is likely limited by a couple factors: 1. not every agency (about 600 I'm aware of) with a transparency portal publishes an audit log. 2. It only works for agencies that are in the same share group as the full texy network audit file I joined it with. And of course, my own incompetence is a highly likely determinante too. If you think you can help please reach out. 



