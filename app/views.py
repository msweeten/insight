from flask import render_template
from app import app
import psycopg2
import pandas as pd
import json


app.config.from_pyfile('/home/ubuntu/insight/Config.py')

print(app.config['DB_HOST'])

def connect():
    db_connect = psycopg2.connect(
        database = app.config['DATABASE'],
        user = app.config['DB_ID'],
        host = app.config['DB_HOST'],
        password = app.config['DB_PW']
    )
    return db_connect

class mydict(dict):
    def __str__(self):
        return json.dumps(self)

base = "https://embed.spotify.com/?uri="
@app.route('/')
def main():
    return render_template('home.html')
@app.route('/home.html')
def home():
    return render_template('home.html')

@app.route('/about.html')
def about():
    return render_template('about.html')
    return render_template('about.html')
@app.route('/Avant-Garde')
def avant():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Avant-Garde' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Avant-Garde.html', subgenre = subgenre)
@app.route('/Minimal')
def minimal():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Minimal' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Minimal.html', subgenre = subgenre)
@app.route('/Orchestral')
def orchestral():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Orchestral' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri',  '/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Orchestral.html', subgenre = subgenre)
@app.route('/Romantic')
def romantic():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Romantic' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/'+ query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Romantic.html', subgenre = subgenre)
@app.route('/Classical+Period')
def classical():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Classical Period' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri',  '/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Classical+Period.html', subgenre = subgenre)
@app.route('/Early+Music')
def earlymusic():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Early Music' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri',  '/' +query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Early+Music.html', subgenre = subgenre)
@app.route('/Opera')
def Opera():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Opera' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri',  '/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Opera.html', subgenre = subgenre)
@app.route('/Baroque')
def baroque():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Baroque' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri',  '/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Baroque.html', subgenre = subgenre)
@app.route('/Renaissance')
def ren():
    con = connect()
    query = """SELECT "Uri" FROM node_communities_update WHERE "NewGenre"='Renaissance' ORDER BY "popularity" DESC LIMIT 150;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/'+ query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Renaissance.html', subgenre = subgenre)
@app.route('/<song_uri>')
def get_song_uri(song_uri):
    con = connect()
    #if
    cur = con.cursor()
    check = """SELECT "Uri" FROM node_communities_update;"""
    cur.execute(check)
    songs = cur.fetchall()
    songs = set(list(songs))
    s_check = (song_uri,)
    if s_check in songs:

        previous_song = base + song_uri.replace('/results', '')
        print previous_song
        previous_song = dict({'uri': previous_song})

        query = """SELECT "Community" FROM node_communities_update WHERE "Uri" = '{0}'""".format(song_uri)
        query_result = pd.read_sql_query(query, con)
        query_result = query_result.iloc[0]['Community']
        query_comm = """SELECT "Uri" FROM node_communities_update WHERE "Community" = {0} AND "Uri" != '{1}' ORDER BY "popularity" DESC LIMIT 150;""".format(query_result, song_uri)
        comm_results = pd.read_sql_query(query_comm, con)
        subgenre = []
        comm_results_v = list(comm_results['Uri'])

        for i in range(len(comm_results_v)):
            dict_song = mydict([['widget', base + comm_results_v[i]], ['results_uri',  '/' + comm_results_v[i]]])
            subgenre.append(dict_song)
        return render_template('results.html', previous_song = previous_song, subgenre = subgenre)
    else:
        return render_template('404.html')

@app.route('/slides')
def slides():
    return render_template('slides.html')

@app.route('/extended')
def extended():
    return render_template('extendedslides.html')
#@app.route('/<genre>/<int:page>/')

#@app.route('/<genre>/PreviousResults')
