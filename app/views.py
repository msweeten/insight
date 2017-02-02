from flask import render_template
from app import app
import psycopg2
import pandas as pd
import json

app.config.from_pyfile('/home/msweeten/insight/Config.py')

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
@app.route('/Avant-Garde')
def avant():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Avant-Garde' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Avant-Garde.html', subgenre = subgenre)
@app.route('/Minimal')
def minimal():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Minimal' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Minimal.html', subgenre = subgenre)
@app.route('/Orchestral')
def orchestral():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Orchestral' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Orchestral.html', subgenre = subgenre)
@app.route('/Romantic')
def romantic():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Romantic' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Romantic.html', subgenre = subgenre)
@app.route('/Classical+Period')
def classical():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Classical Period' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Classical+Period.html', subgenre = subgenre)
@app.route('/Early+Music')
def earlymusic():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Early Music' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Early+Music.html', subgenre = subgenre)
@app.route('/Opera')
def Opera():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Opera' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Opera.html', subgenre = subgenre)
@app.route('/Baroque')
def baroque():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Baroque' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Baroque.html', subgenre = subgenre)
@app.route('/Renaissance')
def ren():
    con = connect()
    query = """SELECT "Uri" FROM node_communities WHERE "NewGenre"='Renaissance' ORDER BY "popularity" LIMIT 20;"""
    query_results = pd.read_sql_query(query, con)
    subgenre = []
    query_results_v = list(query_results['Uri'])

    print query_results_v
    for i in range(len(query_results_v)):
        print query_results_v[i]
        dict_song = mydict([['widget', base + query_results_v[i]], ['results_uri', '/results/' + query_results_v[i]]])
        subgenre.append(dict_song)
    return render_template('Renaissance.html', subgenre = subgenre)
@app.route('/results/<song_uri>')
def get_song_uri(song_uri):
    previous_song = base + song_uri.replace('/results', '')
    print previous_song
    previous_song = dict({'uri': previous_song})

    query = text("""SELECT "Community" FROM node_communities WHERE "Uri" = :ge""").bindparams(ge=(song_uri))
    return render_template('results.html', previous_song = previous_song)

