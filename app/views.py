from flask import render_template
from app import app

@app.route('/')
def main():
    return(render_template('home.html'))
@app.route('/home.html')
def home():
    return(render_template('home.html'))

@app.route('/about.html')
def about():
    return(render_template('about3.html'))
@app.route('/Avant-Garde.html')
def avant():
    return(render_template('Avant-Garde.html'))
@app.route('/Minimal.html')
def minimal():
    return(render_template('Minimal.html'))
@app.route('/Orchestral.html')
def orchestral():
    return(render_template('Orchestral.html'))
@app.route('/Romantic.html')
def romantic():
    return(render_template('Romantic.html'))
@app.route('/Classical+Period.html')
def classical():
    return(render_template('Classical+Period.html'))
@app.route('/Early+Music.html')
def earlymusic():
    return(render_template('Early+Music.html'))
@app.route('/Chant.html')
def Chant():
    return(render_template('Chant.html'))
@app.route('/Opera.html')
def Opera():
    return(render_template('Opera.html'))
@app.route('/Baroque.html')
def baroque():
    return(render_template('Baroque.html'))
@app.route('/Renaissance.html')
def ren():
    return(render_template('Renaissance.html'))
