import psycopg2
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class call_authorization(object):
    def __init__(self, num):
        """
           Creates Spotify API instance using Key
        """
        exec(open('/home/msweeten/insight/Config.py').read())
        client_credentials_manager = SpotifyClientCredentials(client_id = CLIENT_ID, client_secret = CLIENT_SECRET)
        sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
        self.sp = sp
        self.genre = 'classical'
        con = None
        con = psycopg2.connect(database = DATABASE, user = DB_ID, host=DB_HOST, password=DB_PW)
        self.con = con
        self.columns = ('song_name', 'song_uri', 'song_duration_ms', 'popularity', 'artists_name', 'artist_id', 'album_name', 'album_uri','genre', 'set_type')
        self.num_songs = num
        self.keywords = ['name', 'uri', 'duration_ms', 'popularity']
        self.artist_kw = ['name', 'uri']
        self.album_kw = ['name', 'uri']
        self.genres = ['Avant Garde', 'Baroque',
                  'Choral',
                  'Early Music', 'Classical Period',
                  'Minimal', 'Opera',
                  'Orchestral', 'Renaissance',
                  'Romantic']

    def grab_data(self):
        """Grabs between 1000 and 1050 new songs from Spotify Web API
        """
        cur = self.con.cursor()
        query_term = "genre:" + self.genre
        new_files = 0
        offset = 0
        while new_files < self.num_songs:
            query_results = self.sp.search(q = query_term, type = 'track', limit = 50, offset = offset)
            query_results = query_results['tracks']['items']

            for q in query_results:
                URI = q['uri']
                cur.execute("SELECT * FROM classical_update WHERE song_uri=(%s)", (URI,))
                match = cur.fetchall()
                if len(match) == 0:
                    data = []
                    for w in self.keywords:
                        data.append(q[w])
                    for aw in self.artist_kw:
                        artist_data = q['artists']
                        if len(aw) == 1:
                            data.append(artist_data[aw])
                        if len(aw) > 1:
                            art_dat = []
                            for art in artist_data:
                                art_dat.append(art[aw])
                            artists_values = ', '.join(art_dat)
                            data.append(artists_values)
                    for alw in self.album_kw:
                        album_data = q['album']
                        data.append(album_data[alw])

                    album_name = q['album']['name']
                    prelabeled = [g for g in self.genres if g in album_name]
                    if len(prelabeled) > 0:
                        genre = prelabeled[0]
                        set_type = 'training'
                    else:
                        genre = ""
                        set_type = 'test'
                    data.append(genre)
                    data.append(set_type)
                    data.append(new_files)
                    data = tuple(data)
                    cur.execute("INSERT INTO escrow (song_name, song_uri, song_duration_ms, popularity, artists_name, artist_id, album_name, album_uri,genre, set_type, index) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data)
                    new_files += 1                

            offset += 50
            
        self.con.commit()
        cur.close()
        self.con.close()
        #add node ref
        #set to 0
        

