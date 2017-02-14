import psycopg2
import pandas as pd
import re
from operator import itemgetter
from sqlalchemy import create_engine

def open_db():
    """Open Database connection
    """

    exec(open('/home/msweeten/insight/Config.py').read())
    con = None
    con = psycopg2.connect(database = DATABASE, user = DB_ID, host=DB_HOST, password=DB_PW)
    engine = create_engine('postgresql://%s:%s@localhost/%s'%(DB_ID,DB_PW,DATABASE))
    return con, engine

def update_all_nodes(self, con, iteration):
    """Creates a new node column setting all to 0
       Creates new nodes
    """
    query = 'SELECT * FROM classical_update;'
    song_data = pd.read_sql_query(query, con)
    done_rows = []
    node = 1
    node_label = [0]*len(song_data)
    song_data['node' + str(iteration)] = node_label
    song_data['index'] = list(range(0, len(song_data)))
    for row in range(0, len(song_data)):
        row_data = song_data.iloc[row]        
        if row_data['node' + str(iteration)] != 0:
            continue
        song_data.set_value(row, 'node' + str(iteration), node)
        composer_uri = row_data['artist_id']
        composer_uri = composer_uri.split(', ')[0]
        song_name = row_data['song_name']
        song_key = re.findall('[A-Z] Major|[A-Z] Minor', song_name)
        song_split = re.split('[?:.,]', song_name)
        song_end = song_split[len(song_split) - 1]
        if len(song_key) == 0:
            song_key = ''
        else:
            song_key = song_key[0]

        song_no = re.findall('No\. [0-9]{1,}', song_name)
        if len(song_no) == 0:
            song_no = ''
        else:
            song_no = song_no[0]
        opus = re.findall('Op\. [0-9]{1,}', song_name)
        if len(opus) == 0:
            opus = ''
        else:
            opus = opus[0].upper()

        artist_match = song_data[song_data['artist_id'].str.contains(composer_uri)]
        artist_match = artist_match[artist_match['node' + str(iteration)] == 0]
        for i in range(len(artist_match)):
            a = artist_match.iloc[i]
            match_song = a['song_name']
            index = a['index']
            match_key = re.findall('[A-Z] Major|[A-Z] Minor', match_song)
            if len(match_key) == 0:
                match_key = ''
            else:
                match_key = match_key[0]
            match_opus = re.findall('Op\. [0-9]{1,}|WoO [0-9]{1,}|WOO [0-9]{1,}|WWV [0-9]{1,}', match_song)
            if len(match_opus) == 0:
                match_opus = ''
            else:
                match_opus = match_opus[0].upper()
            match_split = re.split('[?:.,]', match_song)
            match_end = match_split[len(match_split)- 1]
            match_no = re.findall('No\. [0-9]{1,}', match_song)
            if len(match_no) == 0:
                match_no = ''
            else:
                match_no = match_no[0]
                
            if match_song in song_split or song_name in match_split:

                song_data.set_value(index, 'node' + str(iteration), node)
            elif any(s in match_song for s in song_split):
                """Match full song
                   Match almost full song
                """
                if match_song == song_name:
                    song_data.set_value(index, 'node' + str(iteration), node)
                else:
                    fuzzy_match = fuzz.partial_ratio(match_song, song_name)
                    if fuzzy_match > 65:
                        if song_key in match_key or match_key in song_key:
                            if opus == '' or match_opus == '':
                                if song_no == '' or match_no == '' or song_no == match_no:
                                    #\xfc = umlaut
                                    match_end = match_end.replace(u'\xfc', 'u')
                                    song_end = song_end.replace(u'\xfc', 'u')
                                    if match_end == song_end:
                                        song_data.set_value(index, 'node' + str(iteration), node)
                            elif opus == match_opus:
                                if song_no == '' or match_no == '' or song_no == match_no:
                                    match_end = match_end.replace(u'\xfc', 'u')
                                    song_end = song_end.replace(u'\xfc', 'u')
                                    if match_end == song_end:
                                        song_data.set_value(index, 'node' + str(iteration), node)
        node += 1

    nodes = list(song_data['node' + str(iteration)].values)
    song_uris = list(song_data['song_uri'].values)
    song_data = None
    
    #create new node variable
    #insert data into each index
    cur = con.cursor()
    new_column = """ALTER TABLE classical_updated ADD node{0} INTEGER""".format(str(iteration))
    cur.execute(new_column)
    for s in range(len(song_uris)):
        uri = song_uris[s]
        node = nodes[s]
        insert_node = """UPDATE classical_updated SET node={0} WHERE song_uri={1}""".format(node, uri)
        cur.execute(insert_node)
    con.commit()
    
class merge_datasets(object):
    def __init__(self, iteration):
        """Instantiates connection with PostgreSQL database
           Copies node data to new column
        """
        self.con, self.engine = open_db()
        self.iteration = iteration
        #copy existing node data to node+1
        #UPDATE table SET node0 = node1
        new_column = """ALTER TABLE classical_updated ADD node{0} INTEGER""".format(str(iteration))
        cur = self.con.cursor()
        cur.execute(new_column)
        copy_column = """UPDATE classical_updated SET node{0}=node{1}""".format(str(iteration), str(iteration - 1))
        cur.execute()
        self.con.commit()
        
    def match_songs(self):
        """Partial and Full matches identical songs in escrow
        """

        select_escrow = """SELECT song_uri FROM escrow;"""
        cur = self.con.cursor()
        cur.execute(select_escrow)
        song_uris = cur.fetchall()
        #alter regular node to node0 when making classical_update
        full_query = """SELECT song_name, artist_name, artist_id, node{0} FROM classical_update""".format(self.iteration)
        song_data = pd.read_sql_query(full_query, con)
        new_node = max(list(song_data['node' + str(self.iteration)].values)) + 1
        for s in song_uris:
            #grab artist_id data
            #grab song_names,node_number from classical_update where artist_id match
            #match to nodes or create new node (find max node number)
            song_query = """SELECT artist_id,song_name FROM escrow WHERE song_uri='{0}' """.format(s)
            cur.execute(song_query)
            song_metadata = cur.fetchone()
            composer_uri = song_metadata[0]
            composer_uri = composer_uri.split(', ')[0]
            song_name = song_metadata[1]
            song_key = re.findall('[A-Z] Major|[A-Z] Minor', song_name)
            song_split = re.split('[?:.,]', song_name)
            song_end = song_split[len(song_split) - 1]
            if len(song_key) == 0:
                song_key = ''
            else:
                song_key = song_key[0]
            song_no = re.findall('No\. [0-9]{1,}', song_name)
            if len(song_no) == 0:
                song_no = ''
            else:
                song_no = song_no[0]
            opus = re.findall('Op\. [0-9]{1,}', song_name)
            if len(opus) == 0:
                opus = ''
            else:
                opus = opus[0].upper()
            artist_match = song_data[song_data['artist_id'].str.contains(composer_uri)]
            matches = []#tuple
            fmatch = False
            for i in range(len(artist_match)):
                a = artist_match.iloc[i]
                node = a['node']
                match_song = a['song_name']
                match_key = re.findall('[A-Z] Major|[A-Z] Minor', match_song)
                if len(match_key) == 0:
                    match_key = ''
                else:
                    match_key = match_key[0]
                match_opus = re.findall('Op\. [0-9]{1,}|WoO [0-9]{1,}|WOO [0-9]{1,}|WWV [0-9]{1,}', match_song)
                if len(match_opus) == 0:
                    match_opus = ''
                else:
                    match_opus = match_opus[0].upper()
                match_split = re.split('[?:.,]', match_song)
                match_end = match_split[len(match_split)- 1]
                match_no = re.findall('No\. [0-9]{1,}', match_song)
                if len(match_no) == 0:
                    match_no = ''
                else:
                    match_no = match_no[0]

                if match_song in song_split or song_name in match_split:
                    fmatch = True
                    match.append((100, node))
                else:
                    fuzzy_match = fuzz.partial_ratio(match_song, song_name)
                    if fuzzy_match > 65:
                        if song_key in match_key or match_key in song_key:
                            if opus == '' or match_opus == '':
                                if song_no == '' or match_no == '' or song_no == match_no:
                                    match_end = match_end.replace(u'\xfc', 'u')
                                    song_end = song_end.replace(u'\xfc', 'u')
                                    if match_end == song_end:
                                        fmatch = True
                                        match.append((fuzzy_match, node))
                                        
                            elif opus == match_opus:
                                if song_no == '' or match_no == '' or song_no == match_no:
                                    #\xfc = umlaut
                                    match_end = match_end.replace(u'\xfc', 'u')
                                    song_end = song_end.replace(u'\xfc', 'u')
                                    if match_end == song_end:
                                        fmatch = True
                                        match.append((fuzzy_match, node))
            if not fmatch:
                match.append((0, newnode))
                newnode += 1
            node_match = max(match, key=itemgetter(0))[1]
            update_node_val = """UPDATE escrow SET node{0}={1} WHERE song_uri={2}""".format(self.iteration, node_match, s)
            cur.execute(update_node_val)
        self.con.commit()
            
            
    def update_network(self):
        """Takes new song nodes and updates 
           Weight Edges for existing connections
           Creates New Data for New Connections
           Finally moves data to classical_update
           Clears escrow table
        """
        #All New Songs be sure to match songs in escrow to each other
        cur = self.con.cursor()
        query = """SELECT album_uri, node{0} FROM escrow;""".format(str(self.iteration))
        cur.execute(query)
        new_songs = cur.fetchall()

        #Match max node in existing network
        max_node_query = """SELECT MAX(node{0}) FROM classical_updated""".format(str(self.iteration))
        cur.execute(max_node_query)
        max_node = cur.fetchone()

        #modify 
        network = []
        edge_weights = []
        for i in range(len(new_songs)):
            song = new_songs[i]
            #Match Old Network
            match_album = """SELECT node{0} FROM classical_updated WHERE album_uri={1}""".format(str(self.iteration), song[0])
            cur.execute(match_album)
            node_matches = cur.fetchall()

            edges_old = [tuple(song[1], n) for n in node_matches]
            #Match New Network
            match_album_new = """SELECT node{0} FROM escrow WHERE album_uri={1} AND index > {2}""".format(str(self.iteration), song[0], i)
            cur.execute(match_album_new)
            node_matches_new = cur.fetchall()
            edges_new = [tuple(song[1], n) for n in node_matches_new if n != song[1]]
            edges = edges_old + edges_new

            for e in edges:
                edge_list = list(e)
                if edge_list in network:
                    e_ind = network.index(edge_list)
                    edge_weights[e_ind] += 1
                elif [edge_list[1], edge_list[0]] in network:
                    e_ind = network.index([edge_list[1], edge_list[0]])
                    edge_weights[e_ind] += 1
                else:
                    network.append(edge_list)
                    edge_weights.append(1)

        #links (both edges) in network_update
        for edg in range(len(network)):
            e = network[edg]
            #links (both edges) in network_update
            edge_query = """SELECT "Weight" FROM network_update WHERE ("NodeA"={0} AND "NodeB"={1}) OR ("NodeA"={1} AND "NodeB"={0})""".format(e[0], e[1])
            cur.execute(edge_query)
            match = cur.fetchone()
            if match == None:
                weight = match[2] + e[2]
                update_edge = """UPDATE network_update SET Weight={2} WHERE ("NodeA"={0} AND "NodeB"={1}) OR ("NodeA"={1} AND "NodeB"={0})""".format(e[0], e[1], weight)
                cur.execute(update_edge)
            else:
                weight = e[2]
                update_edge = """INSERT INTO network_update ("NodeA", "NodeB", "Weight", iteration) VALUES ({0}, {1}, {2}, {3})""".format(e[0], e[1], weight, self.iteration)
                cur.execute(update_edge)
        con.commit()
            
        #update network
        #table: network_update
        #insert new links
        #CREATE TABLE network_broad AS TABLE network_update
        
    def move_escrow(self):
        """Moves escrow db to classical_update
           Clears escrow
        """
        cur = self.con.cursor()        
        get_escrow = """SELECT (song_name, song_uri, song_duration_ms, popularity, artists_name, artist_id, album_name, album_uri, genre, set_type) FROM escrow;"""
        cur.execute(get_escrow)
        new_songs = cur.fetchall()
        for n in new_songs:
            insert = """INSERT INTO classical_update (song_name, song_uri, song_duration_ms, popularity, artists_name, artist_id, album_name, album_uri, genre, set_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cur.execute(insert, n)
        clear_escrow = """TRUNCATE escrow;"""
        cur.execute(clear_escrow)
        self.con.commit()
        
    def create_network(self):
        """Takes all song nodes and creates new network
           Weight Edges for all connections
           Creates New Data for New Connections
        """
        net = pd.DataFrame(columns = ('Node A', 'Node B', 'Weight'))
        query = 'SELECT * FROM classical_updated;'
        dataset = pd.read_sql_query(query, con)
        albums = list(set(dataset['album_uri'].values))
        network = []
        edge_weights = []
        for a in albums:
            print(str(albums.index(a)) + ' out of ' + str(len(albums) - 1))
            #perhaps link all training data for each subgenre
            album_sub = dataset[dataset['album_uri'] == a]
            nodes = list(set(album_sub['node' + str(self.iteration)].values))
            nodes.sort()
            edges = list(itertools.combinations(nodes, 2))
            for e in edges:
                edge_list = list(e)
                if edge_list in network:
                    e_ind = network.index(edge_list)
                    edge_weights[e_ind] += 1
                elif [edge_list[1], edge_list[0]] in network:
                    e_ind = network.index([edge_list[1], edge_list[0]])
                    edge_weights[e_ind] += 1
                else:
                    network.append(edge_list)
                    edge_weights.append(1)
        print('Put in SQL')
        cur = self.con.cursor()
        iter_num = 0
        net = pd.DataFrame(network, columns = ('NodeA', 'NodeB'))
        net['Weights'] = edge_weights
        net.to_sql('network_update', self.engine, if_exists='replace', index = False)
        
    def update_net_metadata(con, engine):
        """Takes node data and assigns a label to training data
        """
        query = 'SELECT * FROM classical_update;'
        node_data = pd.read_sql_query(query, con)
        print('Node List')
        nodes = list(set(node_data['node' + str(self.iteration)]))
        node_set = []
        print('Starting...')
        for i in range(len(nodes)):
            n = nodes[i]
            print('Node ' + str(i) + ' of ' + str(len(nodes)) + ' nodes')
            subset = node_data[node_data['node' + str(self.iteration)] == n]
            if any(subset['set_type'] == 'training'):
                if len(subset) == 1:
                    training_vote = subset
                else:
                    training_vote = subset[subset['set_type'] == 'training']
                #could have multiple modes
                known = 1
            else:
                training_vote = subset
                known = 0
            if len(training_vote) == 1:
                mode = training_vote['genre'].values[0]
            else:
                mode = Counter(list(training_vote['genre'].values))
                mode = mode.most_common(1)
                mode = list(mode[0])[0]
            node_set.append([n, known, mode])
        nset = pd.DataFrame(node_set, columns = ('Node', 'Known', 'Genre'))
        nset.to_sql('node_updated', engine, if_exists='replace', index = False)
    
