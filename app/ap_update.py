import Spotify_Update as su    
import Ident_nodes as idn
import Update_Network as un
import Update_Communities as uc

import sys
import psycopg2

reload(sys)
sys.setdefaultencoding('utf-8')


def get_iteration():
    con,engine = idn.open_db()
    cur = con.cursor()
    query = """SELECT Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='classical_update'"""
    cur.execute(query)
    columns = cur.fetchall()

    node_names = []
    for c in columns:
        if 'node' in c[0]:
            node_names.append(c[0])
    max_node = max(node_names)
    iteration = int(max_node.replace('node', '')) + 1
    return iteration
    

def update():
    """Update Song Data and Network
    """
    call = su.call_authorization(5000)
    call.grab_data()

    iteration = get_iteration()
    new_iteration = idn.merge_datasets(iteration)
    #match_songs
    new_iteration.match_songs()
    #update network
    new_iteration.update_network()
    #move escrow and clear
    new_iteration.move_escrow()
    #update network metadata
    new_iteration.update_net_metadata()
    #opens connection
    up = un.update_net(iteration)
    #create new network pickle
    up.update_network()
    #initiate community detection
    cd = uc.comm_detect(iteration)
    cd.infomap()
    cd.create_comm_db()


