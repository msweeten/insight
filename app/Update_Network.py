import Ident_nodes as idn
import pickle
import pandas as pd

class update_net(object):
    def __init__(self, iteration):
        """Open PostgreSQL connection
           Every 7 iterations, redo network matching
           Every 7 iterations instantiate order to
           Create new network from scratch
        """
        #temporary no case for iteration
        if iteration % 7 == 0 and iteration < 0:
            idn.update_nodes()
            self.net = 'New'
        else:
            self.net = 'Old'
        self.con, self.engine = idn.open_db()
        self.iteration = iteration
    def update_network(self):
        """Updates network from pickle if
           self.net == 'Old'
           Else create new network from scratch
           NEVER REPLACE Original pickle
           Place new network in pickle
        """
        cur = self.con.cursor()
        if self.net == 'New':
            query = "SELECT * FROM network_update;"
            db = pd.read_sql_query(query, self.con)
            nodes1 = list(db['NodeA'].values)
            nodes2 = list(db['NodeB'].values)
            nodes = list(set(nodes1 + nodes2))
            network = Graph()
            for n in nodes:
                print 'Node ' + str(n) + ' out of ' + str(len(nodes))
                network.add_vertex('node' + str(n))
            edges = []
            subset = db[['NodeA', 'NodeB']]
            for i in range(len(subset)):
                edge = list(subset.iloc[i].values)
                print 'Edge ' + str(i) + ' out of ' + str(len(subset) - 1)
                edges.append(('node' + str(edge[0]), 'node' + str(edge[1])))
                #network.add_edge('node' + str(edge[0]), 'node' + str(edge[1]))
            network.add_edges(edges)
            #pickle network
            with open('/home/ubuntu/insight/network_update.pickle', 'wb') as f:
                pickle.dump(network, f)
            

        elif self.net == 'Old':
            #open old pickle
            with open('/home/ubuntu/insight/network_update.pickle', 'rb') as f:
                network = pickle.load(f)
            
            previous_max_node = """SELECT DISTINCT T.Node AS prev_nodes FROM 
                                (SELECT "NodeA" AS Node FROM network_update 
                                 WHERE iteration!={0}
                                 UNION ALL
                                 SELECT "NodeB" AS Node FROM network_update
                                 WHERE iteration!={0})
                                 AS T;""".format(self.iteration)
            cur.execute(previous_max_node)
            previous_nodes = cur.fetchall()
            all_new_nodes_q = """SELECT DISTINCT T.Node AS nodes FROM
                              (SELECT "NodeA" AS Node FROM network_update
                               WHERE iteration={0}
                               UNION ALL
                               SELECT "NodeB" AS Node FROM network_update
                               WHERE iteration={0})
                               AS T;""".format(self.iteration)
            cur.execute(all_new_nodes_q)
            all_new_nodes = cur.fetchall()

            new_nodes = [n[0] for n in all_new_nodes if n not in previous_nodes]
            #wipe memory clean of previous nodes
            previous_nodes = None

            if len(new_nodes) > 0:
                for node in new_nodes:
                    network.add_vertex('node' + str(node))

            #Clear memory of new_nodes
            new_nodes = None
            
            new_edges = """SELECT "NodeA", "NodeB" from network_update WHERE iteration={0}""".format(self.iteration)
            cur.execute(new_edges)
            edges_new = cur.fetchall()
            edges = []
            for e in edges_new:
                edges.append(('node' + str(e[0]), 'node' + str(e[1])))
                        
            network.add_edges(edges)

            with open('/home/ubuntu/insight/network_update.pickle', 'wb') as f:
                pickle.dump(network, f)



            



                
            
            
            
            
            


