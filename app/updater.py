import Spotify_Update as su
import Ident_nodes as idn
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


#su
call = su.call_authorization(1000)
call.grab_data()


#idn
#iteration = 1 always start at 1

import Ident_nodes as idn
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

new_iteration = idn.merge_datasets(1)
#match_songs
new_iteration.match_songs()

#update network

new_iteration.update_network()
#move escrow and clear
new_iteration.move_escrow()
#update network metadata
new_iteration.update_net_metadata()



import Update_Network as un
#opens connection
up = un.update_net(1)
#create new network pickle
up.update_network()




import Update_Communities as uc

#initiate community detection
cd = uc.comm_detect(1)
cd.infomap()
cd.create_comm_db()

