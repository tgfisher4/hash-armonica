from ChordClient import ChordClient
import HashArmonicaUtils as utils
from RPCClient import RPCClient
import threading

from sortedcontainers import SortedList

class HashArmonica:
    def __init__(self, cluster_name=None, bitwidth=128, succlistlen=4, stabilizer_timeout=1, fixer_timeout=3, failure_timeout=10, verbose=False):
        
        # Store the data that we own/have replicas
        self.table = SortedDict({})
       
        # Pass in callback function to ChordPeer to call when alert is needed
        self.chord = ChordClient(...)

        # Spawn server thread to listen
        threading.Thread(target=lambda: utils.Server(self), daemon=True).start()

        
        # TODO: ChordClient returns nodeid, successor
        # succ is a tuple with ?
        self.nodeid, succ = self.chord.join()
        
        # TODO: Query successor with our nodeid to obtain the data that we now own
        # Create socket with successor
        kwargs = {
            'verbose' : True
            'timeout': 1MILLION
            'addr': succ_addr
        }
        self.rpc = RPCClient(**kwargs)
            
        new_data = self.rpc.push_keys(nodeid) # gonna need to send something to signify joining
        for datum in new_data:
            self.table[datum[0]] = datum[1]


    ''' CLIENT METHODS '''

    ''' Implements insertion into the hash table with chain replication
    Automatically set replication status to 'original' or 'replica'
    '''
    def insert(self, datum):
        pass

    ''' For scan we would need to either sort with the hash function or 
    store string keys in the value field of the SortedDict table
    '''
    
    ''' SERVER METHODS '''
    ''' Used when new node joins the system and is now responsible
    for some of the data items that its successor was holding on to
    '''
    def push_keys(self, joiner):
        # Also return the succ list
        return [k, self.table[k] for k in self.table.irange(self.chord.pred+1, joiner)]

    #TODO
    def pull_keys(self, new_pred):
        for k in self.table.irange(new_peer+1, self.chord.pred):
            self.insert((k, self.table[k]))


    def callback_fxn(self, new_pred):
        self.pull_keys(new_pred)
