# hash-armonica
Hash Armonica is a simple distributed hash table based on the chord protocol.

CSE 40771 Distributed Systems FA21 final project.

Authors: Jason Brown & Graham Fisher

### Running:
To launch a node in the HashArmonica system for the sake of having a live node, execute the following, making changes to the system parameters via config.json if desired:
```
conda activate distsys
conda install sortedcontainers
vim config.json
python3 -c 'from HashArmonica import HashArmonica; import json; ha = HashArmonica(**json.loads(open('config.json').read()))'
```

To launch a node for the sake of interacting with the system, we recommend using the good ole' Python interpreter, as follows.
```
conda activate distsys
conda install sortedcontainers
python3
>>> from HashArmonica import HashArmonica
>>> ha = HashArmonica(
  cluster_name=<identifying name to share across participants in system>
  force_nodeid=<force a particular nodeid instead of using the hashed IP:port. Good for testing>
  bitwidth=<system bitwidth: higher number have more overhead, but less chance for collisions>
  replication_factor=<number of nodes on which a copy of any given data item should be stored>
  stabilizer_timeout=<time between stabilizer invocations>
  fixer_timeout=<time between fixer/poker invocations>
  wait_for_stable_timeout=<when system state does not allow further work, how long until we try again?>
  failure_timeout=<how long should a node in the system wait after sending another a message before it assumes the other has crashed>
  verbose=<enable LOTS of debugging output?>
)
>>> ha.insert(<key>, <value>)
>>> ha.lookup(<key>)
>>> ha.delete(<key>)
>>> print(ha)
 <node representation as a string, displaying system and node information, as well as data items stored on this node>
```


Apologies for the messy code, dead code, outdated comments and old TODOs, etc.  
We focused instead on developing evaluation metrics and scenario, and then in gathering and analysing the data, and in sharing insights from our project in our final report.  
