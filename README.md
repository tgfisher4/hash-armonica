# hash-armonica
Hash Armonica is a simple distributed hash table based on the chord protocol.

CSE 40771 Distributed Systems FA21 final project.

Authors: Jason Brown & Graham Fisher

### Running:
To launch a node in the HashArmonica system for the sake of having a live node, execute the following:
```
conda activate distsys
conda install sortedcontainers
python3 -c 'from HashArmonica import HashArmonica;
```
To launch a node for the sake of interacting with the system, we recommend using the good ole' Python interpreter as follow:
```
conda activate distsys
conda install sortedcontainers
python3
>>> from HashArmonica import HashArmonica
>>> ha = HashArmonica
```


Apologies for the messy code, dead code, outdated comments and old TODOs, etc.  
We focused instead on developing evaluation metrics and scenario, and then in gathering and analysing the data, and in sharing insights from our project in our final report.  
