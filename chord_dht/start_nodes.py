import subprocess
from subprocess import Popen

commands = ["python dht_service.py node_3000 3000",
            "python dht_service.py node_3001 3001",
            "python dht_service.py node_3002 3002",
            "python dht_service.py node_3003 3003",
            "python dht_service.py node_3004 3004",
            "python dht_service.py node_3005 3005",
            "python dht_service.py node_3006 3006",
            "python dht_service.py node_3007 3007",
            "python dht_service.py node_3008 3008",
            "python dht_service.py node_3009 3009"
            ]

procs = [Popen(i, shell=True) for i in commands]
for p in procs:
    p.wait()
