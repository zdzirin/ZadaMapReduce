# Example usage of the ZadaMapReduce system
from rpc_master import MasterNode
import subprocess
from time import sleep

host = 'localhost'
port = 7000
nodes = []

for i in range(4):
    i += 1
    if i < 3:
        nodes.append(subprocess.Popen(["py","rpc_mapper.py",host,str(port + i)]))
    else:
        nodes.append(subprocess.Popen(["py", "rpc_reducer.py", host, str(port + i)]))
master = MasterNode()

print('Master node created!')

master.register_mappers(['http://localhost:7001', 'http://localhost:7002'])
master.register_reducers(['http://localhost:7003', 'http://localhost:7004'])


def word_count_map_fn(a_list):
    result = {}
    a_list = a_list[1:]
    for word in a_list:
        if word in result:
            result[word] += 1
        else:
            result[word] = 1
    return result

def word_count_red_fn(data):
    reduced = {}
    for item in data:
        item = item.split(", ")
        if item[0] not in reduced.keys():
            reduced[item[0]] = int(item[1])
        else: 
            reduced[item[0]] += int(item[1])
    return reduced

master.run_mapred('EdgarAllanPoeVol1.txt', word_count_map_fn, word_count_red_fn, "output.txt")




sleep(120) # two minutes then shutsdown
master.shutdown_stack()
for node in nodes:
    node.terminate()
print('end of script')
