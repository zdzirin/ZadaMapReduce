# Example usage of the ZadaMapReduce system
from rpc_master import MasterNode
from time import sleep
import subprocess, re


host = 'localhost'
port = 7000
nodes = []

for i in range(4):
    i += 1
    if i < 3:
        nodes.append(subprocess.Popen(
            ["py", "rpc_mapper.py", host, str(port + i)]))
    else:
        nodes.append(subprocess.Popen(
            ["py", "rpc_reducer.py", host, str(port + i)]))
master = MasterNode()

print('Master node created!')

master.register_mappers(['http://localhost:7001', 'http://localhost:7002'])
master.register_reducers(['http://localhost:7003', 'http://localhost:7004'])


def inverted_index_map_fn(a_list):
    result = {}
    index = int(a_list.pop(0))
    for word in a_list:
        if word in result:
            result[word].append(index)
        else:
            result[word] = [index]
        index += len(word)

    return result


def inverted_index_red_fn(data):
    reduced = {}
    for item in data:
        item = item.split(", ", 1)
        key = item[0]
        values = re.sub(r"[^0-9 ]", '', item[1]).split(" ")
        print("%s: %s" % (key, values))

        if key not in reduced.keys():
            reduced[key] = values
        else:
            reduced[key].extend(values)
    return reduced


master.run_mapred('EdgarAllanPoeVol1.txt', inverted_index_map_fn, inverted_index_red_fn, "output2.txt")


sleep(120)  # two minutes then shutsdown
master.shutdown_stack()
for node in nodes:
    node.terminate()
print('end of script')
