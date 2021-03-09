from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from threading import Thread
import sys, re, marshal, types


def string_to_word_list(string):
    words = string.lower()
    words = re.sub(r"[^a-zA-Z0-9 '’]", ' ', words)
    word_list = words.split(' ')
    return [x for x in word_list if x != '']

class Mapper(object):
    def __init__(self, host, port):
        self.server = SimpleXMLRPCServer((host, port), allow_none=True)
        self.host = host
        self.port = port
        self.data = None
        self.red_proxies = []
        self.mapped_data = None
        self.index = 0
        print('Initialized Mapper')

    def set_reducers(self, reducers):
        print('setting reducers...')
        if type(reducers) == str:
            red_proxy = ServerProxy(reducers)
            self.red_proxies.append(red_proxy)
        elif type(reducers) == list:
            for reducer in reducers:
                red_proxy = ServerProxy(reducer)
                self.red_proxies.append(ServerProxy(reducer))
        print('...reducers set!')

    def remote_shutdown(self):
        print('Shutting down...')
        Thread(target=self.server.shutdown)
        print('Shutdown')
        return 1

    def get_data(self, data):
        print('data recieved...')
        self.data = string_to_word_list(data)
        print('...data set!')

    def map_data(self, map_fn):
        code = marshal.loads(map_fn.data)
        func = types.FunctionType(code, globals(), "mapper_name")
        print('mapping data...')
        self.mapped_data = func(self.data)
        print('...mapping finished!')
        self.shuffle_data()

    def hash_fn(self, key):
        print("hashing key %s" % key)
        return len(key) % len(self.red_proxies)

    def shuffle_data(self):
        print("shuffling data...")
        shuffle = {}
        for key in self.mapped_data.keys():
            # Maps each key to what server it will go to so 
            # we only have to make a call to each server getting data one time
            i = self.hash_fn(key)
            if i not in shuffle.keys():
                shuffle[i] = []
            shuffle[i].append("%s, %s" % (key,self.mapped_data[key]))
        print('...mapped data for sending...')
        for i in shuffle.keys():
            self.red_proxies[i].add_data(shuffle[i])
            self.red_proxies[i].mapper_finished()

        # let any reducers no data was sent to know this mapper is done
        left_to_update = [i for i in range(len(self.red_proxies)) if i not in shuffle.keys()]
        for i in left_to_update:
            self.red_proxies[i].mapper_finished()
        print("...shuffling finished")

        

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Incorrect arguments please deliver as py rpc_mapper.py host host_port')
    else:
        mapper = Mapper(sys.argv[1], int(sys.argv[2]))
        mapper.server.register_function(mapper.remote_shutdown)
        mapper.server.register_function(mapper.set_reducers)
        mapper.server.register_function(mapper.get_data)
        mapper.server.register_function(mapper.map_data)

        try:
            mapper.server.serve_forever()
        except KeyboardInterrupt:
            print('Closing Out')
