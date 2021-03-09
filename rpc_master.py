from xmlrpc.client import ServerProxy
from multiprocessing import Pool
from threading import Thread
from time import sleep
from marshal import dumps
from datetime import datetime

class MasterNode(object):
    def __init__(self):
        self.map_proxies = []
        self.red_proxies = []
        
    def register_mappers(self, mappers):
        '''
        Creates and stores server proxies for given argument mappers. Takes either be a string specifying http://host:port or a list of these strings
        MUST REGISTER MAPPERS BEFORE REDUCERS 
        '''
        if type(mappers) == str:
            map_proxy = ServerProxy(mappers)
            self.map_proxies.append(map_proxy)
        elif type(mappers) == list:
            for mapper in mappers:
                map_proxy = ServerProxy(mapper)
                self.map_proxies.append(ServerProxy(mapper))

    def register_reducers(self, reducers):
        '''
        Creates and stores server proxies for given argument mappers. Takes either be a string specifying http://host:port or a list of these strings 
        MUST REGISTER MAPPERS BEFORE REDUCERS
        '''
        if type(reducers) == str:
            red_proxy = ServerProxy(reducers)
            self.red_proxies.append(red_proxy)
        elif type(reducers) == list:
            for reducer in reducers:
                red_proxy = ServerProxy(reducer)
                self.red_proxies.append(ServerProxy(reducer))
        
        for proxy in self.map_proxies:
            proxy.set_reducers(reducers)

    def shutdown_stack(self):
        '''
        Shuts down each mapper, and reducer's server 
        '''
        print('shutting down mappers...')
        for proxy in self.map_proxies:
            proxy.remote_shutdown()

        print('shutting down reducers...')
        for proxy in self.red_proxies:
            proxy.remote_shutdown()

        print('Shutdown!')

    def send_input_to_proxy(self, proxy, data):
        '''
        Sends input data (string) to the given ServerProxy
        '''
        proxy.get_data(data)

    def split_and_send_input(self, input_data):
        '''
        Splits the input data into chunks and sends each chunk to a mapper server
        '''
        words = ""
        if type(input_data) == str:
            f = open(input_data, "r", encoding="utf-8")
            words += f.read()
            f.close()
            
        elif type(input_data) == list:
            for item in list:
                f = open(item, "r", encoding="utf-8")
                words += " " + f.read()
                f.close()
        else:
            print('input data must be a string specifying file location or a list of such strings')

        chunk_size = len(words) // len(self.map_proxies)
        start = 0
        for i in range(1, len(self.map_proxies) + 1):
            if (i == len(self.map_proxies)):
                self.send_input_to_proxy(self.map_proxies[i - 1], str(start) + " " +  words[start:])
            else:
                end = chunk_size * i
                self.send_input_to_proxy(
                    self.map_proxies[i - 1], str(start) + " " + words[start:end])
                last = end

    def serialize_fn(self, fn):
        return dumps(fn.__code__)

    def call_map_fn(self, proxy, map_fn):
        proxy.map_data(map_fn)

    def run_mapred(self, input_data, map_fn, red_fn, output_location):
        '''
        Runs MapReduce function on the given input data using the given map and reduct functions and outputs to the given output location
        '''
        # Split and send the input to the mappers
        self.split_and_send_input(input_data)
        
        # Prepare the output file
        now = datetime.now()
        now_str = now.strftime("%m/%d/%Y %H:%M:%S")
        f = open(output_location, 'w')
        f.write('MapReduce Output %s\n' % now_str)
        f.close()
    
        # Prepare the reducers
        red_fn = dumps(red_fn.__code__)
        for proxy in self.red_proxies:
            proxy.set_red_fn(red_fn)
            proxy.set_n_mappers(len(self.map_proxies))
            proxy.set_output_location(output_location)

        map_fn = dumps(map_fn.__code__)
        for proxy in self.map_proxies:
            newthread = Thread(target=self.call_map_fn, args=(proxy,map_fn))
            newthread.start()
