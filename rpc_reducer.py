from xmlrpc.server import SimpleXMLRPCServer
from threading import Thread
import sys, re, marshal, types

class Reducer(object):
    def __init__(self, host, port):
        self.server = SimpleXMLRPCServer((host, port), allow_none=True)
        self.host = host
        self.port = port
        self.n_mappers = None
        self.output_location = None
        self.data = []
        self.red_fn = None
        self.reduced_data = None
        self.mappers_finished = 0
        print('Initialized Reducer')

    def set_red_fn(self, red_fn):
        code = marshal.loads(red_fn.data)
        self.red_fn = types.FunctionType(code, globals(), "reducer_fn")

    def set_output_location(self, ol):
        self.output_location = ol
    
    def set_n_mappers(self, n):
        self.n_mappers = n

    def set_object_data(self, n_mappers=None, output_location=None, red_fn=None, master=None):
        if n_mappers:
            self.set_n_mappers(n_mappers)

        if output_location:
            self.set_output_location(output_location) 
        
        if red_fn:
            self.set_red_fn(red_fn)

    def remote_shutdown(self):
        print('Shutting down...')
        Thread(target=self.server.shutdown)
        print('Shutdown')
        return 1

    def write_reduced_data(self):
        print('writing reduced data...')
        write_str = ""
        for key in self.reduced_data:
            write_str += "%s: %s\n" % (key, self.reduced_data[key])
        f = open(self.output_location, 'a')
        f.write(write_str)
        f.close()
        print('...reduced data written!')

    def run_reduce_fn(self):
        print('reducing data...')
        self.reduced_data = self.red_fn(self.data)
        print('...data reduced!')
        self.write_reduced_data()

    def add_data(self, data):
        print('Adding data')
        self.data.extend(data)

    def mapper_finished(self):
        self.mappers_finished += 1
        print('A mapper finished, now %s/%s mappers have finished!' % (self.mappers_finished, self.n_mappers))
        if self.mappers_finished == self.n_mappers:
            self.run_reduce_fn()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Incorrect arguments please deliver as py rpc_mapper.py host host_port')
    else:
        reducer = Reducer(sys.argv[1], int(sys.argv[2]))
        reducer.server.register_function(reducer.remote_shutdown)
        reducer.server.register_function(reducer.set_n_mappers)
        reducer.server.register_function(reducer.set_red_fn)
        reducer.server.register_function(reducer.set_output_location)
        reducer.server.register_function(reducer.add_data)
        reducer.server.register_function(reducer.mapper_finished)
        
        try:
            reducer.server.serve_forever()
        except KeyboardInterrupt:
            print('Closing Out')
