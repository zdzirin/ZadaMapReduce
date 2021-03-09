Zachary Zirin's MapReduce Python implementation

FOR TESTING/GRADING:

       running `py inverted_index_example.py` and `py word_count_example.py` will launch the program for 
       word count and inverted index respectively, each program runs for 2 minutes and then exits. Wait the two minutes 
       or cancel with cntrl+c in the terminal/causing a keyboard interrupt.


Concept: To be able to call a distributed MapReduce function from within python

Execution:

        My implementation of the MapReduce algorithm comes in the form of a python module, 
        allowing users to run MapReduce by creating a python script to execute the algorithm

        Basic Steps To Execute MapReduce:
                1. Spawn as many mappers and reducers as you desire 
                        -> These are defined in rpc_mapper.py and rpc_reducer.py
                2. Create MasterNode python object in execution python script
                3. Register mappers and reducers with master in script
                4. Call master to run mapreduce by passing it input and output locations, and python function definitions for map and reduce in script
                5. Call shutdown function (delayed so the algorithm can run)

        This is the workflow/design that I decided on for my map reduce system for a couple reasons, here they are:

                1. I thought it would be novel/interesting to create a system that was controlled by an execution script the user ran instead of sending commands to a master server
                2. Providing the system as a python module makes it easy to understand and work with for anyone interested.
                3. Additionally this set up gives a lot of freedom/control over different aspects/configuration of the system and how it runs.
                4. Allows easier diagnosing of problems and testing that may arise in the system.
                5. Easily develop python map and reduce functions to use with the system and easily pass them in allowing great generalness to the algorithm.
        
        Enjoy!

Documentation:

        The MasterNode Class, 
                Your mapreduce controller

        Constructor:
        
                MasterNode() 
                        creates a new MasterNode object used for execution and thereof of the MapReduce algorithm

        Variables:



        Functions:
        
                .register_mappers(mappers) 
                        mappers is either a string specifying where to reach the mapper at in the following format: 'http://{host}:{port}'
                                or a list of such strings

                        creates a proxy object for each mapper provide and appends it to internal list self.map_proxies 
                        Must be called before the following function .register_reducers()
                
                .register_reducers(reducers) 
                        reducers is either a string specifying where to reach the mapper at in the following format: 'http://{host}:{port}'
                                or a list of such strings

                                
                        creates a proxy object for each reducer provided and appends it to internal list self.red_proxies
                        
                        Note! This function also passes reducers to each mapper for them to run this function as well creating references to each reducer within each mapper
                        Because of this! That means you MUST CALL .register_mappers BEFORE CALLING .register_reducers

                .run_mapred(input_location, map_fn, red_fn, output_location)
                        input_location, a string or list of strings specifying the .txt files to be used as input
                        map_fn, a python function which accepts a list of strings as an argument
                        red_fn, a python function which accepts a list of strings of the format "key, value"
                        output_location, a string specifying the file to output results to

                        This function runs the map reduce operation by doing the following steps, 
                        you can follow these as well to run map reduce without calling this function:

                                1. Splits and sends the input data
                                        -> uses the function split_and_send_input(input_location),
                                           combines input data into one long string and evenly divides that 
                                           string amongst mappeers
                                2. Prepares output file
                                        -> Creates file if not existent and overwrites if it is
                                        -> Writes 'MapReduce Output mm/dd/yyyy hh:mm:ss' at top of file
                                3. Sets the data for each reducer
                                        -> For each proxy in the reducers proxy list does the following:
                                                1. sets the reduce function
                                                        For help with this see the sending functions section below
                                                2. sets the number of mappers
                                                3. sets the output locations

                                4. For each mapper calls for them to execute the map function each in a new thread
                                        -> by putting each call in a new thread they are sent out without waiting
                                           for the job on the server you just sent to finish

                                Step four launches the process which is continued by the mappers. 
                                From here each mapper maps the data, divides it by which reducer it's going to,
                                and sends the appropriate data to each rpc_reducer.py and lets each reducer know that it's done
                                From here the process once every mapper is complete the reducers take over, reducing the data
                                and writing the results to the output file.

                .shutdown_stack():
                        shuts down each mapper and then each reducer

                .send_input_to_proxy(proxy, data):
                        sends data, a string
                        to proxy a mapper ServerProxy object

                .serialize_fn(fn):
                        Serializes a function for use as an argument in a call to the remote server

                .call_map_fn(proxy, fn):
                        calls fn, a function serialzed with .serialize_fn()
                        on proxy, a mapper ServerProxy object
        
Mappers and Reducers:
        create a mapper or a reducer in a dedicated terminal by using the following 
        command in a terminal with python installed:
        
        `py {rpc_mapper.py/rpc_reducer.py} {host} {port}` 
                                  
        -> The scripts expect to be called followed by the host and then the port number.
        -> You must spawn these reducers and mappers before they can be registered and used
           with the master node.                      
