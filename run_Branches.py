from concurrent import futures
import time
import json
import multiprocessing

import grpc
import example_pb2
import example_pb2_grpc

from Branch import Branch

with open('./input.json') as f:
  input = json.load(f) 

# parsing the input.json file for info about th branches
branches = []
for i in input:
  if i['type'] == 'branch':
    branches.append(i['id'])

# define a function to create an instance of the class Branch
def branch_process(idd, balance, branches):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    example_pb2_grpc.add_RPCServicer_to_server(Branch(idd,balance,branches), server)
    server.add_insecure_port('0.0.0.0:5000'+ str(idd))   # ID of the branch used to distinguish the port
    server.start()
    print('Branch '+str(idd) +' server started. Listening at 0.0.0.0:5000'+ str(idd) +'.')
    while True:
      time.sleep(5)


# using multiprocessing to create different process for each instance of th class Branch
if __name__ == '__main__':
  for i in input:
    if i['type'] == 'branch':
      multiprocessing.Process(target=branch_process, args=(i['id'],i['balance'],branches)).start()
