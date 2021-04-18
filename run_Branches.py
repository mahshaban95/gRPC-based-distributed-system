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

output2 = []  # for creating the output file for project 2
for i in input:
  if i['type'] == 'branch':
    branches.append(i['id'])
    output2.append({"pid": i['id'], "data": []})        # pid number for all branches along with an empty list fot their data was created 


# appending the event id after pid
for i in input:
  if i['type'] == 'customer':
    for j in i['events']:
      if j['interface'] in {'deposit', 'withdraw'}:
        output2.append({"eventid": j['id'], "data": []})

# storing the output2 list into the json file that will be updated later in each branch 
with open('output2.json', 'w') as outfile:
    json.dump(output2, outfile)

# define a function to create an instance of the class Branch
def branch_process(idd, balance, branches):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    example_pb2_grpc.add_RPCServicer_to_server(Branch(idd,balance,branches), server)
    server.add_insecure_port('0.0.0.0:5000'+ str(idd))   # ID of the branch used to distinguish the port
    server.start()
    print('Branch '+str(idd) +' server started. Listening at 0.0.0.0:5000'+ str(idd) +'.')
    while True:
      time.sleep(5)


# using multiprocessing to create different process for each instance of th class Branch
# if __name__ == '__main__':
#   for i in input:
#     if i['type'] == 'branch':
#       multiprocessing.Process(target=branch_process, args=(i['id'],i['balance'],branches)).start()
