from concurrent import futures
import time
import copy
import sys
import json
import fcntl

import grpc
import example_pb2
import example_pb2_grpc

class Branch(example_pb2_grpc.RPCServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # iterate the processID of the branches

        # building a list of all other branches (for propagating requests)
        for st in branches:
            if st != self.id:
                self.stubList.append('0.0.0.0:5000'+ str(st))

        # set the clock to 0
        self.clock = 1

        # list of the output for project 2
        self.output2_data = {
                "pid": self.id,
                "data": []
            }

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self,request, context):
        # appending requests messages for debugging purpose
        self.recvMsg.append(request)
      
        type1 = request.type    # type of the requester (customer or branch)
        # checking if the request is coming from a customer
        if type1 == 'customer':
            # checking if the id of the customer and the branch are the same
            if request.id != self.id:
                response = example_pb2.Response(
                        id = self.id,
                        recv = [{'result':'failed! This is not your branch.'}]
                    )
                return response
            else:
                recv = []  #list in case of multiple "recv" message in the same response
                for event in request.events:
                    # checking if the customer's request is a deposit
                    if event.interface == 'deposit':
                        
                        self.clock = max(self.clock, request.remote_clock) + 1   #updating the clock (deposit_request) 

                        self.output2_data["data"].append({ "id": event.id, "name": "deposit_request", "clock":self.clock })
                        
                        self.balance = self.balance + event.money    #updating the balance

                        self.clock +=1                               #updating the clock (deposit_execute) 
  
                        self.output2_data["data"].append({ "id": event.id, "name": "deposit_execute", "clock":self.clock })

                        recv.append({'interface':'deposit','result':'success'})

                        # propagating the deposit request to all other branches
                        for address in self.stubList:
                            # sending request
                            channel = grpc.insecure_channel(address)
                            stub = example_pb2_grpc.RPCStub(channel)
                            request_prop = copy.deepcopy(request)
                            request_prop.type = 'branch'     # modifing the original requester type as branch 
                            request_prop.remote_clock = self.clock
                            try:
                                response_prob = stub.MsgDelivery(request_prop)
                                self.clock = max(self.clock, response_prob.remote_clock) + 1   #updating the clock (deposit_propagate_response) 

                                self.output2_data["data"].append({ "id": event.id, "name": "deposit_propagate_response", "clock":self.clock })

                            except grpc.RpcError as err:
                                print('execption at branch1')
                                print(err.details()) 
                                print('{}, {}'.format(err.code().name, err.code().value)) 

                        self.clock +=1                              #updating the clock (deposit_response) 

                        self.output2_data["data"].append({ "id": event.id, "name": "deposit_response", "clock":self.clock })                 

                    # checking if the customer's request is a withdraw
                    if event.interface == 'withdraw':

                        self.clock = max(self.clock, request.remote_clock) + 1   #updating the clock (withdraw_request) 

                        self.output2_data["data"].append({ "id": event.id, "name": "withdraw_request", "clock":self.clock })

                        # checking if balance is enough!
                        if event.money > self.balance:
                            response = example_pb2.Response(
                                    id = self.id,
                                    recv = [{'result':'failed! Not enough balance.'}]
                                )
                            return response

                        self.balance = self.balance - event.money   #updating the balance
                        
                        self.clock +=1                               #updating the clock (withdraw_execute) 

                        self.output2_data["data"].append({ "id": event.id, "name": "withdraw_execute", "clock":self.clock })

                        recv.append({'interface':'withdraw','result':'success'})

                        # propagating the withdraw request to all other branches
                        for address in self.stubList:
                            # sending request
                            channel = grpc.insecure_channel(address)
                            stub = example_pb2_grpc.RPCStub(channel)
                            request_prop = copy.deepcopy(request)
                            request_prop.type = 'branch'      # modifing the original requester type as branch
                            request_prop.remote_clock = self.clock
                            try:
                                response_prob = stub.MsgDelivery(request_prop)
                                self.clock = max(self.clock, response_prob.remote_clock) + 1   #updating the clock (withdraw_propagate_response) 

                                self.output2_data["data"].append({ "id": event.id, "name": "withdraw_propagate_response", "clock":self.clock })

                            except grpc.RpcError as err:
                                print('execption at branch1')
                                print(err.details()) 
                                print('{}, {}'.format(err.code().name, err.code().value)) 

                        self.clock +=1                              #updating the clock (withdraw_response) 

                        self.output2_data["data"].append({ "id": event.id, "name": "withdraw_response", "clock":self.clock })

                    # checking if the customer's request is a query
                    if event.interface == 'query':
                        time.sleep(3)   # sleeps for 3 seconds for propagation to occur
                        recv.append({'interface':'query','result':'success','money':self.balance})
        

        
                with open("output2_"+ str(self.id) +".json", "w") as jsonFile:
                    fcntl.flock(jsonFile, fcntl.LOCK_EX)
                    json.dump(self.output2_data, jsonFile)
                    fcntl.flock(jsonFile, fcntl.LOCK_UN)
                        
                # creating the respomse to the customer
                response = example_pb2.Response(
                            id = self.id,
                            recv = recv
                        )
                return response

        # checking if the request is coming from a branch
        elif type1 == 'branch':
            recv = []
            for event in request.events:
                if event.interface == 'deposit':

                    self.clock = max(self.clock, request.remote_clock) + 1  #updating the clock (deposit_propagate_request) 

                    self.output2_data["data"].append({ "id": event.id, "name": "deposit_propagate_request", "clock":self.clock })

                    self.balance = self.balance + event.money
                        
                    self.clock +=1                               #updating the clock (deposit_propagate_execute) 

                    self.output2_data["data"].append({ "id": event.id, "name": "deposit_propagate_execute", "clock":self.clock })

                    recv.append({'interface':'deposit','result':'success'})
        
                if event.interface == 'withdraw':

                    self.clock = max(self.clock, request.remote_clock) + 1  #updating the clock (withdraw_propagate_request) 

                    self.output2_data["data"].append({ "id": event.id, "name": "withdraw_propagate_request", "clock":self.clock })

                    self.balance = self.balance - event.money

                    self.clock +=1                               #updating the clock (withdraw_propagate_execute) 

                    self.output2_data["data"].append({ "id": event.id, "name": "withdraw_propagate_execute", "clock":self.clock })

                    recv.append({'interface':'withdraw','result':'success'})



            with open("output2_"+ str(self.id) +".json", "w") as jsonFile:
                fcntl.flock(jsonFile, fcntl.LOCK_EX)
                json.dump(self.output2_data, jsonFile)
                fcntl.flock(jsonFile, fcntl.LOCK_UN)
                        
            # creating the response to the branch
            response = example_pb2.Response(
                        id = self.id,
                        recv = recv,
                        remote_clock = self.clock
                    )
            return response
        
