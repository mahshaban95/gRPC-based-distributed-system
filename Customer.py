import grpc
import example_pb2
import example_pb2_grpc
import time

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

    # TODO: students are expected to create the Customer stub
    def createStub(self):
        self.channel = grpc.insecure_channel('0.0.0.0:5000'+ str(self.id))
        self.stub = example_pb2_grpc.RPCStub(self.channel)

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        events = self.events
        # create the request for customers
        request = example_pb2.Request(
            id = self.id,
            type = "customer",
            events = events
        )
        try:
            # getting the reponse from the branch
            response = self.stub.MsgDelivery(request)
            self.recvMsg.append(response)
            return response
        except grpc.RpcError as err:
            print('execption at customer')
            print(err.details()) 
            print('{}, {}'.format(err.code().name, err.code().value)) 