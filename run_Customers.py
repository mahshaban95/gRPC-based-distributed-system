import json
import multiprocessing
from google.protobuf.json_format import MessageToJson
import time
import Customer


with open('./input.json') as f:
  input = json.load(f) 

# define a function to create an instance of the class Customer
def customer_process(i):
    customer = Customer.Customer(i['id'], i['events'])
    customer.createStub()
    out = customer.executeEvents()
    # parsing the branch respose
    recv_list = []
    for recv1 in out.recv:
        recv_list.append({'interface': recv1.interface, 'result': recv1.result, 'money': recv1.money})
    # storing the output in output.txt
    output = {'id': out.id, 'recv': recv_list}
    with open('output.txt', 'a') as outfile:
        outfile.write(str(output)+'\n')
    

# using multiprocessing to create different process for each instance of th class Customer
if __name__ == '__main__':
  # parsing the input.json file
  for i in input:
    if i['type'] == 'customer':
      print(str(i['id']) + '===' + str(i['events']))
      p = multiprocessing.Process(target=customer_process, args=(i,))
      p.start()
      # p.join()

  print('The results will be appended to the "output.text" file created in the same directory! ')








