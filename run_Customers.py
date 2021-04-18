import json
import multiprocessing
from google.protobuf.json_format import MessageToJson
import time
import Customer
import glob
from itertools import groupby

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
  p = {}
  for i in input:
    if i['type'] == 'customer':
      print(str(i['id']) + '===' + str(i['events']))
      p[str(i['id'])] = multiprocessing.Process(target=customer_process, args=(i,))
      p[str(i['id'])].start()
      # p.join()

  print('The results will be appended to the "output.text" file created in the same directory! ')
  process_info = list(p.values())
  print('Preparing the results for part 2 of the project!... ')
  while True:
    process_status = []
    for i in process_info:
      process_status.append(i.is_alive())
    if not any(process_status):
      print(process_status)
      break
  print('The results for project can be found in the "output2.json" file created in the same directory! ')
  result = []
  for f in glob.glob("output2_*.json"):
      with open(f, "rb") as infile:
          result.append(json.load(infile))

  with open("merged_file.json", "w") as outfile:
      json.dump(result, outfile)

  output2_data = []
  with open("merged_file.json", "rb") as infile:
      output2_data = json.load(infile)

  eventid_data = []
  for i in output2_data:
    for j in i['data']:
      eventid_data.append(j)

  eventid = []
  for i in input:
    if i['type'] == 'customer':
      for j in i['events']:
        if j['interface'] in {'deposit', 'withdraw'}:
          eventid.append({"eventid": j['id'], "data": []})  

  eventid_data.sort(key=lambda x:x.get('id'))
  for k,v in groupby(eventid_data,key=lambda x:x.get('id')):
    for i in eventid:
      if i['eventid'] == k:
        mm = list(v)
        mm.sort(key=lambda x:x.get('clock'))
        i['data'] = (mm)

  output2_data.sort(key=lambda x:x.get('pid'))
  output2_data.append(eventid)
  with open("output2.json", "w") as outfile:
    json.dump(output2_data, outfile)