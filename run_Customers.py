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

  print('The results (for Project 1) will be appended to the "output.text" file created in the same directory! ')
  process_info = list(p.values())
  print('Preparing the results for Project 2!... ')
  # Waiting until all customer processes are finished
  while True:
    process_status = []
    for i in process_info:
      process_status.append(i.is_alive())
    if not any(process_status):
      break
  print('The results for project can be found in the "output2.json" file created in the same directory! ')
  # merging all data from different branches to a single file
  result = []
  for f in glob.glob("output2_*.json"):
      with open(f, "rb") as infile:
          result.append(json.load(infile))

  with open("merged_file.json", "w") as outfile:
      json.dump(result, outfile)

  # reading data from the merged file to get events data
  output2_data = []
  with open("merged_file.json", "rb") as infile:
      output2_data = json.load(infile)

  # getting all events data in a single list
  eventid_data = []
  for i in output2_data:
    for j in i['data']:
      eventid_data.append(j)

  # building the main structure for the eventid to which the events will be appended
  eventid = []
  for i in input:
    if i['type'] == 'customer':
      for j in i['events']:
        if j['interface'] in {'deposit', 'withdraw'}:
          eventid.append({"eventid": j['id'], "data": []})  

  # sorting and grouping the events based on the id
  eventid_data.sort(key=lambda x:x.get('id'))
  for k,v in groupby(eventid_data,key=lambda x:x.get('id')):
    # appending the events data to the eventid main object
    for i in eventid:
      if i['eventid'] == k:
        mm = list(v)
        mm.sort(key=lambda x:x.get('clock'))
        i['data'] = (mm)

  output2_data.sort(key=lambda x:x.get('pid')) # sorting the branches data based on the pid
  # appending the events grouped data to the main output array
  output2_data.append(eventid)
  # creating the output file
  with open("output2.json", "w") as outfile:
    json.dump(output2_data, outfile)