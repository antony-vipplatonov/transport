from confluent_kafka import Consumer
import time
import json
import copy
import requests
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'latest'}

consumer = Consumer(conf)
local_ip = '192.168.43.230'
consumer.subscribe(['segments_test2'])
q = dict()
while 1:
    msg = consumer.poll(2)
    if msg is None:
        print(0, end = ' ')
        copy_q = copy.deepcopy(q)
        for i in q:
            result = {'sender_name':q[i][1]['sender_name'],'send_time':q[i][1]['send_time'],'message':''}
            
            if q[i][1]['quantity_of_segments'] == len(q[i])-1:  # not q[i][0] and
                mess = [0]*q[i][1]['quantity_of_segments']
                for u in q[i][1:]:
                    mess[int(u['number_of_this_segment'])] = u['segment']
                if 0 in mess:
                    print('ERROR ERROR ERROR ERROR ERROR ERROR ')
                    resp = requests.post(f'http://{local_ip}:8004/receive',json=json.loads(f'{{"error":true, "send_time":"{q[i][1]["send_time"]}"}}'))
                else:
                    result['message']=''.join(mess)
                    print(result)
                    resp = requests.post(f'http://{local_ip}:8004/receive',json=json.result)
            else:
                q[i][0] += 1
                print('ERROR ERROR ERROR ERROR ERROR ERROR ')
                resp = requests.post(f'http://{local_ip}:8004/receive',json=json.loads(f'{{"error":true, "send_time":"{q[i][1]["send_time"]}"}}'))
            del copy_q[i]
        q = copy.deepcopy(copy_q)
                
                        
            
        
    else:
        segment = json.loads(msg.value().decode('utf-8'))
        if segment['send_time'] not in q:
            q[segment['send_time']] = [0]
        else:
            q[segment['send_time']][0] = 0
        q[segment['send_time']].append(segment)
        print(1, end = ' ')


        copy_q = copy.deepcopy(q)
        for i in q:
            result = {'sender_name':q[i][1]['sender_name'],'send_time':q[i][1]['send_time'],'message':''}
            
            if q[i][1]['quantity_of_segments'] == len(q[i])-1:
                mess = [0]*q[i][1]['quantity_of_segments']
                for u in q[i][1:]:
                    mess[int(u['number_of_this_segment'])] = u['segment']
                if 0 in mess:
                    copy_q[i][0] += 1
                    if copy_q[i][0] == 9:
                        print('ERROR ERROR ERROR ERROR ERROR ERROR ')
                        resp = requests.post(f'http://{local_ip}:8004/receive',json=json.loads(f'{{"error":true, "send_time":"{q[i][1]["send_time"]}"}}'))
                        del copy_q[i]
                else:
                    result['message']=''.join(mess)
                    print(result)
                    resp = requests.post(f'http://{local_ip}:8004/receive',json=result)
                    del copy_q[i]
            elif q[i][1]['quantity_of_segments'] > len(q[i])-1:
                
                copy_q[i][0] += 1
                if copy_q[i][0] == 9:
                    print('ERROR ERROR ERROR ERROR ERROR ERROR ')
                    resp = requests.post(f'http://{local_ip}:8004/receive',json=json.loads(f'{{"error":true, "send_time":"{q[i][1]["send_time"]}"}}'))
                    del copy_q[i]
            else:
                print('ERROR ERROR ERROR ERROR ERROR ERROR ')
                resp = requests.post(f'http://{local_ip}:8004/receive',json=json.loads(f'{{"error":true, "send_time":"{q[i][1]["send_time"]}"}}'))
                del copy_q[i]
                
        q = copy.deepcopy(copy_q)
                

        
