import json
import logging
import math
import time
from django.views.decorators.csrf import csrf_exempt
import random
import requests
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework import status
from transport import settings
from kafka import KafkaProducer
from kafka import KafkaConsumer


fmt = getattr(settings, 'LOG_FORMAT', None)
lvl = getattr(settings, 'LOG_LEVEL', logging.DEBUG)
local_ip = '192.168.1.104'
logging.basicConfig(format=fmt, level=lvl)
logging.debug("Logging started on %s for %s" % (logging.root.name, logging.getLevelName(lvl)))

class ResponseThen(Response):
    def __init__(self,then_callback,request, **kwargs):
        super().__init__(**kwargs)
        self.then_callback = then_callback
        self.request = request
    def close(self):
        super().close()
        #logging.debug(self.request)
        self.then_callback(self.request)

@csrf_exempt
@api_view(['Post'])
def send(request):
    def after_return(request):
        quantity_of_segments = math.ceil(len(request['message'])/50)
        for i in range(0, len(request['message']),50):
            logging.debug(request['message'][i:i+50])
            segment = {'segment':request['message'][i:i+50],'sender_name':request['sender_name'],'send_time':request['send_time'], 'quantity_of_segments':quantity_of_segments, 'number_of_this_segment':i/50}
            resp = requests.post(f'http://{local_ip}:8000/code',json=segment)
            if resp.status_code!=200:
                logging.debug('что-то пошло не так')
                break


    logging.debug(request)
    return ResponseThen(after_return,request.data, status=status.HTTP_200_OK)   


producer = KafkaProducer(bootstrap_servers='localhost:9092')
@csrf_exempt
@api_view(['Post'])
def transfer(request):
    def after_return(request):
        serialized_data = json.dumps(request)
        producer.send('segments_test2',key = bytes(request['send_time'],encoding='utf-8'), value=bytes(serialized_data,encoding='utf-8'))



    logging.debug(request)
    return ResponseThen(after_return,request.data, status=status.HTTP_200_OK)  