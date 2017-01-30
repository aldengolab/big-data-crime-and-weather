#!/usr/bin/env python
'''
This code is a CGI interface with crime-reports.html. 
Please run requirements.txt on your web server first. 
'''
import cgi, cgitb
from kafka import KafkaProducer
import time

def message_to_user(message):
    '''
    An message_to_user message for the user.
    '''
    print 'Content-type: text/html\n\n'
    print '<html><head> <title>HBase Output</title> </head><body>'
    print '''<div align="center" style="padding:10px;color:white;font-size:16px;
    font-family:arial;font-weight:bold;border: 1px solid white;
    margin-right:25%;margin-left:25%;margin-top:10px;margin-bottom:20px;
    background-color:gray;">&nbsp;{}&nbsp;</div>'''.format(message)
    print '</body></html>'

def check_inputs(inputs):
    '''
    Makes sure all entries are full. 
    '''
    for i in range(len(inputs)): 
        if inputs[i] == '':
            return False
    if inputs[0] > 12 or inputs[0] < 0: 
        return False
    if inputs[1] > 31 or inputs[1] < 0:
        return False
    if inputs[2] < 2001 or inputs[2] > 2016:
        return False
    if inputs[3] > 23 or inputs[3] < 0:
        return False

    return True

def main(inputs, count=0):
    '''
    Adds report to kafka.
    '''
    clear = 1
    for i in range(7, len(inputs)):
        if inputs[i] == 1:
            clear = 0
    inputs.append(clear)

    for i in range(len(inputs)):
        inputs[i] = str(inputs[i])

    producer = KafkaProducer(bootstrap_servers='hdp-m.c.mpcs53013-2016.internal:6667')
    message = ','.join(inputs)
    message = bytes(message)
    producer.send('agolab', message)
    producer.close()


form = cgi.FieldStorage()
month = int(form.getfirst('month', ''))
day = int(form.getfirst('day', ''))
year = int(form.getfirst('year', ''))
hour = int(form.getfirst('hour', ''))
crime = form.getfirst('crimeType', '')
neighborhood = form.getfirst('community', '')
temperature = float(form.getfirst('temperature', ''))
fog = int(form.getfirst('fog', '0'))
rain = int(form.getfirst('rain', '0'))
snow = int(form.getfirst('snow', '0'))
hail = int(form.getfirst('hail', '0'))
thunder = int(form.getfirst('thunder', '0'))
tornado = int(form.getfirst('tornado', '0'))
inputs = [month, day, year, hour, crime, neighborhood, temperature, fog, rain, snow, hail, thunder, tornado]

if check_inputs(inputs):
    main(inputs)
    message_to_user('Successfully submitted.')
else: 
    message_to_user('Check inputs: {}'.format(inputs))



