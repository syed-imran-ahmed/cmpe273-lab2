
import logging
import json
import urllib
import heapq
from datetime import datetime as dt, time as t

from spyne import Application, srpc, ServiceBase, Iterable, UnsignedInteger,String

from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication


class HelloWorldService(ServiceBase):
    @srpc(String, String,String, _returns=Iterable(String))
    def checkcrime(lat, lon,radius):
        url='https://api.spotcrime.com/crimes.json?lat='+lat+'&lon='+lon+'&radius='+radius+'&key=.'
        print url
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        #initialize dictionary
        dict = {
                "total_crime":0,
                "the_most_dangerous_streets":[],
                "crime_type_count":{
                    "Assault":0,
                    "Arrest":0,
                    "Burglary":0,
                    "Robbery":0,
                    "Theft":0,
                    "Other":0
                    },
                "event_time_count":{
                    "12:01am-3am": 0,
                    "3:01am-6am" : 0,
                    "6:01am-9am" : 0,
                    "9:01am-12noon" : 0,
                    "12:01pm-3pm" : 0,
                    "3:01pm-6pm" : 0,
                    "6:01pm-9pm" : 0,
                    "9:01pm-12midnight" : 0
                    }
            }

        dict['total_crime'] = len(data['crimes'])
        street_dict = {}

        for obj in data['crimes']:

        ###parse the crime type###
            if obj.get('type')=='Assault':
                dict['crime_type_count']['Assault'] = dict['crime_type_count'].get('Assault')+1
            elif obj.get('type')=='Arrest':
                dict['crime_type_count']['Arrest'] = dict['crime_type_count'].get('Arrest')+1
            elif obj.get('type')=='Burglary':
                dict['crime_type_count']['Burglary'] = dict['crime_type_count'].get('Burglary')+1
            elif obj.get('type')=='Robbery':
                dict['crime_type_count']['Robbery'] = dict['crime_type_count'].get('Robbery')+1
            elif obj.get('type')=='Theft':
                dict['crime_type_count']['Theft'] = dict['crime_type_count'].get('Theft')+1
            elif obj.get('type')=='Other':
                dict['crime_type_count']['Other'] = dict['crime_type_count'].get('Other')+1


            ####parse the crime time#####
            ti = obj.get('date')[9:]
            #x = '11:01 PM'

            d = dt.strptime(ti,'%I:%M %p')
            crime_time = d.time()
            #print crime_time
            #print now_time < crime_time
            #print crime_time
            if crime_time >= t(00,01,00) and crime_time <= t(03,00,00):
                dict['event_time_count']['12:01am-3am'] = dict['event_time_count'].get('12:01am-3am')+1
            elif crime_time >= t(03,01,00) and crime_time <= t(06,00,00):
                dict['event_time_count']['3:01am-6am'] = dict['event_time_count'].get('3:01am-6am')+1
            elif crime_time >= t(06,01,00) and crime_time <= t(9,00,00):
                dict['event_time_count']['6:01am-9am'] = dict['event_time_count'].get('6:01am-9am')+1
            elif crime_time >= t(9,01,00) and crime_time <= t(12,00,00):
                dict['event_time_count']['9:01am-12noon'] = dict['event_time_count'].get('9:01am-12noon')+1
            elif crime_time >= t(12,01,00) and crime_time <= t(15,00,00):
                dict['event_time_count']['12:01pm-3pm'] = dict['event_time_count'].get('12:01pm-3pm')+1
            elif crime_time >= t(15,01,00) and crime_time <= t(18,00,00):
                dict['event_time_count']['3:01pm-6pm'] = dict['event_time_count'].get('3:01pm-6pm')+1
            elif crime_time >= t(18,01,00) and crime_time <= t(21,00,00):
                dict['event_time_count']['6:01pm-9pm'] = dict['event_time_count'].get('6:01pm-9pm')+1
            elif crime_time >= t(21,01,00) and crime_time < t(00,00,00) or crime_time == t(00,00,00):
                dict['event_time_count']['9:01pm-12midnight'] = dict['event_time_count'].get('9:01pm-12midnight')+1
            #print dict
        #print data["crimes"]["address"]

            ###parse the street####
            add = obj.get('address')
            a=''
            if 'OF ' in add:
                a = add.split('OF ')
                for o in a:
                    o=o.strip()
                    if 'ST' in o:
                        if street_dict.has_key(o):
                            street_dict[o] = street_dict.get(o)+1
                        else:
                            street_dict.update({o:1})
            elif '& ' in add:
                a =add.split('& ')
                for o in a:
                    o=o.strip()
                    if 'ST' in o:
                        if street_dict.has_key(o):
                            street_dict[o] = street_dict.get(o)+1
                        else:
                            street_dict.update({o:1})
            elif 'BLOCK ' in add:
                a = add.split('BLOCK ')
                for o in a:
                    o=o.strip()
                    if 'ST' in o:
                        if street_dict.has_key(o):
                            street_dict[o] = street_dict.get(o)+1
                        else:
                            street_dict.update({o:1})

        dict['the_most_dangerous_streets'] = heapq.nlargest(3,street_dict, key=street_dict.get) 
        yield dict


if __name__ == '__main__':
    # Python daemon boilerplate
    from wsgiref.simple_server import make_server

    logging.basicConfig(level=logging.DEBUG)
    application = Application([HelloWorldService], 'checkcrime.',
        in_protocol=HttpRpc(validator='soft'),
        out_protocol=JsonDocument(ignore_wrappers=True),
    )

    wsgi_application = WsgiApplication(application)

    server = make_server('127.0.0.1', 8000, wsgi_application)

    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()