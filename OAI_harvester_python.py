#!/usr/bin/env python3

import re
import os
import sys 
import time
import socket
import string
import datetime
import codecs

from pprint import pprint

from urllib.parse import urlparse
from urllib.parse import quote_plus

from http.client import HTTPConnection

from xml.parsers.expat import ExpatError
from xml.etree.ElementTree import fromstring
from xml.etree.ElementTree import tostring
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement

class OAI_2_SOLR(object):
    def __init__(self, mdo):
        self.mdo=mdo
        pass

    def parse_oai_record(self):
        add=Element("add")
        for item in self.mdo.data.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
            doc = SubElement(add,"doc")
            out = SubElement(doc, "field", name="id")
            id = item.findall(".//{http://www.openarchives.org/OAI/2.0/}identifier")[0].text
            out.text = id

            skip = item.findall('.//{http://www.openarchives.org/OAI/2.0/}header')[0].attrib

            if 'status' in skip.keys():
                if skip["status"] == "deleted":
                    skip=True
                    print("deleted  : "+ id)
                    solr_data="<delete><query>id:\""+id+"\"</query></delete>"
                    self.commit(solr_data)
            else:
                skip = False


            if not skip:
                fr = SubElement(doc, "field", name="fullrecord")
                fr.text = ""

                for field in item.findall(".//{http://www.openarchives.org/OAI/2.0/}index")[0]:
                    if field.tag.split('}')[1] == "date":
                        if len(field.text.strip()) == 4:
                            out = SubElement(doc, "field", name="date_date")
                            out.text = field.text+"-01-01T00:00:00Z"
                            fr.text += out.text+" "
                    if field.tag.split('}')[1] == "enddate":
                        if len(field.text.strip()) == 4:
                            out = SubElement(doc, "field", name="enddate_date")
                            out.text = field.text+"-01-01T00:00:00Z"
                            fr.text += out.text+" "

                for element in item.getiterator():
                    if element.text:
                        if len(element.text.strip()) > 0:
                            if not element.tag.split('}')[1] == "datestamp":
                                out = SubElement(doc, "field", name=element.tag.split('}')[1].lower())
                                out.text=element.text
                                fr.text += out.text+" "
                                out = SubElement(doc, "field", name=element.tag.split('}')[1].lower()+"_str")
                                out.text=element.text
                            elif len(element.text.strip()) == 10:
                                out = SubElement(doc, "field", name="datestamp_date")
                                out.text=element.text+"T00:00:00Z"
                                fr.text += out.text+" "

        return(tostring(add).encode('utf-8', 'ignore'))

    def commit(self, data):
        url=urlparse("http://localhost:8080")
        headers = {"Content-type" : "text/xml; charset=utf-8", "Accept": "text/plain"}
        conn = HTTPConnection(url.netloc)
        try:
            conn.request("POST","/solr/update/", data, headers)
            response = conn.getresponse()
            data = response.read()
            for item in fromstring(data).getiterator():
                if "name" in item.attrib.keys():
                    if item.attrib["name"] == "status":
                        if item.text == "0":
                            print("commit ok")
                            return(True)
        except:
            print("epic FAIL")
            time.sleep(10)
            return(False)

        print("epic FAIL")
        time.sleep(10)
        return(False)

class MDO_2_SOLR(object):
    OAIbaseURL = "http://services.kb.nl/mdo/oai"
    next_token = False
    token = ""

    def __init__(self):
        verb = "ListRecords"
        #command = "&resumptionToken=GGC!2009-10-23T22:06:37.076Z!null!INDEXING!1474500"
        command = "&set=GGC&metadataPrefix=INDEXING"
        #url=urlparse(self.OAIbaseURL+"?verb="+verb + command + "&metadataPrefix=INDEXING")
        url=urlparse(self.OAIbaseURL+"?verb="+verb + command )
        conn = HTTPConnection(url.netloc)
        conn.request('GET', url.path + "?" + url.query)
        response = conn.getresponse()

        if response.status == 200:
            data = response.read().decode('utf-8' , 'ignore')
            if data.find('<<') > -1:
                data=data.replace("<<", "<")
                data=data.replace(">>", ">")
            self.data = fromstring(data)
            token=self.data.findall('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')[0].text
            if token:
                self.next_token=token
        else:
            print("Fatal, no connection to " + self.OAIbaseURL)
            os._exit(-1)


    def resume(self):
        if self.next_token:
            verb = "ListRecords"
            print(self.next_token)
            command = "&resumptionToken="+self.next_token

            url = urlparse(self.OAIbaseURL+"?verb="+verb + command)
            response = False

            try:
                conn = HTTPConnection(url.netloc)
                conn.request('GET', url.path + "?" + url.query)
                response = conn.getresponse()
            except:
                print(self.OAIbaseURL+"?verb="+verb + command)
                print("No 200 OK !!, sleeping 1 min.")
                time.sleep(50)
            if response:
                if response.status == 200:
                    data = response.read().decode('utf-8' , 'ignore')
                    self.data = fromstring(data)
                    token = self.data.findall('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')[0].text

                    if token and not self.token == token:
                        self.token = self.next_token 
                        self.next_token = token
                    elif token:
                        print("Token = Next_token")
                        os._exit(-1)
                else:
                    print("No 200 OK !!, sleeping 1 min.")
                    time.sleep(50)

if __name__ == "__main__":
    mdo2solr = MDO_2_SOLR()
    oai2solr = OAI_2_SOLR(mdo2solr)
    data = oai2solr.parse_oai_record()
    while not oai2solr.commit(data):
        pass

    while mdo2solr.next_token:
        mdo2solr.resume()
        data = oai2solr.parse_oai_record()
        while not oai2solr.commit(data):
            pass
