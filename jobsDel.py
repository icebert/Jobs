#!/bin/env python
from __future__ import print_function

import re
import os
import sys
import socket
import json
import getpass

from config import Config


class jobsDel:
    def __init__(self, host='127.0.0.1', port=1510, jobid=0):
        self.host = host
        self.port = int(port)
        self.jobid = jobid
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def submit(self):
        user = getpass.getuser()
        self.socket.connect((self.host, self.port))
        self.send(json.dumps({'cmd' : 'del',
                              'id'  : self.jobid,
                              'user': user
                             }))
        respond = self.receive()
        self.socket.close()
        return respond
    
    def send(self, data):
        self.socket.sendall(str(len(data))+'|'+data)
    
    def receive(self):
        chunks = []
        data = self.socket.recv(1024)
        data = data.split('|', 1)
        length = int(data[0])
        data = data[1]
        chunks.append(data)
        length -= len(data)
        while length > 0:
            data = self.socket.recv(1024)
            chunks.append(data)
            length -= len(data)
        return json.loads(''.join(chunks))




if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: {0} <jobid>".format(re.sub(r'.*/', '', sys.argv[0])), file=sys.stderr)
        exit(1)
    
    path = os.path.dirname(os.path.abspath(__file__))
    config = Config(path+'/jobs.conf')
    
    try:
        jobid = int(sys.argv[1])
    except:
        print("Error: jobid should be numeric", file=sys.stderr)
        exit(1)
    
    try:
        client = jobsDel(config.host, config.port, jobid)
        respond = client.submit()
        
        if respond['status'] == 'ok':
            print("Deleted job {0}".format(respond['msg']), file=sys.stderr)
        else:
            print("Error: {0}".format(respond['msg']), file=sys.stderr)
    except socket.error:
        print("Error: could not communicate with server", file=sys.stderr)




## 
## Copyright (C) 2015 Meng Wang
## 
## This program is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License version 3
## published by the Free Software Foundation. You can obtain a copy of
## the GNU General Public License at
##
##                 http://www.gnu.org/licenses/
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 