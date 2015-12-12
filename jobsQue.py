#!/bin/env python
from __future__ import print_function

import os
import sys
import socket
import json

from config import Config


class jobsQue:
    def __init__(self, host='127.0.0.1', port=1510, user=''):
        self.host = host
        self.port = int(port)
        self.user = user
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def submit(self):
        self.socket.connect((self.host, self.port))
        self.send(json.dumps({'cmd' : 'que',
                              'user': self.user}))
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
    if len(sys.argv) != 1 and len(sys.argv) != 2:
        print("Usage: {0} <username>".format(sys.argv[0]), file=sys.stderr)
        exit(1)
    
    path = os.path.dirname(os.path.abspath(__file__))
    config = Config(path+'/jobs.conf')
    
    if len(sys.argv) == 1:
        user = ''
    else:
        user = sys.argv[1]
    
    try:
        client = jobsQue(config.host, config.port, user)
        respond = client.submit()
        
        if respond['status'] == 'ok':
            print('\t'.join(['JOBID', 'NAME', 'USER', 'ST', 'TIME', 'CPUs']))
            if respond['msg']:
                for item in respond['msg']:
                    print('\t'.join(item))
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