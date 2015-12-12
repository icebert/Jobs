#!/bin/env python
from __future__ import print_function

import re
import os
import sys
import socket
import json
import getpass

from config import Config


class jobsSub:
    def __init__(self, host='127.0.0.1', port=1510, wd='', script=''):
        self.host = host
        self.port = int(port)
        self.wd = wd
        self.script = script
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def submit(self):
        n = int(self.findMeta('n', 1))
        name = self.findMeta('J', '.')
        user = getpass.getuser()
        self.socket.connect((self.host, self.port))
        self.send(json.dumps({'cmd' : 'sub',
                              'wd'  : self.wd,
                              'script' : self.script,
                              'n'   : n,
                              'name': name,
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
    
    def findMeta(self, param, default):
        val = default
        for line in self.script.split('\n'):
            if re.match(r'^#SBATCH ', line) and '-'+param in line.split():
                val = line.split()[2]
                break
        return val





if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: {0} <script file>".format(re.sub(r'.*/', '', sys.argv[0])), file=sys.stderr)
        exit(1)
    
    path = os.path.dirname(os.path.abspath(__file__))
    config = Config(path+'/jobs.conf')
    
    script_file = sys.argv[1]
    try:
        with open(script_file, 'r') as f:
            script = f.read()
    except IOError:
        print("Error: could not find script file", file=sys.stderr)
        exit(1)
    
    try:
        client = jobsSub(config.host, config.port, os.getcwd(), script)
        respond = client.submit()
        
        if respond['status'] == 'ok':
            print("Submitted job {0} with id {1}".format(script_file, respond['msg']), file=sys.stderr)
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