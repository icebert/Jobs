#!/bin/env python

import re
import os
import sys
import socket
import json
import datetime
import threading
import signal
import errno
import resource

from config import Config

path = os.path.dirname(os.path.abspath(__file__))



class Job(object):
    def __init__(self, id, user, wd, n, name, subtime):
        self.id   = id
        self.user = user
        self.wd   = wd
        self.n    = n
        self.name = name
        self.subtime = subtime
        self.runtime = None
        self.pid = None




class Resource:
    def __init__(self, process):
        self.id = 0
        self.totalcpu = process
        self.freecpu = process
        self.run = []
        self.queue = []
    
    
    def nextID(self):
        self.id += 1
        return self.id
    
    
    def getRunning(self):
        return self.run
    
    
    def getQueued(self):
        return self.queue
    
    
    def find(self, id):
        for job in self.run + self.queue:
            if job.id == id:
                return job
        return None
    
    
    def apply(self, job):
        if job.n > self.totalcpu:
            raise Exception('Requested cpus exceed total available')
        self.queue.append(job)
        self.refresh()
    
    
    def free(self, id):
        index = None
        for i in range(0, len(self.queue)):
            if self.queue[i].id == id:
                index = i
                break
        if index is not None:
            del self.queue[index]
            return
        
        for i in range(0, len(self.run)):
            if self.run[i].id == id:
                index = i
                break
        if index is None:
            return
        
        self.kill(self.run[index].pid)
        self.freecpu += self.run[index].n
        del self.run[index]
        self.refresh()
    
    
    def kill(self, pid):
        try:
            gid = os.getpgid(pid)
            if gid:
                os.killpg(gid, signal.SIGKILL)
        except OSError as e:
            print('{0}\tError: {1}'.format(
                   datetime.datetime.now().strftime(
                   "%Y-%m-%d %H:%M:%S"), e))
    
    
    def refresh(self):
        while self.queue and self.queue[0].n <= self.freecpu:
            try:
                self.freecpu -= self.queue[0].n
                self.run.append(self.queue[0])
                del self.queue[0]
                
                r,w=os.pipe()
                r,w=os.fdopen(r,'r',0), os.fdopen(w,'w',0)
                
                pid = os.fork()
                if pid == 0:
                    os.setsid()
                    pid = os.fork()
                    if pid == 0:
                        os.chdir(self.run[-1].wd)
                        os.umask(0)
                        
                        maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
                        if (maxfd == resource.RLIM_INFINITY):
                            maxfd = 1024
                        for fd in range(0, maxfd):
                            try:
                                os.close(fd)
                            except OSError:
                                pass
                        
                        os.open('/dev/null', os.O_RDWR)
                        os.dup2(0, 1)
                        os.dup2(0, 2)
                        
                        jobid = self.run[-1].id
                        os.execl('/bin/bash', '/bin/bash',
                                 '{path}/scripts/{id}.sh'.format(path=path, id=jobid))
                    else:
                        r.close()
                        w.write(str(pid))
                        w.close()
                        os._exit(0)
                else:
                    w.close()
                    ccpid = r.read()
                    r.close()
                    os.wait()
                    self.run[-1].pid = int(ccpid)
                    self.run[-1].runtime = datetime.datetime.now()
            except Exception as e:
                print('{0}\tError: {1}'.format(datetime.datetime.now().strftime(
                                               "%Y-%m-%d %H:%M:%S"), e))
                




class jobsServer:
    def __init__(self, port=1510, process=1):
        self.resource = Resource(int(process))
        self.lock = threading.Lock()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', int(port)))
    
    
    def run(self):
        self.socket.listen(5)
        while True:
            conn, addr = self.socket.accept()
            handler = Handler(conn, self.lock, self.resource)
            handler.start()




class Handler(threading.Thread):
    def __init__(self, conn, lock, resource):
        threading.Thread.__init__(self)
        self.conn = conn
        self.lock = lock
        self.resource = resource
    
    
    def run(self):
        request = self.receive()
        
        self.lock.acquire()
        try:
            if   request['cmd'] == 'sub':
                msg = self.submit(request)
            elif request['cmd'] == 'del':
                msg = self.delete(request)
            elif request['cmd'] == 'que':
                msg = self.queue(request)
            else:
                raise Exception('Unsupported command')
            status = 'ok'
        except Exception as e:
            status = 'error'
            msg = str(e)
        self.lock.release()
        
        self.send(json.dumps({'status' : status, 'msg' : msg}))
        self.conn.close()
    
    
    def send(self, data):
        self.conn.sendall(str(len(data))+'|'+data)
    
    
    def receive(self):
        chunks = []
        data = self.conn.recv(1024)
        data = data.split('|', 1)
        length = int(data[0])
        data = data[1]
        chunks.append(data)
        length -= len(data)
        while length > 0:
            data = self.conn.recv(1024)
            chunks.append(data)
            length -= len(data)
        return json.loads(''.join(chunks))
    
    
    def submit(self, request):
        id     = self.resource.nextID()
        user   = request['user']
        wd     = request['wd']
        n      = request['n']
        name   = request['name']
        subtime= datetime.datetime.now()
        script = re.sub(r'exit ', path+'/jobsDel.py '+str(id)+' 2>/dev/null\nexit ',
                        request['script'])
        script = ('exec 1>{wd}/job-{id}.stdout\n'.format(wd=wd, id=id) +
                  'exec 2>{wd}/job-{id}.stderr\n'.format(wd=wd, id=id) +
                  'source /home/'+user+'/.bashrc\n' +
                  'cd '+wd+'\n\n' +
                   script + '\n\n' +
                   path+'/jobsDel.py '+str(id)+' 2>/dev/null\n' +
                  'exit 0\n'
                 )
        with open('{path}/scripts/{id}.sh'.format(path=path, id=id), 'w') as f:
            f.write(script)
        
        job = Job(id, user, wd, n, name, subtime)
        self.resource.apply(job)
        return id
    
    
    def delete(self, request):
        id   = request['id']
        user = request['user']
        job  = self.resource.find(id)
        if job is None or job.user != user:
            raise Exception('job with id {0} not found for user {1}'.format(id, user))
        
        self.resource.free(id)
        os.remove('{path}/scripts/{id}.sh'.format(path=path, id=id))
        return id
    
    
    def queue(self, request):
        user = request['user']
        que  = []
        curtime = datetime.datetime.now()
        for job in self.resource.getQueued():
            if user == '' or user == job.user:
                time = datetime.datetime(1,1,1) + datetime.timedelta(
                       seconds=int((curtime - job.subtime).total_seconds()))
                time = '{d}-{h:02d}:{m:02d}:{s:02d}'.format(d=time.day-1,  h=time.hour,
                                                            m=time.minute, s=time.second)
                
                item = [str(job.id), job.name, job.user, 'Q', time, str(job.n)]
                que.append(item)
        
        for job in self.resource.getRunning():
            if user == '' or user == job.user:
                time = datetime.datetime(1,1,1) + datetime.timedelta(
                       seconds=int((curtime - job.runtime).total_seconds()))
                time = '{d}-{h:02d}:{m:02d}:{s:02d}'.format(d=time.day-1,  h=time.hour,
                                                            m=time.minute, s=time.second)
                
                item = [str(job.id), job.name, job.user, 'R', time, str(job.n)]
                que.append(item)
        return que




if __name__ == '__main__':
    if not os.path.exists(path+'/scripts'):
        os.mkdir(path+'/scripts')
    
    config = Config(path+'/jobs.conf')
    
    server = jobsServer(config.port, config.process)
    server.run()




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