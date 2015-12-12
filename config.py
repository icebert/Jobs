#!/bin/env python

class Config:
    def __init__(self, conf):
        with open(conf, 'r') as f:
            for line in f:
                if line[0] == '#' or line[0] == '\n': continue
                name, value = line.rstrip('\n').rstrip('\r').split()
                setattr(self, name, value)




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