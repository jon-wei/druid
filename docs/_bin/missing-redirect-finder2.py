#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import sys

if len(sys.argv) != 2:
  sys.stderr.write('usage: program <del_paths_file>\n')
  sys.exit(1)

del_paths = sys.argv[1]

dep_dict = {}
with open(del_paths, 'r') as del_paths_file:
    for line in del_paths_file.readlines():
        subidx = line.index("/", 0)
        line2 = line[subidx+1:]
        subidx = line2.index("/", 0)
        line3 = line2[subidx+1:]
        dep_dict[line3] = True

    listtt = []
    for dep in dep_dict:
        listtt.append(dep)

    listtt.sort()
    for xx in listtt:
        print(xx.strip("\n"))
