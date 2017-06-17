#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#
# Copyright 2017 Lightcopy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import re
import time
import urlparse

def parse_path(path):
    """
    Split file URI into scheme, host, port and actual path.

    :param path: path uri as string, e.g. hdfs://localhost:8020/path/to/file
    :return: tuple (scheme, host, port, path)
    """
    uri = urlparse.urlsplit(path)
    scheme, netloc, path = uri.scheme, uri.netloc, uri.path
    # split netloc into host:port
    groups = re.search(r"^(.+):(\d+)$", netloc)
    host = groups.group(1) if groups else netloc
    port = int(groups.group(2)) if groups else None
    return scheme, host, port, path

def merge_path(scheme, host, port, path):
    """
    Merge path components into uri.

    :param scheme: uri scheme, e.g. hdfs
    :param host: host name
    :param port: port
    :param path: file path
    :return: uri as string
    """
    netloc = "%s:%s" % (host, port) if port else host
    return urlparse.urlunparse((scheme, netloc, path, "", "", ""))

def time_now():
    """
    Get current time in milliseconds.

    :return: unix timestamp in milliseconds as long value
    """
    return long(time.time() * 1000L)
