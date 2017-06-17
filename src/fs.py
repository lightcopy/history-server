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

import grp
import os
import pwd
import src.util as util
from snakebite.client import Client

DEFAULT_HDFS_HOST = "localhost"
DEFAULT_HDFS_PORT = 8020
# file types
FILETYPE_FILE = "f"
FILETYPE_DIR = "d"

def get(scheme=None, host=None, port=None):
    """
    Get file system client based on scheme, host, and port.
    Example:
        fs.get("hdfs") returns HDFS client with default host and port
        fs.get() returns local file system client
        fs.get("hdfs", "abc", 8020) returns HDFS client to connect to hdfs://abc:8020

    :param scheme: uri scheme, either empty or "hdfs"
    :param host: host value, if empty then default value is used
    :param port: port value, if empty then default value is used
    :return: file system client
    """
    if scheme == "hdfs":
        hdfs_host = host if host else DEFAULT_HDFS_HOST
        hdfs_port = port if port else DEFAULT_HDFS_PORT
        return HDFS(hdfs_host, hdfs_port)
    else:
        return LocalFileSystem()

def from_path(path):
    """
    Infer file system client from path.
    Similar to `.get()` method, this method returns file system client based on settings in
    provided uri.
    Example:
        fs.from_path("/tmp") returns local file system client
        fs.from_path("hdfs:/tmp") returns HDFS client with default settings
        fs.from_path("hdfs://abc:8020/tmp") returns HDFS client for hdfs://abc:8020

    :param path: file system path for inference
    :return: file system client
    """
    scheme, host, port, path = util.parse_path(path)
    return get(scheme, host, port)

class FileSystem(object):
    """
    Abstract file system class.
    Provides basic methods to access files and list directories.
    """
    def isdir(self, path):
        """
        Return True if path exists and is directory, False otherwise.

        :param path: path to test
        :return: True if path exists and is directory, False otherwise
        """
        raise NotImplementedError()

    def exists(self, path):
        """
        Return True if path exists in file system, False otherwise.

        :param path: path to test
        :return: True if path exists, False otherwise
        """
        raise NotImplementedError()

    def listdir(self, path):
        """
        Return child inodes as list of dictionaries:
        {
            'group': u'supergroup',
            'permission': 420,
            'file_type': 'f',
            'access_time': 1367317324982L,
            'block_replication': 1,
            'modification_time': 1367317325346L,
            'length': 6783L,
            'blocksize': 134217728L,
            'owner': u'wouter',
            'path': '/Makefile'
        }
        If path is not a directory should throw exception.

        :param path: directory path to list
        :return: list of child directories or files
        """
        raise NotImplementedError()

    def cat(self, path):
        """
        Return generator of strings when reading file.
        If path is directory should throw an exception. Caller must ensure that generator is closed
        after usage.

        :param path: file path to read (assumed to be a text file)
        :return: generator of strings representing content of the file
        """
        raise NotImplementedError()

class LocalFileSystem(FileSystem):
    def __init__(self):
        self._root = "/"

    def _norm_path(self, path):
        """
        Resolve and normalize path.
        """
        return os.path.realpath(path)

    def _path_stat(self, path):
        file_path = self._norm_path(path)
        file_type = FILETYPE_DIR if os.path.isdir(file_path) else FILETYPE_FILE
        stat_result = os.stat(file_path)
        owner = pwd.getpwuid(stat_result.st_uid).pw_name
        group = grp.getgrgid(stat_result.st_gid).gr_name
        permission = int(oct(stat_result.st_mode & 0777))
        return {
            "group": group,
            "owner": owner,
            "permission": permission,
            "file_type": file_type,
            "access_time": stat_result.st_ctime * 1000L,
            "modification_time": stat_result.st_mtime * 1000L,
            "length": long(stat_result.st_size),
            "block_replication": 0,
            "blocksize": 0L,
            "path": file_path
        }

    def isdir(self, path):
        normpath = self._norm_path(path)
        return os.path.isdir(normpath)

    def exists(self, path):
        normpath = self._norm_path(path)
        return os.path.exists(normpath)

    def listdir(self, path):
        normpath = self._norm_path(path)
        if not self.isdir(normpath):
            raise OSError("Not a directory: '%s'" % normpath)
        return [self._path_stat(os.path.join(normpath, x)) for x in os.listdir(normpath)]

    def cat(self, path):
        normpath = self._norm_path(path)
        if self.isdir(normpath):
            raise OSError("Cannot open %s, is a directory" % normpath)
        return open(normpath, "r")

class HDFS(FileSystem):
    def __init__(self, host, port):
        self._scheme = "hdfs"
        self._host = host
        self._port = port
        self._client = Client(host, port=port, use_trash=False)

    def _norm_path(self, path):
        """
        Normalize path and extract filepath component.
        """
        # method returns (scheme, host, port, path)
        filepath = util.parse_path(path)[3]
        return filepath if os.path.isabs(filepath) else os.path.realpath(filepath)

    def _norm_uri(self, path):
        return util.merge_path(self._scheme, self._host, self._port, path)

    def _path_stat(self, obj):
        """
        Reconstruct object stats.
        """
        file_path = self._norm_uri(obj["path"])
        file_type = FILETYPE_DIR if obj["file_type"] == "d" else FILETYPE_FILE
        return {
            "group": str(obj["group"]),
            "owner": str(obj["owner"]),
            "permission": obj["permission"],
            "file_type": file_type,
            "access_time": obj["access_time"],
            "modification_time": obj["modification_time"],
            "length": obj["length"],
            "block_replication": obj["block_replication"],
            "blocksize": obj["blocksize"],
            "path": file_path
        }

    def isdir(self, path):
        normpath = self._norm_path(path)
        return self._client.test(normpath, exists=True, directory=True)

    def exists(self, path):
        normpath = self._norm_path(path)
        return self._client.test(normpath, exists=True)

    def listdir(self, path):
        normpath = self._norm_path(path)
        if not self.isdir(normpath):
            raise OSError("Not a directory: '%s'" % normpath)
        gen = self._client\
            .ls([normpath], recurse=False, include_toplevel=False, include_children=True)
        return [self._path_stat(x) for x in gen]

    def cat(self, path):
        normpath = self._norm_path(path)
        if self.isdir(normpath):
            raise OSError("Cannot open %s, is a directory" % normpath)
        # cat returns generator of generator of strings which is different from documentation
        fgen = self._client.cat([normpath], check_crc=False)
        # always return the first generator for provided path
        return fgen.next()
