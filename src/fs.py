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

# pylint: disable=R0913,too-many-arguments
# pylint: disable=E1305,too-many-format-args
class FileStatus(object):
    """
    FileStatus class represents inode metadata in either local file system or HDFS and mirrors
    org.apache.hadoop.fs.FileStatus class in Hadoop.
    """
    def __init__(self, group, owner, permission, file_type, access_time, modification_time, length,
                 block_replication, block_size, path):
        self._group = group
        self._owner = owner
        self._permission = permission
        self._file_type = file_type
        self._access_time = access_time
        self._modification_time = modification_time
        self._length = length
        self._block_replication = block_replication
        self._block_size = block_size
        self._path = path

    @property
    def group(self):
        return self._group

    @property
    def owner(self):
        return self._owner

    @property
    def permission(self):
        return self._permission

    @property
    def file_type(self):
        return self._file_type

    @property
    def access_time(self):
        return self._access_time

    @property
    def modification_time(self):
        return self._modification_time

    @property
    def length(self):
        return self._length

    @property
    def block_replication(self):
        return self._block_replication

    @property
    def block_size(self):
        return self._block_size

    @property
    def path(self):
        return self._path

    def __str__(self):
        return ("{group: %s, owner: %s, permission: %s, file_type: %s, atime: %s, mtime: %s, " + \
            "length: %s, block_replication: %s, block_size: %s, path: %s}") % (
                self.group, self.owner, self.permission, self.file_type, self.access_time,
                self.modification_time, self.length, self.block_replication, self.block_size,
                self.path)

    def __repr__(self):
        return self.__str__()
# pylint: enable=R0913,too-many-arguments
# pylint: enable=E1305,too-many-format-args

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
        Return child inodes as generator of FileStatus.
        If path is not a directory should throw exception.

        :param path: directory path to list
        :return: generator of child directories or files
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
        if not path:
            raise ValueError("Path %s" % path)
        return os.path.realpath(path)

    def _path_stat(self, path):
        file_path = self._norm_path(path)
        file_type = FILETYPE_DIR if os.path.isdir(file_path) else FILETYPE_FILE
        stat_result = os.stat(file_path)
        owner = pwd.getpwuid(stat_result.st_uid).pw_name
        group = grp.getgrgid(stat_result.st_gid).gr_name
        permission = int(oct(stat_result.st_mode & 0777))
        return FileStatus(
            group=group,
            owner=owner,
            permission=permission,
            file_type=file_type,
            access_time=long(stat_result.st_atime * 1000L),
            modification_time=long(stat_result.st_mtime * 1000L),
            length=long(stat_result.st_size),
            block_replication=0,
            block_size=0L,
            path=file_path
        )

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
        for node in os.listdir(normpath):
            yield self._path_stat(os.path.join(normpath, node))

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
        self._fs = Client(host, port=port, use_trash=False)

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
        return FileStatus(
            group=str(obj["group"]),
            owner=str(obj["owner"]),
            permission=obj["permission"],
            file_type=file_type,
            access_time=obj["access_time"],
            modification_time=obj["modification_time"],
            length=obj["length"],
            block_replication=obj["block_replication"],
            block_size=obj["blocksize"],
            path=file_path
        )

    def isdir(self, path):
        normpath = self._norm_path(path)
        return self._fs.test(normpath, exists=True, directory=True)

    def exists(self, path):
        normpath = self._norm_path(path)
        return self._fs.test(normpath, exists=True)

    def listdir(self, path):
        normpath = self._norm_path(path)
        if not self.isdir(normpath):
            raise OSError("Not a directory: '%s'" % normpath)
        gen = self._fs\
            .ls([normpath], recurse=False, include_toplevel=False, include_children=True)
        for node in gen:
            yield self._path_stat(node)

    def cat(self, path):
        normpath = self._norm_path(path)
        if self.isdir(normpath):
            raise OSError("Cannot open %s, is a directory" % normpath)
        # cat returns generator of generator of strings which is different from documentation
        fgen = self._fs.cat([normpath], check_crc=False)
        # always return the first generator for provided path
        return fgen.next()
