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

import os
import unittest
import mock
import src.fs as fs

# pylint: disable=W0212,protected-access
class FsSuite(unittest.TestCase):
    def test_get(self):
        self.assertTrue(isinstance(fs.get(), fs.LocalFileSystem))
        self.assertTrue(isinstance(fs.get("file"), fs.LocalFileSystem))
        self.assertTrue(isinstance(fs.get("hdfs"), fs.HDFS))
        self.assertTrue(isinstance(fs.get("hdfs", "host", 8020), fs.HDFS))

    def test_from_path(self):
        self.assertTrue(isinstance(fs.from_path("/tmp"), fs.LocalFileSystem))
        self.assertTrue(isinstance(fs.from_path("file:/tmp"), fs.LocalFileSystem))
        self.assertTrue(isinstance(fs.from_path("hdfs:/tmp"), fs.HDFS))
        self.assertTrue(isinstance(fs.from_path("hdfs://localhost:8020/tmp"), fs.HDFS))

    def test_file_system(self):
        client = fs.FileSystem()
        with self.assertRaises(NotImplementedError):
            client.isdir("path")
        with self.assertRaises(NotImplementedError):
            client.exists("path")
        with self.assertRaises(NotImplementedError):
            client.listdir("path")
        with self.assertRaises(NotImplementedError):
            client.cat("path")

    def test_local_fs_norm_path(self):
        client = fs.get()
        self.assertEquals(client._norm_path("tmp"), os.path.realpath("tmp"))
        self.assertEquals(client._norm_path("/tmp"), os.path.realpath("/tmp"))
        with self.assertRaises(ValueError):
            client._norm_path(None)

    def test_file_status_str(self):
        status = fs.FileStatus(
            group="group",
            owner="owner",
            permission=755,
            file_type="f",
            access_time=1000L,
            modification_time=2000L,
            length=123456L,
            block_replication=3,
            block_size=128L,
            path="/path/to/file"
        )
        res = "%s" % status
        self.assertEquals(
            res,
            "{group: group, owner: owner, permission: 755, file_type: f, " + \
            "atime: 1000, mtime: 2000, length: 123456, block_replication: 3, " + \
            "block_size: 128, path: /path/to/file}"
        )
        self.assertEquals(status.__repr__(), status.__str__())

    @mock.patch("src.fs.os")
    @mock.patch("src.fs.pwd")
    @mock.patch("src.fs.grp")
    def test_local_fs_path_stat_dir(self, mock_grp, mock_pwd, mock_os):
        mock_os.path.realpath.return_value = "/xyz"
        mock_os.path.isdir.return_value = True
        # mock for stats
        stat = mock.Mock()
        stat.st_mtime = 2
        stat.st_atime = 3
        stat.st_size = 400L
        stat.st_uid = 123
        stat.st_gid = 124
        stat.st_mode = 1234
        mock_os.stat.return_value = stat
        # mock pwd and grp libs
        getpwuid = mock.Mock()
        getpwuid.pw_name = "my_user"
        getgrgid = mock.Mock()
        getgrgid.gr_name = "my_group"
        mock_pwd.getpwuid.return_value = getpwuid
        mock_grp.getgrgid.return_value = getgrgid

        client = fs.get()
        status = client._path_stat("xyz")
        self.assertEquals(status.group, "my_group")
        self.assertEquals(status.owner, "my_user")
        self.assertEquals(status.permission, int(oct(stat.st_mode & 0777)))
        self.assertEquals(status.file_type, "d")
        self.assertEquals(status.access_time, stat.st_atime * 1000L)
        self.assertEquals(status.modification_time, stat.st_mtime * 1000L)
        self.assertEquals(status.length, stat.st_size)
        self.assertEquals(status.block_replication, 0)
        self.assertEquals(status.block_size, 0L)
        self.assertEquals(status.path, "/xyz")

        # test method calls
        mock_os.stat.assert_called_with("/xyz")
        mock_pwd.getpwuid.assert_called_with(123)
        mock_grp.getgrgid.assert_called_with(124)

    @mock.patch("src.fs.os")
    @mock.patch("src.fs.pwd")
    @mock.patch("src.fs.grp")
    def test_local_fs_path_stat_file(self, mock_grp, mock_pwd, mock_os):
        mock_os.path.realpath.return_value = "/xyz"
        mock_os.path.isdir.return_value = False
        # mock for stats
        stat = mock.Mock()
        stat.st_mtime = 2
        stat.st_atime = 3
        stat.st_size = 400L
        stat.st_uid = 123
        stat.st_gid = 124
        stat.st_mode = 1234
        mock_os.stat.return_value = stat
        # mock pwd and grp libs
        getpwuid = mock.Mock()
        getpwuid.pw_name = "my_user"
        getgrgid = mock.Mock()
        getgrgid.gr_name = "my_group"
        mock_pwd.getpwuid.return_value = getpwuid
        mock_grp.getgrgid.return_value = getgrgid

        client = fs.get()
        status = client._path_stat("xyz")
        self.assertEquals(status.group, "my_group")
        self.assertEquals(status.owner, "my_user")
        self.assertEquals(status.permission, int(oct(stat.st_mode & 0777)))
        self.assertEquals(status.file_type, "f")
        self.assertEquals(status.access_time, stat.st_atime * 1000L)
        self.assertEquals(status.modification_time, stat.st_mtime * 1000L)
        self.assertEquals(status.length, stat.st_size)
        self.assertEquals(status.block_replication, 0)
        self.assertEquals(status.block_size, 0L)
        self.assertEquals(status.path, "/xyz")
        # test method calls
        mock_os.stat.assert_called_with("/xyz")
        mock_pwd.getpwuid.assert_called_with(123)
        mock_grp.getgrgid.assert_called_with(124)

    @mock.patch("src.fs.os")
    def test_local_fs_isdir_false(self, mock_os):
        mock_os.path.realpath.return_value = "/xyz"
        mock_os.path.isdir.return_value = False

        client = fs.get()
        self.assertFalse(client.isdir("xyz"))
        mock_os.path.realpath.assert_called_with("xyz")
        mock_os.path.isdir.assert_called_with("/xyz")

    @mock.patch("src.fs.os")
    def test_local_fs_isdir_true(self, mock_os):
        mock_os.path.realpath.return_value = "/abc"
        mock_os.path.isdir.return_value = True

        client = fs.get()
        self.assertTrue(client.isdir("abc"))
        mock_os.path.realpath.assert_called_with("abc")
        mock_os.path.isdir.assert_called_with("/abc")

    @mock.patch("src.fs.os")
    def test_local_fs_exists_false(self, mock_os):
        mock_os.path.realpath.return_value = "/xyz"
        mock_os.path.exists.return_value = False

        client = fs.get()
        self.assertFalse(client.exists("xyz"))
        mock_os.path.realpath.assert_called_with("xyz")
        mock_os.path.exists.assert_called_with("/xyz")

    @mock.patch("src.fs.os")
    def test_local_fs_exists_true(self, mock_os):
        mock_os.path.realpath.return_value = "/abc"
        mock_os.path.exists.return_value = True

        client = fs.get()
        self.assertTrue(client.exists("abc"))
        mock_os.path.realpath.assert_called_with("abc")
        mock_os.path.exists.assert_called_with("/abc")

    @mock.patch("src.fs.os")
    def test_local_fs_listdir(self, mock_os):
        mock_os.path.realpath.return_value = "/abc"
        mock_os.path.isdir.return_value = False
        mock_os.listdir.return_value = ["file1", "file2"]
        # use default implementation
        mock_os.path.join = os.path.join

        client = fs.get()
        # should fail since path is not directory
        with self.assertRaises(OSError):
            list(client.listdir("abc"))

        # test correct case
        mock_os.path.isdir.return_value = True
        mock_path_stat = mock.Mock()
        client._path_stat = mock_path_stat
        res = [x for x in client.listdir("abc")]
        self.assertEquals(len(res), 2)
        calls = [mock.call("/abc/file1"), mock.call("/abc/file2")]
        mock_path_stat.assert_has_calls(calls)

    @mock.patch("src.fs.os")
    @mock.patch("src.fs.open")
    def test_local_fs_cat(self, mock_open, mock_os):
        mock_os.path.realpath.return_value = "/abc"
        mock_os.path.isdir.return_value = True

        client = fs.get()
        # should fail for directory
        with self.assertRaises(OSError):
            client.cat("abc")

        # should return mock for file
        mock_os.path.isdir.return_value = False
        self.assertTrue(client.cat("abc") is not None)
        mock_open.assert_called_with("/abc", "r")

    def test_hdfs_init(self):
        client = fs.get("hdfs")
        self.assertEquals(client._scheme, "hdfs")
        self.assertEquals(client._host, "localhost")
        self.assertEquals(client._port, 8020)

        client = fs.get("hdfs", "host", 5070)
        self.assertEquals(client._scheme, "hdfs")
        self.assertEquals(client._host, "host")
        self.assertEquals(client._port, 5070)

    @mock.patch("src.fs.os")
    def test_hdfs_norm_path(self, mock_os):
        client = fs.get("hdfs")

        mock_os.path.realpath = os.path.realpath
        mock_os.path.isabs = os.path.isabs
        self.assertEquals(client._norm_path("hdfs://host:8020/path/to/file"), "/path/to/file")
        self.assertEquals(client._norm_path("/path/to/file"), "/path/to/file")

        mock_os.path.realpath = mock.Mock()
        mock_os.path.realpath.return_value = "/root/file"
        self.assertEquals(client._norm_path("path/to/file"), "/root/file")

    def test_hdfs_norm_uri(self):
        client = fs.get("hdfs", "host", 5070)
        self.assertEquals(client._norm_uri("path"), "hdfs://host:5070/path")
        self.assertEquals(client._norm_uri("/path"), "hdfs://host:5070/path")

    def test_hdfs_path_stat(self):
        client = fs.get("hdfs", "host", 5070)
        # test directory
        obj = {
            "group": u"my_group",
            "owner": u"my_owner",
            "permission": 777,
            "file_type": "d",
            "access_time": 4000L,
            "modification_time": 5000L,
            "length": 756L,
            "block_replication": 3,
            "blocksize": 128000L,
            "path": "path/to/dir"
        }
        status = client._path_stat(obj)
        self.assertEquals(status.group, "my_group")
        self.assertEquals(status.owner, "my_owner")
        self.assertEquals(status.permission, 777)
        self.assertEquals(status.file_type, "d")
        self.assertEquals(status.access_time, 4000L)
        self.assertEquals(status.modification_time, 5000L)
        self.assertEquals(status.length, 756L)
        self.assertEquals(status.block_replication, 3)
        self.assertEquals(status.block_size, 128000L)
        self.assertEquals(status.path, "hdfs://host:5070/path/to/dir")

        # test file
        obj = {
            "group": u"my_group",
            "owner": u"my_owner",
            "permission": 555,
            "file_type": "f",
            "access_time": 4000L,
            "modification_time": 5000L,
            "length": 75623498234234934L,
            "block_replication": 3,
            "blocksize": 128000L,
            "path": "path/to/file"
        }
        status = client._path_stat(obj)
        self.assertEquals(status.group, "my_group")
        self.assertEquals(status.owner, "my_owner")
        self.assertEquals(status.permission, 555)
        self.assertEquals(status.file_type, "f")
        self.assertEquals(status.access_time, 4000L)
        self.assertEquals(status.modification_time, 5000L)
        self.assertEquals(status.length, 75623498234234934L)
        self.assertEquals(status.block_replication, 3)
        self.assertEquals(status.block_size, 128000L)
        self.assertEquals(status.path, "hdfs://host:5070/path/to/file")

    def test_hdfs_isdir(self):
        client = fs.get("hdfs")
        client._fs = mock.Mock()
        client.isdir("hdfs://host:8020/path/to/file")
        client._fs.test.assert_called_with("/path/to/file", exists=True, directory=True)

        client.isdir("/a/b/c")
        client._fs.test.assert_called_with("/a/b/c", exists=True, directory=True)

    def test_hdfs_exists(self):
        client = fs.get("hdfs")
        client._fs = mock.Mock()
        client.exists("hdfs://host:8020/path/to/file")
        client._fs.test.assert_called_with("/path/to/file", exists=True)

    def test_hdfs_listdir(self):
        client = fs.get("hdfs", "host", 8020)
        client.isdir = mock.Mock()
        client.isdir.return_value = False

        # should fail to list non-directory
        with self.assertRaises(OSError):
            list(client.listdir("/path"))

        # should list directory
        client.isdir.return_value = True
        client._fs = mock.Mock()
        client._fs.ls.return_value = [{"a": "b"}, {"c": "d"}]
        client._path_stat = mock.Mock()

        res = [x for x in client.listdir("/path")]
        self.assertEquals(len(res), 2)
        client._fs.ls.assert_called_with(
            ["/path"], recurse=False, include_toplevel=False, include_children=True)
        calls = [mock.call({"a": "b"}), mock.call({"c": "d"})]
        client._path_stat.assert_has_calls(calls)

        res = [x for x in client.listdir("hdfs://host:8020/path")]
        self.assertEquals(len(res), 2)
        client._fs.ls.assert_called_with(
            ["/path"], recurse=False, include_toplevel=False, include_children=True)
        calls = [mock.call({"a": "b"}), mock.call({"c": "d"})]
        client._path_stat.assert_has_calls(calls)

    def test_hdfs_cat(self):
        client = fs.get("hdfs")
        client.isdir = mock.Mock()
        client.isdir.return_value = True

        # should fail to cat directory
        with self.assertRaises(OSError):
            list(client.cat("/path"))

        # should return iterator when reading file
        client.isdir.return_value = False
        client._fs = mock.Mock()
        # cat returns generator of generator of strings
        client._fs.cat.return_value = iter([iter(["a", "b", "c"]), iter(["d", "e", "f"])])

        self.assertEquals(list(client.cat("/path")), ["a", "b", "c"])
        client._fs.cat.assert_called_with(["/path"], check_crc=False)

        client._fs.cat.return_value = iter([iter(["a", "b", "c"]), iter(["d", "e", "f"])])
        self.assertEquals(list(client.cat("hdfs://host:8020/path")), ["a", "b", "c"])
        client._fs.cat.assert_called_with(["/path"], check_crc=False)
# pylint: enable=W0212,protected-access

def suites():
    return [
        FsSuite
    ]
