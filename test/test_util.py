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

import unittest
import mock
import src.util as util

class UtilSuite(unittest.TestCase):
    def test_parse_path(self):
        self.assertEquals(util.parse_path(""), ("", "", None, ""))
        self.assertEquals(util.parse_path("hdfs:/path"), ("hdfs", "", None, "/path"))
        self.assertEquals(util.parse_path("hdfs:/host/path"), ("hdfs", "", None, "/host/path"))
        self.assertEquals(util.parse_path("hdfs:/host:/path"), ("hdfs", "", None, "/host:/path"))
        self.assertEquals(util.parse_path("hdfs://host:8020/path"), ("hdfs", "host", 8020, "/path"))
        self.assertEquals(util.parse_path("/path"), ("", "", None, "/path"))
        self.assertEquals(util.parse_path("file:/path"), ("file", "", None, "/path"))

    def test_merge_path(self):
        self.assertEquals(util.merge_path("hdfs", "host", 8020, "/path"), "hdfs://host:8020/path")
        self.assertEquals(util.merge_path("hdfs", "host", "8020", "/path"), "hdfs://host:8020/path")
        self.assertEquals(util.merge_path("hdfs", "host", 8020, "path"), "hdfs://host:8020/path")
        self.assertEquals(util.merge_path("hdfs", "host", None, "/path"), "hdfs://host/path")
        self.assertEquals(util.merge_path("hdfs", "", None, "/path"), "hdfs:/path")
        self.assertEquals(util.merge_path("hdfs", "", "", "/path"), "hdfs:/path")
        self.assertEquals(util.merge_path("file", "", "", "/path"), "file:///path")
        self.assertEquals(util.merge_path("file", "", "", "path"), "file:///path")
        self.assertEquals(util.merge_path("", "", "", "/path"), "/path")
        self.assertEquals(util.merge_path("", "", "", "path"), "path")

    def test_parse_merge_path(self):
        self.assertEquals(
            util.merge_path(*util.parse_path("file:/path")),
            "file:///path"
        )
        self.assertEquals(
            util.merge_path(*util.parse_path("file:///path")),
            "file:///path"
        )
        self.assertEquals(
            util.merge_path(*util.parse_path("hdfs://host:8020/path")),
            "hdfs://host:8020/path"
        )
        self.assertEquals(
            util.merge_path(*util.parse_path("hdfs:/path")),
            "hdfs:/path"
        )

    @mock.patch("src.util.time")
    def test_time_now(self, mock_time):
        mock_time.time.return_value = 12345.1232342
        self.assertEquals(util.time_now(), 12345123L)

def suites():
    return [
        UtilSuite
    ]
