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
import src.hm as hm

# pylint: disable=W0212,protected-access
class ApplicationSuite(unittest.TestCase):
    def test_parse_app_name(self):
        # correct cases
        self.assertEquals(
            hm.Application._parse_app_name("app-20170618085827-0000"),
            ("app-20170618085827-0000", False)
        )
        self.assertEquals(
            hm.Application._parse_app_name("app-20170618085827-0000.inprogress"),
            ("app-20170618085827-0000", True)
        )
        self.assertEquals(
            hm.Application._parse_app_name("local-1497733035840"),
            ("local-1497733035840", False)
        )
        self.assertEquals(
            hm.Application._parse_app_name("local-1497733035840.inprogress"),
            ("local-1497733035840", True)
        )

        # incorrect cases
        self.assertEquals(hm.Application._parse_app_name("test"), None)
        self.assertEquals(hm.Application._parse_app_name("    "), None)
        self.assertEquals(hm.Application._parse_app_name("app-20170618085827-000"), None)
        self.assertEquals(hm.Application._parse_app_name("20170618085827-0000"), None)
        self.assertEquals(hm.Application._parse_app_name(" app-20170618085827-0000"), None)
        self.assertEquals(hm.Application._parse_app_name("app-20170618085827-0000 "), None)
        self.assertEquals(
            hm.Application._parse_app_name("app-20170618085827-0000.incomplete"),
            None
        )
        self.assertEquals(hm.Application._parse_app_name("app-20170618085827-0000."), None)
        self.assertEquals(
            hm.Application._parse_app_name("app-20170618085827-0000.progress"),
            None
        )
        self.assertEquals(
            hm.Application._parse_app_name("app-20170618085827-0000.in_progress"),
            None
        )
        self.assertEquals(
            hm.Application._parse_app_name("app-20170618085827-0000.inprogress "),
            None
        )

    @mock.patch("src.hm.util.time_now")
    def test_try_infer_from_path(self, mock_time):
        mock_time.return_value = 1234L
        # infer successfully
        app = hm.Application.try_infer_from_path("/tmp/app-20170618085827-0000.inprogress")
        self.assertEquals(app.app_id, "app-20170618085827-0000")
        self.assertEquals(app.status, hm.APP_PROCESS)
        self.assertEquals(app.in_progress, True)
        self.assertEquals(app.path, "/tmp/app-20170618085827-0000.inprogress")
        self.assertEquals(app.modification_time, 1234L)

        app = hm.Application.try_infer_from_path("/tmp/app-20170618085827-0000")
        self.assertEquals(app.app_id, "app-20170618085827-0000")
        self.assertEquals(app.status, hm.APP_PROCESS)
        self.assertEquals(app.in_progress, False)
        self.assertEquals(app.path, "/tmp/app-20170618085827-0000")
        self.assertEquals(app.modification_time, 1234L)

        app = hm.Application.try_infer_from_path("hdfs://host:8020/tmp/app-20170618085827-0000")
        self.assertEquals(app.app_id, "app-20170618085827-0000")
        self.assertEquals(app.status, hm.APP_PROCESS)
        self.assertEquals(app.in_progress, False)
        self.assertEquals(app.path, "hdfs://host:8020/tmp/app-20170618085827-0000")
        self.assertEquals(app.modification_time, 1234L)

        app = hm.Application.try_infer_from_path(
            "hdfs://host:8020/tmp/local-1497733035840.inprogress")
        self.assertEquals(app.app_id, "local-1497733035840")
        self.assertEquals(app.status, hm.APP_PROCESS)
        self.assertEquals(app.in_progress, True)
        self.assertEquals(app.path, "hdfs://host:8020/tmp/local-1497733035840.inprogress")
        self.assertEquals(app.modification_time, 1234L)

        # fail to infer
        app = hm.Application.try_infer_from_path("/tmp/app-0000.inprogress")
        self.assertEquals(app, None)
        app = hm.Application.try_infer_from_path("/tmp/app-0000")
        self.assertEquals(app, None)
        app = hm.Application.try_infer_from_path("hdfs://host:8020/tmp/app-0000")
        self.assertEquals(app, None)

    @mock.patch("src.hm.util.time_now")
    def test_update_status(self, mock_time):
        mock_time.return_value = 1235L
        app = hm.Application("app-123", hm.APP_PROCESS, False, "/tmp/app-123", 1234L)

        app.update_status(hm.APP_PROCESS)
        self.assertEquals(app.status, hm.APP_PROCESS)
        self.assertEquals(app.modification_time, 1235L)

        app.update_status(hm.APP_SUCCESS, 2345L)
        self.assertEquals(app.status, hm.APP_SUCCESS)
        self.assertEquals(app.modification_time, 2345L)

        app.update_status(hm.APP_FAILURE)
        self.assertEquals(app.status, hm.APP_FAILURE)
        self.assertEquals(app.modification_time, 1235L)

    def test_src_repr(self):
        app = hm.Application("app-123", hm.APP_PROCESS, False, "/a/app-123", 123L)
        res = "%s" % app
        self.assertEquals(
            res,
            "{app_id: app-123, status: PROCESS, in_progress: False, path: /a/app-123, mtime: 123}"
        )
        self.assertEquals(app.__repr__(), res)
# pylint: enable=W0212,protected-access

class HistoryManagerSuite(unittest.TestCase):
    pass

def suites():
    return [
        ApplicationSuite,
        HistoryManagerSuite
    ]
