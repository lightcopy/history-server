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

import Queue as threadqueue
import unittest
import mock
import src.hm as hm

# mock logger to remove verbose output
hm.logger = mock.Mock()

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

# pylint: disable=W0212,protected-access
class EventProcessSuite(unittest.TestCase):
    def test_event_process_init(self):
        # create event process with valid attributes
        queue = mock.Mock()
        conn = mock.Mock()
        proc = hm.EventProcess("exec_id", "mongo_uri", 1.2, queue, conn)
        self.assertEquals(proc._exec_id, "exec_id")
        self.assertEquals(proc._mongo_uri, "mongo_uri")
        self.assertEquals(proc._interval, 1.2)
        self.assertEquals(proc._app_queue, queue)
        self.assertEquals(proc._conn, conn)

        # fail to create because of the invalid interval
        with self.assertRaises(ValueError):
            hm.EventProcess("exec_id", "mongo", 0.0, queue, conn)
        with self.assertRaises(ValueError):
            hm.EventProcess("exec_id", "mongo", -1.0, queue, conn)

    def test_get_next_app(self):
        # mock queue, returns dummy dictionary as application
        app = mock.Mock()
        queue = mock.Mock()
        queue.get.return_value = app
        self.assertEquals(hm.EventProcess.get_next_app(queue), app)
        queue.get.assert_called_with(block=False)

        # test when queue is empty
        queue.get.side_effect = threadqueue.Empty()
        self.assertEquals(hm.EventProcess.get_next_app(queue), None)

    @mock.patch("src.hm.time")
    def test_process_app_err_1(self, mock_time):
        # Update test once process_app is modified to process event log
        app = mock.Mock()
        proc = hm.EventProcess("exec_id", "mongo", 1.2, mock.Mock(), mock.Mock())

        mock_time.sleep.side_effect = Exception("Test")
        with self.assertRaises(Exception):
            proc._process_app(app)

    @mock.patch("src.hm.time")
    def test_process_app_err_2(self, mock_time):
        # Update test once process_app is modified to process event log
        app = mock.Mock()
        proc = hm.EventProcess("exec_id", "mongo", 1.2, mock.Mock(), mock.Mock())

        mock_time.sleep.side_effect = KeyboardInterrupt("Test")
        with self.assertRaises(KeyboardInterrupt):
            proc._process_app(app)

    @mock.patch("src.hm.time")
    @mock.patch("src.hm.util")
    def test_process_app_ok(self, mock_util, mock_time):
        # Update test once process_app is modified to process event log
        app = mock.Mock()
        app.app_id = "app"
        conn = mock.Mock()
        proc = hm.EventProcess("exec_id", "mongo", 1.2, mock.Mock(), conn)

        mock_time.sleep.side_effect = StandardError("Test")
        mock_util.time_now.return_value = 123L
        proc._process_app(app)
        conn.send.assert_called_with(
            {"app_id": "app", "status": hm.APP_FAILURE, "finish_time": 123L})

        # case when no exception is thrown
        mock_time.sleep.side_effect = None
        mock_util.time_now.return_value = 123L
        proc._process_app(app)
        conn.send.assert_called_with(
            {"app_id": "app", "status": hm.APP_SUCCESS, "finish_time": 123L})

    def test_str_repr(self):
        proc = hm.EventProcess("id", "mongo", 1.2, mock.Mock(), mock.Mock())
        res = "%s" % proc
        self.assertEquals(res, "{exec_id: id, mongo_uri: mongo, interval: 1.2}")
        self.assertEquals(proc.__repr__(), proc.__str__())
# pylint: enable=W0212,protected-access

# pylint: disable=W0212,protected-access
class WatchProcessSuite(unittest.TestCase):
    @mock.patch("src.hm.fs")
    def test_watch_process_init(self, mock_fs):
        fsk = mock.Mock()
        fsk.isdir.return_value = True
        mock_fs.from_path.return_value = fsk
        app_dict = mock.Mock()
        app_queue = mock.Mock()
        conns = [mock.Mock()]

        # test valid process
        proc = hm.WatchProcess(1.2, "/path/to/dir", app_dict, app_queue, conns)
        self.assertEquals(proc._fs, fsk)
        self.assertEquals(proc._interval, 1.2)
        self.assertEquals(proc._root, "/path/to/dir")
        self.assertEquals(proc._apps, app_dict)
        self.assertEquals(proc._app_queue, app_queue)
        self.assertEquals(proc._conns, conns)

        # invalid interval
        with self.assertRaises(ValueError):
            hm.WatchProcess(0.0, "/path/to/dir", app_dict, app_queue, conns)
        with self.assertRaises(ValueError):
            hm.WatchProcess(-1.2, "/path/to/dir", app_dict, app_queue, conns)

        # root is not a directory
        fsk.isdir.return_value = False
        with self.assertRaises(IOError):
            proc = hm.WatchProcess(1.2, "/path/to/dir", app_dict, app_queue, conns)
        fsk.isdir.assert_called_with("/path/to/dir")

    @mock.patch("src.hm.fs.from_path")
    def test_get_applications(self, mock_from_path):
        status = mock.Mock(file_type="d")
        fsk = mock.Mock()
        fsk.isdir.return_value = True
        fsk.listdir.return_value = [status]
        mock_from_path.return_value = fsk

        # test status as directory
        proc = hm.WatchProcess(1.2, "/path/to/dir", {}, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(res, [])

        # test status as file and app in progress
        status = mock.Mock(file_type="f", path="/tmp/app-20170618085827-0000.inprogress")
        fsk.listdir.return_value = [status]
        proc = hm.WatchProcess(1.2, "/tmp", {}, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(res, [])

        # empty app
        status = mock.Mock(file_type="f", path="/tmp/invalid-app")
        fsk.listdir.return_value = [status]
        proc = hm.WatchProcess(1.2, "/tmp", {}, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(res, [])

    @mock.patch("src.hm.fs.from_path")
    def test_get_applications_correct(self, mock_from_path):
        status = mock.Mock(file_type="d")
        fsk = mock.Mock()
        fsk.isdir.return_value = True
        fsk.listdir.return_value = [status]
        mock_from_path.return_value = fsk

        # correct new app
        status = mock.Mock(file_type="f", path="/tmp/app-20170618085827-0000")
        fsk.listdir.return_value = [status]
        proc = hm.WatchProcess(1.2, "/tmp", {}, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(len(res), 1)
        self.assertEquals(res[0].status, hm.APP_PROCESS)

        # correct existing app (SUCCESS)
        status = mock.Mock(file_type="f", path="/tmp/app-20170618085827-0000")
        fsk.listdir.return_value = [status]
        app_dict = {"app-20170618085827-0000": mock.Mock(status=hm.APP_SUCCESS)}
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(len(res), 0)

        # correct existing app (PROCESS)
        status = mock.Mock(file_type="f", path="/tmp/app-20170618085827-0000")
        fsk.listdir.return_value = [status]
        app_dict = {"app-20170618085827-0000": mock.Mock(status=hm.APP_PROCESS)}
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(len(res), 0)

        # correct failed app, atime < failed time and mtime < failed time
        status = mock.Mock(
            file_type="f",
            path="/tmp/app-20170618085827-0000",
            access_time=1L,
            modification_time=1L
        )
        fsk.listdir.return_value = [status]
        app_dict = {
            "app-20170618085827-0000": mock.Mock(status=hm.APP_FAILURE, modification_time=2L)
        }
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(len(res), 0)

        # correct failed app, mtime > failed time
        status = mock.Mock(
            file_type="f",
            path="/tmp/app-20170618085827-0000",
            access_time=1L,
            modification_time=3L
        )
        fsk.listdir.return_value = [status]
        app_dict = {
            "app-20170618085827-0000": mock.Mock(status=hm.APP_FAILURE, modification_time=2L)
        }
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(len(res), 1)
        res[0].update_status.assert_called_with(hm.APP_CLEANUP_PROCESS)

        # correct failed app with atime and mtime larger than current time
        status = mock.Mock(
            file_type="f",
            path="/tmp/app-20170618085827-0000",
            access_time=3L,
            modification_time=3L
        )
        fsk.listdir.return_value = [status]
        app_dict = {
            "app-20170618085827-0000": mock.Mock(status=hm.APP_FAILURE, modification_time=2L)
        }
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(len(res), 1)
        res[0].update_status.assert_called_with(hm.APP_CLEANUP_PROCESS)

        # correct failed app with atime > current time and mtime < current time
        status = mock.Mock(
            file_type="f",
            path="/tmp/app-20170618085827-0000",
            access_time=3L,
            modification_time=1L
        )
        fsk.listdir.return_value = [status]
        app_dict = {
            "app-20170618085827-0000": mock.Mock(status=hm.APP_FAILURE, modification_time=2L)
        }
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        res = list(proc._get_applications())
        self.assertEquals(len(res), 0)

    @mock.patch("src.hm.fs.from_path")
    def test_process_message(self, mock_from_path):
        fsk = mock.Mock()
        fsk.isdir.return_value = True
        mock_from_path.return_value = fsk

        # test application with success status
        app_dict = {
            "123": hm.Application("123", hm.APP_PROCESS, False, "/tmp/123", 1L)
        }
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        proc._process_message({"app_id": "123", "status": hm.APP_SUCCESS, "finish_time": 123L})
        self.assertEquals(app_dict["123"].status, hm.APP_SUCCESS)
        self.assertEquals(app_dict["123"].modification_time, 123L)

        # test application with failure status
        app_dict = {
            "123": hm.Application("123", hm.APP_PROCESS, False, "/tmp/123", 1L)
        }
        proc = hm.WatchProcess(1.2, "/tmp", app_dict, mock.Mock(), [mock.Mock()])
        proc._process_message({"app_id": "123", "status": hm.APP_FAILURE, "finish_time": 123L})
        self.assertEquals(app_dict["123"].status, hm.APP_FAILURE)
        self.assertEquals(app_dict["123"].modification_time, 123L)
# pylint: enable=W0212,protected-access

# pylint: disable=W0212,protected-access
class HistoryManagerSuite(unittest.TestCase):
    def test_init(self):
        manager = hm.HistoryManager("/tmp", num_processes=2, interval=5.0, mongo_uri="mongo")

        self.assertEquals(manager._root, "/tmp")
        self.assertEquals(manager._mongo_uri, "mongo")
        self.assertEquals(manager._num_processes, 2)
        self.assertEquals(manager._interval, 5.0)
        self.assertEquals(manager._exec_interval, 1.0)
        self.assertEquals(manager._executors, [])
        self.assertEquals(manager._watch, None)

        with self.assertRaises(ValueError):
            hm.HistoryManager("/tmp", num_processes=0, interval=5.0)
        with self.assertRaises(ValueError):
            hm.HistoryManager("/tmp", num_processes=-1, interval=5.0)

        with self.assertRaises(ValueError):
            hm.HistoryManager("/tmp", num_processes=2, interval=0.0)
        with self.assertRaises(ValueError):
            hm.HistoryManager("/tmp", num_processes=2, interval=-1.0)

    def test_prepare_event_process(self):
        exc, conn = hm.HistoryManager.prepare_event_process("ep-123", "mongo", 12.3, mock.Mock())
        self.assertTrue(conn is not None)
        self.assertTrue(isinstance(exc, hm.EventProcess))
        self.assertEquals(exc.__str__(), "{exec_id: ep-123, mongo_uri: mongo, interval: 12.3}")

    @mock.patch("src.hm.fs")
    def test_prepare_watch_process(self, mock_fs):
        mock_fs.from_path.return_value = mock.Mock()
        res = hm.HistoryManager.prepare_watch_process(
            12.3, "/abc", mock.Mock(), mock.Mock(), [mock.Mock()])
        self.assertTrue(isinstance(res, hm.WatchProcess))
        self.assertEquals(res._interval, 12.3)
        self.assertEquals(res._root, "/abc")

    def test_clean_up_state(self):
        manager = hm.HistoryManager("/abc")
        manager._clean_up_state()
        self.assertEquals(manager._app_queue, None)
        self.assertEquals(manager._executors, None)
        self.assertEquals(manager._watch, None)
        self.assertEquals(manager._apps, None)
        self.assertEquals(manager._mongo_uri, None)

    def test_app_status(self):
        manager = hm.HistoryManager("/abc")
        manager._apps = {
            "app-1": mock.Mock(status=hm.APP_PROCESS),
            "app-2": mock.Mock(status=hm.APP_SUCCESS)
        }
        self.assertEquals(manager.app_status("app-1"), hm.APP_PROCESS)
        self.assertEquals(manager.app_status("app-2"), hm.APP_SUCCESS)
        self.assertEquals(manager.app_status("app-3"), None)

    def test_start(self):
        event_pr, conn = mock.Mock(), mock.Mock()
        watch_pr = mock.Mock()
        hm.HistoryManager.prepare_event_process = mock.Mock()
        hm.HistoryManager.prepare_event_process.return_value = (event_pr, conn)
        hm.HistoryManager.prepare_watch_process = mock.Mock()
        hm.HistoryManager.prepare_watch_process.return_value = watch_pr

        manager = hm.HistoryManager("/abc", num_processes=3, interval=4.3, mongo_uri="mongo")
        manager.start()

        # internal interval is 1.0 seconds
        calls = [
            mock.call("event_process-0", "mongo", 1.0, manager._app_queue),
            mock.call("event_process-1", "mongo", 1.0, manager._app_queue),
            mock.call("event_process-2", "mongo", 1.0, manager._app_queue)
        ]
        hm.HistoryManager.prepare_event_process.assert_has_calls(calls)
        self.assertEquals(manager._executors, [event_pr, event_pr, event_pr])
        event_pr.start.assert_has_calls([mock.call(), mock.call(), mock.call()])

        calls = [
            mock.call(4.3, "/abc", manager._apps, manager._app_queue, [conn, conn, conn])
        ]
        hm.HistoryManager.prepare_watch_process.assert_has_calls(calls)
        watch_pr.start.assert_has_calls([mock.call()])


    def test_stop(self):
        event_pr, conn = mock.Mock(), mock.Mock()
        event_pr.is_alive.return_value = True
        watch_pr = mock.Mock()
        watch_pr.is_alive.return_value = True
        hm.HistoryManager.prepare_event_process = mock.Mock()
        hm.HistoryManager.prepare_event_process.return_value = (event_pr, conn)
        hm.HistoryManager.prepare_watch_process = mock.Mock()
        hm.HistoryManager.prepare_watch_process.return_value = watch_pr

        manager = hm.HistoryManager("/abc", num_processes=3, interval=4.3)
        manager.start()
        manager.stop()

        # assertions
        watch_pr.is_alive.assert_called_with()
        watch_pr.terminate.assert_called_with()
        watch_pr.join.assert_called_with()
        event_pr.is_alive.assert_has_calls([mock.call(), mock.call(), mock.call()])
        event_pr.terminate.assert_has_calls([mock.call(), mock.call(), mock.call()])
        event_pr.join.assert_has_calls([mock.call(), mock.call(), mock.call()])

    def test_stop_no_processes(self):
        # test when watch and event processes are already terminated
        event_pr, conn = mock.Mock(), mock.Mock()
        event_pr.is_alive.return_value = False
        watch_pr = mock.Mock()
        watch_pr.is_alive.return_value = False
        hm.HistoryManager.prepare_event_process = mock.Mock()
        hm.HistoryManager.prepare_event_process.return_value = (event_pr, conn)
        hm.HistoryManager.prepare_watch_process = mock.Mock()
        hm.HistoryManager.prepare_watch_process.return_value = watch_pr

        manager = hm.HistoryManager("/abc", num_processes=3, interval=4.3)
        manager.start()
        manager.stop()

        watch_pr.is_alive.assert_called_with()
        watch_pr.terminate.assert_has_calls([])
        watch_pr.join.assert_called_with()
        event_pr.is_alive.assert_has_calls([mock.call(), mock.call(), mock.call()])
        event_pr.terminate.assert_has_calls([])
        event_pr.join.assert_has_calls([mock.call(), mock.call(), mock.call()])
# pylint: enable=W0212,protected-access

def suites():
    return [
        ApplicationSuite,
        EventProcessSuite,
        WatchProcessSuite,
        HistoryManagerSuite
    ]
