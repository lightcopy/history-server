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

import multiprocessing
import os
import Queue as threadqueue
import time
import src.fs as fs
import src.util as util
from src.log import logger

# processing statuses
# application has been queued or is being processed
APP_PROCESS = "PROCESS"
# application is successfully loaded
APP_SUCCESS = "SUCCESS"
# application is failed to load
APP_FAILURE = "FAILURE"

class Application(object):
    def __init__(self, app_id, status, path, modification_time):
        """
        Create application for app_id, processing status, path to the application log file,
        and time of the latest modification to the application.

        :param app_id: application id
        :param status: processing status
        :param path: path to the log file
        :param modification_time: time of modification as long value, in milliseconds
        """
        self._app_id = app_id
        self._status = status
        self._path = path
        self._modification_time = modification_time

    @staticmethod
    def from_path(path):
        """
        Create application with process status from file path.

        :param path: fully-qualified path to the application log file
        :return: new application
        """
        file_path = util.parse_path(path)[3]
        file_name = os.path.split(file_path)[1]
        return Application(file_name, APP_PROCESS, path, util.time_now())

    @property
    def app_id(self):
        return self._app_id

    @property
    def status(self):
        return self._status

    @property
    def path(self):
        return self._path

    @property
    def modification_time(self):
        return self._modification_time

    def update_status(self, status, mtime=util.time_now()):
        """
        Update status of this application, also automatically increments modification time.

        :param status: new status
        :param mtime: update time for status
        """
        self._status = status
        self._modification_time = mtime

    def __str__(self):
        return "{app_id: %s, path: %s, status: %s, mtime: %s}" % (
            self._app_id, self._path, self._status, self._modification_time)

    def __repr__(self):
        return self.__str__()

class EventProcess(multiprocessing.Process):
    def __init__(self, exec_id, interval, app_queue, exc_conn):
        super(EventProcess, self).__init__()
        self.daemon = True
        self._exec_id = exec_id
        self._conn = exc_conn
        self._app_queue = app_queue
        self._interval = interval

    @property
    def exec_id(self):
        return self._exec_id

    def _process_app(self, app):
        # process application
        logger.info("Start processing application %s", app)
        time.sleep(50.0)
        logger.info("Finish processing application %s", app)
        msg = {"app_id": app.app_id, "status": APP_SUCCESS, "finish_time": util.time_now()}
        self._conn.send(msg)

    def run(self):
        # processing is synchronous, we only check next task when previous one is either failed or
        # processed successfully
        while True: # pragma: no branch
            # check if new application is available
            app = None
            try:
                app = self._app_queue.get(block=False)
            except threadqueue.Empty:
                logger.debug("%s - no tasks available", self.exec_id)
            if app:
                logger.debug("%s - process application %s", self.exec_id, app)
                self._process_app(app)
            time.sleep(self._interval)

class WatchProcess(multiprocessing.Process):
    def __init__(self, interval, root, apps, app_queue, conns):
        super(WatchProcess, self).__init__()
        self._interval = interval
        self._root = root
        # infer file system that is used to create applications
        self._fs = fs.from_path(root)
        # read/write access to applications dict
        self._apps = apps
        self._app_queue = app_queue
        # pipe connections to all executors
        self._conns = conns

    def _get_applications(self):
        for node in self._fs.listdir(self._root):
            if node["file_type"] == fs.FILETYPE_FILE:
                yield Application.from_path(node["path"])

    def run(self):
        # periodically check directory and pull new applications
        while True: # pragma: no branch
            logger.debug("Applications before update: %s", self._apps)
            # check if there are messages from event processes
            logger.info("Process messages")
            for conn in self._conns:
                while conn.poll():
                    # each message is a dictionary
                    message = conn.recv()
                    logger.debug("Received message %s", message)
                    app = self._apps[message["app_id"]]
                    app.update_status(message["status"], message["finish_time"])
                    logger.info("Updated application %s", app)
                    self._apps[message["app_id"]] = app
            # check root directory for new applications
            # process only files at the root directory
            logger.info("Search applications")
            for app in self._get_applications():
                # if app is being processed or is success we do nto schedule application
                not_schedule = app.app_id in self._apps and (
                    self._apps[app.app_id].status == APP_PROCESS or \
                    self._apps[app.app_id].status == APP_SUCCESS)
                if not_schedule:
                    continue
                logger.debug("Schedule application %s", app)
                self._apps[app.app_id] = app
                self._app_queue.put_nowait(app)
            time.sleep(self._interval)

class HistoryManager(object):
    """
    History manager maintains state and progress of loading and processing new applications.
    """
    def __init__(self, root, num_processes=1, interval=5.0):
        """
        Create history manager for file system folder with number of executor processes and refresh
        interval for directory.

        :param root: directory that contains application logs, spark.eventLog.dir option
        :param num_processes: number of executors to launch for file processing
        :param interval: refresh interval for directory, in seconds
        """
        self._root = root
        self._num_processes = num_processes
        self._interval = interval
        # internal refresh interval for queue in executor process
        self._exec_interval = 1.0
        self.__manager = multiprocessing.Manager()
        # dict of applications that represents process log
        # only watch process has the write access to dictionary, and history manager has only
        # read access
        self._apps = self.__manager.dict()
        # application queue
        self._app_queue = multiprocessing.Queue()
        # list of executor process
        self._executors = []
        # watch process
        self._watch = None

    def _prepare_executor(self, exec_id, interval, app_queue):
        main_conn, exc_conn = multiprocessing.Pipe()
        exc = EventProcess(exec_id, interval, app_queue, exc_conn)
        return exc, main_conn

    def _prepare_watch(self, interval, root, apps, app_queue, conns):
        return WatchProcess(interval, root, apps, app_queue, conns)

    def _clean_up_state(self):
        self._app_queue = None
        self._executors = None
        self._watch = None
        # also update statuses for applications
        self._apps = None

    def app_status(self, app_id):
        return self._apps[app_id] if app_id in self._apps else None

    def start(self):
        logger.info("Start manager")
        pipe_conns = []
        for i in range(self._num_processes):
            exc, pipe_conn = self._prepare_executor(
                "executor-%s" % i, self._exec_interval, self._app_queue)
            self._executors.append(exc)
            pipe_conns.append(pipe_conn)
            logger.info("Start executor %s", exc.exec_id)
            exc.start()
        # prepare watch process
        logger.info("Start watch process")
        self._watch = self._prepare_watch(
            self._interval, self._root, self._apps, self._app_queue, pipe_conns)
        self._watch.start()

    def stop(self):
        logger.info("Stop manager")
        # terminate watch process first
        if self._watch.is_alive():
            logger.info("Stop watch process")
            self._watch.terminate()
        self._watch.join()
        # terminate executors
        for exc in self._executors:
            if exc.is_alive():
                logger.info("Stop executor %s", exc.exec_id)
                exc.terminate()
            exc.join()
        logger.info("Clean up manager state")
        # clean up internal state
        self._clean_up_state()
