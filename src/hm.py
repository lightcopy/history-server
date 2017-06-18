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
import re
import time
import src.fs as fs
import src.log as log
import src.util as util

# processing statuses
# application has been queued or is being processed
APP_PROCESS = "PROCESS"
# application requires cleanup of previous state, usually when failed application is restarted
APP_CLEANUP_PROCESS = "CLEANUP_PROCESS"
# application is successfully loaded
APP_SUCCESS = "SUCCESS"
# application is failed to load
APP_FAILURE = "FAILURE"

# logger for module
logger = log.getLogger("history-manager")

class Application(object):
    """
    Represents application dictionary with different processing statuses.
    """
    def __init__(self, app_id, status, in_progress, path, modification_time):
        """
        Create application for app_id, processing status, path to the application log file,
        and time of the latest modification to the application.

        :param app_id: application id
        :param status: processing status
        :param in_progress: True if application log is in progress, False otherwise (complete)
        :param path: path to the log file
        :param modification_time: time of modification as long value, in milliseconds
        """
        self._app_id = app_id
        self._status = status
        self._in_progress = in_progress
        self._path = path
        self._modification_time = modification_time

    @staticmethod
    def _parse_app_name(name):
        """
        Return application id and ".inprogress" part.

        Currently we only support applications that follow pattern: "app-YYYYMMDDHHmmss-SSSS" or
        "local-TIMESTAMP". Each application might contain suffix ".inprogress" meaning that it is
        incomplete. Examples:
            - app-20170618085827-0000
            - app-20170618085827-0000.inprogress
            - local-1497733035840
            - local-1497733035840.inprogress

        Method returns tuple (app_id, ".inprogress") if match or (app_id, None) if app is complete

        :param name: file name to parse
        :return: tuple (app_id, '.inprogress') or (app_id, None) if match, or None otherwise
        """
        def match(name):
            groups = re.search(r"^(app-\d{14}-\d{4})(\.inprogress)?$", name)
            return groups if groups else re.search(r"^(local-\d{13})(\.inprogress)?$", name)
        groups = match(name)
        return (groups.group(1), groups.group(2) is not None) if groups else None

    @staticmethod
    def try_infer_from_path(path):
        """
        Create application with process status and default parameters from file path.
        If application cannot be created, None is returned.

        :param path: fully-qualified path to the application log file
        :return: new application
        """
        file_path = util.parse_path(path)[3]
        file_name = os.path.split(file_path)[1]

        res = Application._parse_app_name(file_name)
        if res:
            app_id, in_progress = res
            # application contains full path to the file including scheme, host and port
            return Application(app_id, APP_PROCESS, in_progress, path, util.time_now())
        return None

    @property
    def app_id(self):
        return self._app_id

    @property
    def status(self):
        return self._status

    @property
    def in_progress(self):
        return self._in_progress

    @property
    def path(self):
        return self._path

    @property
    def modification_time(self):
        return self._modification_time

    def update_status(self, status, mtime=None):
        """
        Update status of this application, also automatically increments modification time.

        :param status: new status
        :param mtime: update time for status
        """
        self._status = status
        self._modification_time = mtime if mtime else util.time_now()

    def __str__(self):
        return "{app_id: %s, status: %s, in_progress: %s, path: %s, mtime: %s}" % (
            self._app_id, self.status, self.in_progress, self._path, self._modification_time)

    def __repr__(self):
        return self.__str__()

class EventProcess(multiprocessing.Process):
    """
    Event processing thread that reads application logs synchronously.
    """
    def __init__(self, exec_id, interval, app_queue, exc_conn):
        """
        :param exec_id: unique executor id, used as a name for the process
        :param interval: interval in seconds
        :param app_queue: multiprocessing queue with applications
        :param exc_conn: pipe connection to communicate with watch process
        """
        super(EventProcess, self).__init__()
        self.daemon = True
        self._exec_id = str(exec_id)
        self._conn = exc_conn
        self._app_queue = app_queue
        if not interval > 0.0:
            raise ValueError("Invalid interval %s" % interval)
        self._interval = interval

    @staticmethod
    def get_next_app(queue):
        """
        Get next application from provided queue.
        If queue does not have any apps, None is returned.

        :param queue: multiprocessing queue
        :return: appication or None if queue is empty
        """
        try:
            return queue.get(block=False)
        except threadqueue.Empty:
            return None

    def _process_app(self, app):
        """
        Internal method to process application, app is guaranteed to be not None.
        After processing is done, message is sent to watch process with appropriate status.

        :param app: application to process
        """
        logger.info("Start processing application %s", app)
        # Add another case for exceptions that process can tolerate
        # Make sure to perform cleanup of the previous state if required
        try:
            time.sleep(50.0)
        except StandardError as serr:
            # we can tolerate standard error, log exception and send message to watch process
            # also perform state cleanup
            logger.error("Failed to process application %s, err: %s", app, serr)
            msg = {"app_id": app.app_id, "status": APP_FAILURE, "finish_time": util.time_now()}
            self._conn.send(msg)
        except Exception as err:
            logger.exception("Failed to process application %s, err: %s", app, err)
            raise err
        except KeyboardInterrupt as kint:
            logger.exception("Process %s is interrupted, err: %s", app, kint)
            raise kint
        else:
            logger.info("Finish processing application %s", app)
            msg = {"app_id": app.app_id, "status": APP_SUCCESS, "finish_time": util.time_now()}
            self._conn.send(msg)

    def run(self):
        """
        Run method for the process. We periodically check for available application and launch
        processing. Each application is processed synchronously.
        """
        while True: # pragma: no branch
            app = EventProcess.get_next_app(self._app_queue)
            if app:
                logger.debug("%s processing application %s", self._exec_id, app)
                start_time = util.time_now()
                self._process_app(app)
                end_time = util.time_now()
                logger.info("Processing app %s took %s seconds", app, (end_time - start_time)/1e3)
            time.sleep(self._interval)

    def __str__(self):
        return "{exec_id: %s, interval: %s}" % (self._exec_id, self._interval)

    def __repr__(self):
        return self.__str__()

class WatchProcess(multiprocessing.Process):
    """
    Main process to search applications and launch event processing.
    """
    def __init__(self, interval, root, app_dict, app_queue, conns):
        """
        :param interval: interval in seconds
        :param root: root directory for applications
        :param app_dict: dictionary of applications with statuses
        :param app_queue: application queue that event processes listen to
        :param conns: list of pipe connections to event processes
        """
        super(WatchProcess, self).__init__()
        if not interval > 0.0:
            raise ValueError("Invalid interval %s" % interval)
        self._interval = interval
        self._root = root
        # infer file system that is used to create applications
        self._fs = fs.from_path(root)
        if not self._fs.isdir(root):
            raise IOError("Expected directory, found %s" % root)
        # read/write access to applications dict
        self._apps = app_dict
        self._app_queue = app_queue
        # pipe connections to all executors
        self._conns = conns

    def _get_applications(self):
        """
        List all potential applications for processing.

        :return: generator with valid applications
        """
        for status in self._fs.listdir(self._root):
            logger.debug("Process status %s", status)
            # capture files that can be parsed into applications
            # ignore apps in progress by Spark and apps that are being processed or failed
            if status.file_type == fs.FILETYPE_FILE:
                app = Application.try_infer_from_path(status.path)
                if app and not app.in_progress:
                    if app.app_id not in self._apps:
                        # it is a new app, return
                        logger.debug("New application: %s", app)
                        yield app
                    else:
                        # if app is failed to process we should check status or mtime and compare
                        # it with failure time; if mtime is larger than failure time (content
                        # modification time), reload it with cleanup of the previous state,
                        # otherwise ignore.
                        app = self._apps[app.app_id]
                        if app.status == APP_FAILURE and \
                                status.modification_time >= app.modification_time:
                            app.update_status(APP_CLEANUP_PROCESS)
                            logger.debug("Application: %s has been updated for processing", app)
                            yield app

    def _process_message(self, message):
        """
        Process message and update applications accordingly.

        :param message: message received from event process
        """
        logger.debug("Received message %s", message)
        app = self._apps[message["app_id"]]
        app.update_status(message["status"], message["finish_time"])
        logger.info("Updated application %s", app)
        self._apps[app.app_id] = app

    def run(self):
        # periodically check directory and pull new applications
        while True: # pragma: no branch
            logger.debug("Applications before update: %s", self._apps)
            # check if there are messages from event processes
            logger.debug("Process messages")
            start_time = util.time_now()
            for conn in self._conns:
                while conn.poll():
                    # each message is a dictionary
                    message = conn.recv()
                    self._process_message(message)
            # check root directory for new applications
            # process only files at the root directory
            logger.debug("Search applications")
            for app in self._get_applications():
                logger.info("Schedule application %s", app)
                self._apps[app.app_id] = app
                self._app_queue.put_nowait(app)
            end_time = util.time_now()
            logger.debug("Iteration took %s seconds", (end_time - start_time)/1e3)
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
        if num_processes <= 0:
            raise ValueError("Invalid number of processes: %s" % num_processes)
        self._num_processes = int(num_processes)
        if interval <= 0.0:
            raise ValueError("Invalid interval: %s" % interval)
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

    @staticmethod
    def prepare_event_process(exec_id, interval, app_queue):
        """
        Prepare event process and comminucation pipe.

        :param exec_id: process name
        :param interval: refresh interval in seconds
        :param app_queue: application queue
        :return: tuple (event process, pipe end)
        """
        main_conn, exc_conn = multiprocessing.Pipe()
        exc = EventProcess(exec_id, interval, app_queue, exc_conn)
        return exc, main_conn

    @staticmethod
    def prepare_watch_process(interval, root, apps, app_queue, conns):
        """
        Prepare watch process with all comminucation pipes and list of applications.

        :param interval: interval in seconds
        :param root: root directory to search
        :param apps: initial dictionary with apps
        :param app_queue: application queue
        :param conns: list of pipe connections
        :return: watch process
        """
        return WatchProcess(interval, root, apps, app_queue, conns)

    def _clean_up_state(self):
        """
        Clean up existing state, this involves removing data from db.
        """
        self._app_queue = None
        self._executors = None
        self._watch = None
        # also update statuses for applications
        self._apps = None

    def app_status(self, app_id):
        """
        Fetch current application status for app id.
        If application does not exist, return None.

        :param app_id: app id
        :return: status or None if app does not exist
        """
        return self._apps[app_id].status if app_id in self._apps else None

    def start(self):
        """
        Start history manager and daemon threads.
        """
        logger.info("Start manager")
        pipe_conns = []
        for i in range(self._num_processes):
            exc, pipe_conn = HistoryManager.prepare_event_process(
                "event_process-%s" % i, self._exec_interval, self._app_queue)
            self._executors.append(exc)
            pipe_conns.append(pipe_conn)
            logger.info("Start event process %s", exc)
            exc.start()
        # prepare watch process
        logger.info("Start watch process")
        self._watch = HistoryManager.prepare_watch_process(
            self._interval, self._root, self._apps, self._app_queue, pipe_conns)
        self._watch.start()

    def stop(self):
        """
        Stop history manager and clean up state.
        """
        logger.info("Stop manager")
        # terminate watch process first
        if self._watch.is_alive():
            logger.info("Stop watch process")
            self._watch.terminate()
        self._watch.join()
        # terminate event processes
        for exc in self._executors:
            if exc.is_alive():
                logger.info("Stop event process %s", exc)
                exc.terminate()
            exc.join()
        logger.info("Clean up manager state")
        # clean up internal state
        self._clean_up_state()
