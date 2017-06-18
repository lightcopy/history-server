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
import logging
import logging.config
from paths import CONF_PATH

# main configuration file to use
conf_file = os.path.join(CONF_PATH, "log.conf")
# fallback configuration in case file does not exist
conf_dict = {
    "version": 1,
    "formatters": {
        "basic": "[%(asctime)s] %(name)s %(levelname)s - %(message)s"
    },
    "handlers": {
        "class": "logging.StreamHandler",
        "level": "DEBUG",
        "formatter": "basic"
    }
}

# Default application logger
# Log messages available:
# - debug(msg, *args, **kwargs)
# - info(msg, *args, **kwargs)
# - warning(msg, *args, **kwargs)
# - error(msg, *args, **kwargs)
# - critical(msg, *args, **kwargs)
# - exception(msg, *args, **kwargs)

# try loading configuration file, if not found, leave default

if os.path.exists(conf_file): # pragma: no cover
    logging.config.fileConfig(conf_file)
else: # pragma: no cover
    print "[WARN] Configuration is not found for '%s'" % conf_file
    print "[WARN] Loading default logger configuration"
    logging.config.dictConfig(conf_dict)

def getLogger(name):
    """
    Get logger for module/class name.

    :return: logger
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger
