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

import sys
from paths import LIB_PATH

if __name__ == "__main__":
    sys.path.insert(1, LIB_PATH)
    # pylint: disable=C0413,wrong-import-position
    from coverage.cmdline import main
    # pylint: enable=C0413,wrong-import-position
    sys.exit(main())
