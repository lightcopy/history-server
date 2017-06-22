#!/bin/bash

sbt compile -Dspark.eventLog.dir=work "runMain com.github.lightcopy.history.Server"
