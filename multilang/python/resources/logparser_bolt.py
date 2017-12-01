#!/bin/env python2.7
from __future__ import print_function

import os
import dependencies
import re
import storm
import pystorm
import logging
import logging.config
logging.config.fileConfig('logger.conf')
logger = logging.getLogger()

class LogParserBolt(storm.BasicBolt):
    def process(self, tup):
        words = tup.values[0].split('.')
        cluster, user, job, task, pid, cpu, mem = tuple(words)
        storm.emit(words)
        logger.info(
            'cluster:%s,username:%s,jobname:%s,taskname:%s,pid:%s,cpu:%s,mem:%s',
            cluster, user, job, task, pid,cpu, mem
        )

LogParserBolt().run()
