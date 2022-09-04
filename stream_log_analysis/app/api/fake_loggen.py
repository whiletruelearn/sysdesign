#!/usr/bin/python
import time
import datetime
import pytz
import numpy
import random
import gzip
import zipfile
import sys
import argparse
from faker import Faker
from random import randrange
from tzlocal import get_localzone
local = get_localzone()
from clearml import Task
from clearml.automation import TaskScheduler
#todo:
# allow writing different patterns (Common Log, Apache Error log etc)
# log rotation


class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args: # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False


def generate_logs():
    faker = Faker()
    log_lines = 100
    file_prefix = "test"
    output_type  = 'LOG'
    log_format = "ELF"
    sleep_time=None
    timestr = time.strftime("%Y%m%d-%H%M%S")
    otime = datetime.datetime.now()

    outFileName = '/Users/whiletruelearn/projects/sysdesign/stream_log_analysis/data/access_log_'+timestr+'.log' 

    for case in switch(output_type):
        if case('LOG'):
            f = open(outFileName,'w')
            break
        if case('GZ'):
            f = gzip.open(outFileName+'.gz','w')
            break
        if case('CONSOLE'): pass
        if case():
            f = sys.stdout

    response=["200","404","500","301"]

    verb=["GET","POST","DELETE","PUT"]

    resources=["/list","/wp-content","/wp-admin","/explore","/search/tag/list","/app/main/posts","/posts/posts/explore","/apps/cart.jsp?appID="]

    ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]

    flag = True
    while (flag):
        if sleep_time:
            increment = datetime.timedelta(seconds=sleep_time)
        else:
            increment = datetime.timedelta(seconds=random.randint(30, 300))
        otime += increment

        ip = faker.ipv4()
        dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
        tz = datetime.datetime.now(local).strftime('%z')
        vrb = numpy.random.choice(verb,p=[0.6,0.1,0.1,0.2])

        uri = random.choice(resources)
        if uri.find("apps")>0:
            uri += str(random.randint(1000,10000))

        resp = numpy.random.choice(response,p=[0.9,0.04,0.02,0.04])
        byt = int(random.gauss(5000,50))
        referer = faker.uri()
        useragent = numpy.random.choice(ualist,p=[0.5,0.3,0.1,0.05,0.05] )()
        if log_format == "CLF":
            f.write('%s - - [%s %s] "%s %s HTTP/1.0" %s %s\n' % (ip,dt,tz,vrb,uri,resp,byt))
        elif log_format == "ELF": 
            f.write('%s - - [%s %s] "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (ip,dt,tz,vrb,uri,resp,byt,referer,useragent))
        f.flush()

        log_lines = log_lines - 1
        flag = False if log_lines == 0 else True
        if sleep_time:
            time.sleep(sleep_time)



scheduler = TaskScheduler()

scheduler.add_task(
    name='workdays mock job',
    schedule_function=generate_logs,
    minute=1,
    recurring=True,
)

#scheduler.start_remotely(queue='services')
scheduler.start()
