#!/usr/bin/env python
# -*- coding: utf-8 -*-
__all__=['']

import os,sys,configparser,imp,shutil
from numpy import *
import datetime
import datetime as dt
import logging,sys
import getpass
from collections import OrderedDict

#### BASIC LOGGING SETUP
logformat='%(asctime)s %(name)s %(levelname)-8s %(message)s'
logtfmt='%y-%m-%d %H:%M:%S'
#set the basis configuration for logging to the console 
logging.basicConfig(level=logging.INFO,format=logformat,datefmt=logtfmt)
rootlogger=logging.getLogger(__name__) #the root logger goes to the console and to stderr, use stream=sys.stdout to change this
testlogger=logging.getLogger(rootlogger.name+'.TEST');testlogger.setLevel(logging.DEBUG)

#### USER
try: user=getpass.getuser()
except: rootlogger.error('Could not determine the user from the getpass module... some functions may crash (the workdir is set to the current dir)');defaultworkdir='./';user='unknown'
else: defaultworkdir='/dev/shm/%s'%user #/home/bavol/projects/FTIR/tools/programs/sfit4/05.13/test2bavo'
if not os.path.isdir(defaultworkdir): os.makedirs(defaultworkdir)


#### LOAD USER CONFIG ######
configp=configparser.SafeConfigParser(dict_type=OrderedDict)
configfile=os.path.join(os.path.dirname(__file__),'ggg.config')
out=configp.read(configfile)
if len(out)==0 : rootlogger.error('Unable to load the data structures from %s'%configfile)
else: 
  configp._defaults=OrderedDict(configp.items('user'))
config={}
for section in configp.sections():
  for k,v in configp.items(section):
    if configp.has_option('user',k): continue
    try: config.setdefault(section,OrderedDict()).update({k:eval(v)})
    except Exception as e: config.setdefault(section,OrderedDict()).update({k:v});#print k,v,repr(e)


#add packages folders to sys.path
for k,f in list(config['packages'].items()):
  if not os.path.isdir(f): rootlogger.warning('Packages folder %s=%s does not exist'%(k,f))
  else: sys.path.append(f)

#if sys.version_info<(2,7):
  #from ordereddict import OrderedDict 
#else:
  #from ordereddict import OrderedDict 
  ##from collections import OrderedDict #for compatability with hpc ...

    
#### SPECIAL LOGGERS 
import logging.handlers


maillogger=logging.getLogger(rootlogger.name+'.MAIL');maillogger.setLevel(logging.INFO)
maillogger.addHandler(logging.handlers.SMTPHandler(mailhost=("smtp.oma.be", 25),
                                            fromaddr="%s@aeronomie.be"%user, 
                                            toaddrs="%s@aeronomie.be"%user,
                                            subject="FTIR troubles"))
oplogger=logging.getLogger(rootlogger.name+'.OPER');oplogger.setLevel(logging.WARNING)
errlogger=logging.getLogger(rootlogger.name+'.SCHED');errlogger.setLevel(logging.ERROR)

logging.addLevelName( logging.WARNING,  "\033[38;5;202m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
logging.addLevelName( logging.ERROR,    "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
logging.addLevelName( logging.CRITICAL, "\033[1;34m%s\033[1;0m" % logging.getLevelName(logging.CRITICAL))
logging.addLevelName( logging.DEBUG,    "\033[38;5;244m%s\033[1;0m" % logging.getLevelName(logging.DEBUG))



        
