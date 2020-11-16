#!/usr/bin/python
# -*- coding: utf-8 -*-
# the code is created by Minqiang Zhou @ 08-2020
# the code works with python 3.0+

from . import config as gggconfig
from . import user
import logging
import os, glob, sys
import datetime as dt
import datetime
import h5py,re
import py3progressbar as progressbar
from collections import OrderedDict
from ftir.sfit.tools import calibration
import ftir.tools as ft
import ftir.opus as o
from numpy import *
import multiprocessing
import subprocess
from importlib import reload
import fnmatch,time
import string
import random
import pandas as pd


#### BASIC FUNCTIONS AND CLASSES
def getlogger(logger,name):
  """Creates a new child of logger with name "name" """
  try: newlogger=logging.getLogger(logger.name+'.'+name)
  except AttributeError: newlogger=logger
  return newlogger;
  
#### BASIC LOGGING SETUP
logformat='%(asctime)s %(name)s %(levelname)-8s %(message)s'
logtfmt='%y-%m-%d %H:%M:%S'
#set the basis configuration for logging to the console 
logging.basicConfig(level=logging.INFO,format=logformat,datefmt=logtfmt)
rootlogger=logging.getLogger(__name__) #the root logger goes to the console and to stderr, use stream=sys.stdout to change this
testlogger=logging.getLogger(rootlogger.name+'.TEST');testlogger.setLevel(logging.DEBUG)

#### SPECIAL LOGGERS 
import logging.handlers



maillogger=logging.getLogger(rootlogger.name+'.MAIL');maillogger.setLevel(logging.INFO)
maillogger.addHandler(logging.handlers.SMTPHandler(mailhost=("smtp.oma.be", 25),
                                            fromaddr="%s@aeronomie.be"%user, 
                                            toaddrs="%s@aeronomie.be"%user,
                                            subject="ggg2020 job"))
oplogger=logging.getLogger(rootlogger.name+'.OPER');oplogger.setLevel(logging.WARNING)
errlogger=logging.getLogger(rootlogger.name+'.SCHED');errlogger.setLevel(logging.ERROR)

logging.addLevelName( logging.WARNING,  "\033[38;5;202m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
logging.addLevelName( logging.ERROR,    "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
logging.addLevelName( logging.CRITICAL, "\033[1;34m%s\033[1;0m" % logging.getLevelName(logging.CRITICAL))
logging.addLevelName( logging.DEBUG,    "\033[38;5;244m%s\033[1;0m" % logging.getLevelName(logging.DEBUG))

def applyfunc(f,args,keyargs=False):
  """For multiprocessing, executes a function with a list of arguments

  Input arguments::
    function to execute
    the list of arguments;;
  Optional input arguments: keyargs=False, when True, the last element in args should be a dictionary and will be passed to the function as key arguments"""
  if keyargs:
    kargs=dict([(k,v)  for k,v in list(args[-1].items())])
    args=args[:-1]
  else: kargs={}
  try: return f(*args,**kargs)
  except KeyboardInterrupt: raise KeyboardInterruptError()

def applyfuncstar(args):
  """For multiprocessing: dummy function to expand arguments"""
  return applyfunc(*args)

def commandstar(job):
  """For one command job run"""
  try: return subprocess.call(job,shell=True)
  except KeyboardInterrupt: raise KeyboardInterruptError()

def subProcRun(fname,quiet=True):
  """Open a subprocess and return all stdout (NCAR)

  Input argument: list of string (the command to execute and the arguments)"""
  rtn = subprocess.Popen(fname, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True )
  outstr = ''
  while True:
    out = rtn.stdout.read(1)
    if ( out == '' and rtn.poll() != None ):
      break
    if out != '':
      outstr += out
      if not quiet: sys.stdout.write(out)
      sys.stdout.flush()
  stdout, stderr = rtn.communicate()
  return (outstr,stderr);


################## all the subfunctions ##################

def check_strategy(instrument,logger=rootlogger):
  """check if all the requred files are avaiable before running the i2s and gggcode"""
  logger=getlogger(logger,'check_strategy')
  instrument = instrument.lower()
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  if not 'barcos' in gggconfig[instrument]: logger.info('The meteo data is not read from the barcos file')
  i2stemp = os.path.join(gggconfig['ggg2020.config']['i2s_temp_folder'],gggconfig[instrument]['i2s_temp'])
  if not os.path.isfile(i2stemp):
    logger.error('the I2S template is not existed: %s \n also please chech the flimit.??'%i2stemp)
    return 1
  #else:
  #  if os.path.join(gggconfig['ggg2020.config']['i2spath'],'data/i2s',instrument.split('@')[0],)
  #  commandstar()
  celltemp = os.path.join(gggconfig['ggg2020.config']['gggpath'],'cell_status_info',gggconfig[instrument]['pro']+'_cell_status_info.dat')
  if not os.path.isfile(celltemp):
    logger.error('the cell template is not existed: %s \n '%celltemp)
    return 1
  windows = gggconfig[instrument].get('windows',1)
  if  windows!=1:
    logger.info('The retrieval window is %s (not standard TCCON retrieval windows); are you sure?'%windows)
    time.sleep(5)
  return 0
  
def create_filelist(instrument, start_date, end_date=None):
  """ Create a filelist file for the specified site and date range
  If no optional end date is given, today is taken as the end date.

  Arguments:
      site -- name of the site for which to create the filelist
      start_date -- (datetime) starting date of the filelis

  Optional arguments:
      end_date -- end date of the file list (if none, the end_date = start_date + 1 day)

  Return value:
        The number of files in the filelist for the specified date range.
  """
  instrument = instrument.lower()
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  day = datetime.timedelta(days=1)
  if end_date is None: end_date = start_date
  file_list = []
  site_dir = gggconfig[instrument]['data']
  next_date = start_date
  while next_date <= end_date:
    archdir = next_date.strftime(site_dir)
    next_date += day
    if not os.path.exists(archdir): continue
    files = os.listdir(archdir)
    temp = []
    if type(gggconfig[instrument]['spectype'])==str: gggconfig[instrument]['spectype']=[gggconfig[instrument]['spectype'],]
    for item in files:
      for stype in list(gggconfig[instrument]['spectype']):
        if stype in item: temp.append(os.path.join(archdir, item) )
    if len(temp):
      _, temp = zip(*sorted(zip( [float(x.split('.')[-1]) for x in temp],temp)))
      file_list.extend(temp)
    del temp  
  return file_list 


def filterspectra(specinfo,spfilter=[],sublist=None,logger=rootlogger):
  if sublist==None: llist=list(specinfo.keys())
  else: llist=list(sublist)
  if not specinfo: logger.error('Provide a specinfo instance, from get_spec_info');return [],[]
  logger.debug('Using\n\tspfilter=%s'%','.join(list(zip(*spfilter))[1] if len(spfilter) else '-'))
  lllist=list(llist)
  badspec=[]
  for spec in llist:
    removespec=[]
    barcos=specinfo[spec]
    ### filter on spfilter  
    [removespec.append("bad value for %s"%(key)) for filterfunc,key in spfilter if isfinite(barcos[key]).any() and filterfunc(barcos[key])]
    if len(removespec)>0: lllist.remove(spec); badspec.append(spec)
  return lllist, badspec


def get_spec_info(instrument,file_list,specfilter=True,logger=rootlogger):
  """get the information based on the instrument and spectra name

  Arguments:
      instrument -- such as 'bruker125hr@xianghe'
      file_list --  create_filelist(...) 
  
  Optional arguments:
      specfilter -- T/F filter the spectra with the gggconfig['spectrum.filter.barocs'] or not
  
  outputs:
      specinfo -- the OrderedDict where the information of the spectra are included
  """
  logger=getlogger(logger,'get_spec_info')
  instrument = instrument.lower()
  specinfo = OrderedDict()
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  tcorr = gggconfig[instrument].get('tcorr',0)
  psource = gggconfig[instrument].get('pressure',None)
  if 'barcos' in gggconfig[instrument]: 
    for specf in file_list:
      logger.debug('reading parameters from BARCOS file')
      basef = os.path.basename(specf)
      specinfo[basef]={}
      basefmt = gggconfig[instrument]['specfmt']
      specinfo[basef]['date'] = dt.datetime.strptime(re.findall('[0-9]+',basef.split('.')[0])[0],basefmt)
      for key in ('Pins','Tins','Hins','Pout','Tout','Hout', 'WSPD', 'WDIR'): 
        specinfo[basef][key] = 0 ## default values
      for key in ('SNR',): 
        specinfo[basef][key] = nan ## default values  
      bfile = os.path.join(specinfo[basef]['date'].strftime(gggconfig[instrument]['barcos']),specinfo[basef]['date'].strftime('%Y%m%d.hdf'))
      if not os.path.isfile(bfile): logger.error('The barcos file is missing: %s'%bfile); del specinfo[basef]; continue
      fid = h5py.File(bfile,'r')
      specs = [x.decode('utf-8').strip().split('/')[-1] for x in fid['OpusFileOriginal'][...]]
      if not basef in specs: 
        logger.warning('%s is not in the barcos file'%basef); 
        _temp = o.Opus(specf); _temp.get_data()
        try:
          tblock = [x for x in _temp.param if 'TIM' in x][0]
          lblock = [x for x in _temp.param if 'LWM' in x][0]
          specinfo[basef]['stime']= specinfo[basef]['date']+(dt.datetime(1,1,1,*[int(x) for x in tblock['TIM'][0:8].split(':')])-dt.datetime(1,1,1))         
          specinfo[basef]['LWN']= float(lblock['LWN']) 
        except: continue   
        ### compute_snr(instrument)    
      indx = specs.index(basef)
      pflag = 'Meteo/P'
      if psource:
        _a=[x.strip().split(',') for x in psource.split(';')]
        pflag=list(filter(lambda x: x[0] <= specinfo[basef]['date'].strftime('%Y%m%d'), _a))[-1][-1].strip()
      specinfo[basef]['Pins'] = fid['Inst']['PRS'][...][indx]
      specinfo[basef]['Pout'] = fid[pflag]['Average'][...][indx]
      specinfo[basef]['Tins'] = fid['Inst']['TSC'][...][indx]
      specinfo[basef]['Tout'] = fid['Meteo/T']['Average'][indx]
      specinfo[basef]['Hins'] = 0.0 ### no detector inside the instrument for the humidity ### TBD
      specinfo[basef]['Hout'] = fid['Meteo/RelativeHumidity']['Average'][indx]
      specinfo[basef]['WSPD'] = fid['Meteo/WindSpeed']['Average'][indx]  
      specinfo[basef]['WDIR'] = fid['Meteo/WindDirection']['Average'][indx]        
      specinfo[basef]['SIA'] = fid['Meteo/Sun']['Average'][indx]  
      specinfo[basef]['FVSI'] = fid['Meteo/Sun']['StandardDeviation'][indx]/fid['Meteo/Sun']['Average'][indx]*1e2
      specinfo[basef]['DUR']= (dt.datetime(1,1,1,*fid['EndTime'][indx])-dt.datetime(1,1,1,*fid['StartTime'][indx])).seconds       
      specinfo[basef]['stime']= specinfo[basef]['date']+(dt.datetime(1,1,1,*fid['StartTime'][indx])-dt.datetime(1,1,1)) 
      specinfo[basef]['SNR']= fid['Spectra']['SNR'][indx]
      specinfo[basef]['LWN']= fid['Inst']['LWN'][indx]
      specinfo[basef]['Tcorr'] = tcorr+fid['CorrectTime'][...][indx] if 'CorrectTime' in fid else tcorr
      ## calibration surface pressure
      instrid=ft.determine_instrument(instrument=instrument)
      specinfo[basef]['Pout']=calibration(specinfo[basef]['date'],specinfo[basef]['Pout'],instrid['pressure_tccon'])
      for key in ('Pins','Tins','Hins','Pout','Tout','Hout', 'WSPD', 'WDIR'): 
        if not isfinite(specinfo[basef][key]): specinfo[basef][key] = 0 ## default values
      if not isfinite(specinfo[basef]['FVSI']):  specinfo[basef]['FVSI'] = 0.0074
      if not isfinite(specinfo[basef]['SIA']):  specinfo[basef]['SIA'] = 2190.0

  else:
   ####### TBD
    for specf in file_list:
      logger.info('reading parameters from other meteo file and so on ...')  
      basef = os.path.basename(specf)
      for key in ('Pins','Tins','Hins','Pout','Tout','Hout', 'WSPD', 'WDIR'): 
        specinfo[basef][key] = nan ## default values (for normal TCCON retrieval the Pout is very important!)
      specinfo[basef]['SIA'] = 2190.0 
      specinfo[basef]['FVSI'] = 0.0074
      specinfo[basef]['LWN'] =15798.014
      specinfo[basef]['Tcorr']=tcorr
  #OrderedDict(sorted(specinfo.items(),key=lambda x:x[1]['stime']))
  defspfilter=[]
  if specfilter:
    [defspfilter.extend(filterfuncs) for iln,filterfuncs in [x for x in list(gggconfig['spectrum.filter.barcos'].items()) if fnmatch.fnmatch((instrument).lower(),x[0])]]
    if len(defspfilter):
      logger.debug('the spectra is filtered with spectrum.filter.barcos')
      filterlist,badlist=filterspectra(specinfo,spfilter=defspfilter,logger=logger)
      try: filterlist,badlist=filterspectra(specinfo,spfilter=defspfilter,logger=logger)
      except Exception as e: logger.warning('No barcos data found: %s'%repr(e));badlist=[]
      else: trackingmask=array([s not in filterlist for s in specinfo],dtype=bool);#badlist=[]
      if len(badlist):
        for badspec in badlist: 
          del specinfo[badspec]
  return OrderedDict(sorted(specinfo.items(),key=lambda x:x[1]['stime']))


def _random_string(length):
  ### generate a random string
  return ''.join(random.choice(string.ascii_letters) for m in arange(length))
	
#def relocate():
  #### relocate the i2s spectra on yyyy/mm/dd structure
  #fs = glob.glob('/bira-iasb/projects/FTIR/retrievals/data/i2s/maido/bruker125hr/f7/*/*/ma*')
  ##fs = glob.glob('/bira-iasb/projects/FTIR/retrievals/data/i2s/stdenis/bruker125hr/tccon_dual/*/*/ra*')
  #for f in fs:
    #fname = os.path.basename(f); outpath = os.path.join(os.path.dirname(f),fname[8:10])
    #ftarget = os.path.join(outpath,fname)
    #if not os.path.isdir(outpath): commandstar('mkdir -p %s; chmod -R 775 %s'%(outpath,outpath))
    #commandstar('mv %s %s'%(f,ftarget))

def i2s(instrument,stime=None,etime=None,npool=4,skipi2s=False,filelist=None,logger=rootlogger):
  """
  prepare all the input files before the i2s run and lauch the jobs
  
  
  Arguments:
      instrument -- such as 'bruker125hr@xianghe'
      stime  --  start time (datetime)
      etime  --  end time (datetime)
      npool  -- the number of pools for multiprocess
      skipi2s -- T/F skip the i2s process if you already run i2s before
  
  Optional Arguments:
      filelist  -- OPUS file list before the i2s;  see def create_filelist
 
  output:
      speclist ---  <the spectral list after i2s>
  
  """
  logger=getlogger(logger,'i2s')
  logger.info('gather specinfo')
  instrument = instrument.lower()
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  spectype =gggconfig[instrument]['spectype']
  if not type(spectype)==str: spectype=''.join(spectype)
  i2stemp = os.path.join(gggconfig['ggg2020.config']['i2s_temp_folder'], gggconfig[instrument]['i2s_temp'])
  if not os.path.join(i2stemp): logger.error('the i2s temp is not exsited: %s'%i2stemp); return 1
  outpath = os.path.join(gggconfig['ggg2020.config']['i2spath'],'i2s',instrument.split('@')[1],instrument.split('@')[0])
  if not os.path.isdir(outpath): commandstar('mkdir -p %s; chmod -R 775 %s'%(outpath,outpath))
  lat = gggconfig[instrument]['lat']
  lon = gggconfig[instrument]['lon']
  alt = gggconfig[instrument]['alt']
  if not filelist : filelist = create_filelist(instrument, stime,etime) 
  if not len(filelist): logger.warning('There is no spectra between %s and %s'%(stime,etime)); return 1
  pro = gggconfig[instrument]['pro']
  subprocess.call('ln -sf %s %s'%(os.path.join(gggconfig['ggg2020.config']['i2s_temp_folder'],'flimit.i2s*%s'%pro), outpath),shell=True)
  listcommand = os.path.join(outpath,stime.strftime('%Y%m%d')+'_'+etime.strftime('%Y%m%d')+'_list_of_commands.txt')  
  i2s_in_days=[]
  MNYMXY_AC = gggconfig['i2s.input']['mnymxy_ac']
  MNYMXY_DC = gggconfig['i2s.input']['mnymxy_dc']
  specinfo=None
  os.chdir(outpath)

  if not skipi2s:
    ###  craete the i2s.in for each day <the preivous i2s spectra will be removed!>
    logger.info('Runing I2S ...')
    days=0
    while stime+dt.timedelta(days) <= etime:
      mtime = stime+dt.timedelta(days)
      filelist = create_filelist(instrument, mtime)
      if not len(filelist): days += 1 ; continue
      specinfo = get_spec_info(instrument,filelist,logger=logger)
      ### read the template i2s input and generate a new one
      with open(i2stemp,'r+') as fid : lines  = fid.readlines()
      raw_data = os.path.dirname(filelist[0])+'/'
      i2s_spectra_output = './%s/'%(spectype)+mtime.strftime('%Y/%m/%d')+'/' 
      i2s_spectra_output_year = './%s/'%(spectype)+mtime.strftime('%Y')+'/' 
      i2s_spectra_output_month = './%s/'%(spectype)+mtime.strftime('%Y/%m')+'/' 
      ### remove the previous spectra
      commandstar('rm -rf %s'%i2s_spectra_output)
      if not os.path.isdir(i2s_spectra_output_year): commandstar('mkdir -p %s; chmod -R 775 %s'%(i2s_spectra_output_year,i2s_spectra_output_year))
      if not os.path.isdir(i2s_spectra_output_month): commandstar('mkdir -p %s; chmod -R 775 %s'%(i2s_spectra_output_month,i2s_spectra_output_month))
      if not os.path.isdir(i2s_spectra_output): commandstar('mkdir -p %s; chmod -R 775 %s'%(i2s_spectra_output,i2s_spectra_output))
      i2s_in_daily = os.path.join(outpath,mtime.strftime('opus_i2s_%Y%m%d.in'))
      for folder in ('input','log','command'):
        if not os.path.isdir(os.path.join(outpath,folder)):commandstar('mkdir -p %s; chmod -R 775 %s'%(os.path.join(outpath,folder),os.path.join(outpath,folder)))
      nk =1
      i2s_in_days.append(i2s_in_daily)
      ifgtype='DC'
      for _a in gggconfig[instrument]['ifgtype'].split(';'):
        if mtime.strftime('%Y%m%d') > _a.split(',')[0].strip(): ifgtype = _a.split(',')[1].strip()
      if ifgtype=='DC': MNYMXY = MNYMXY_DC
      else: MNYMXY = MNYMXY_AC
      with open(i2s_in_daily,'w+') as f: 
        for line in lines:
          f.writelines(line.replace('<raw_data>',raw_data).replace('<i2s_spectra_output>',i2s_spectra_output).replace('<MNYMXY>',MNYMXY))
        for spec in list(specinfo.keys()):
          ### TBD add some filter? here or in the get_spec_info  --?
          basef = os.path.basename(spec)
          width=25;
          if len(basef)>=25: width=len(basef)+1
          temp = '{:%ss}{:6s}{:4s}{:2s}{:4.0f}{:8.3f} {:8.3f} {:6.1f}{:8.1f}{:9.3f}{:8.1f}{:8.1f}{:8.1f}{:8.1f}{:8.1f}{:9.4f}{:5.1f}{:7.1f}'%width
          line = temp \
                .format(basef,mtime.strftime('%Y'),mtime.strftime('%m'),mtime.strftime('%d') \
                ,nk,lat,lon,alt,
                specinfo[basef]['Tins'], specinfo[basef]['Pins'],specinfo[basef]['Hins'],
                specinfo[basef]['Tout'], specinfo[basef]['Pout'],specinfo[basef]['Hout'],    
                specinfo[basef]['SIA'],specinfo[basef]['FVSI'],specinfo[basef]['WSPD'],specinfo[basef]['WDIR'])
          nk += 2
          f.writelines(line+'\n')
      days += 1 ## each time one 
    ### go the the input folder and run i2s
    os.chdir(outpath)
    jobs=[]
    with open (listcommand,'w+') as f:
      for i2s_in_daily in i2s_in_days:
        f.write(os.path.join(gggconfig['ggg2020.config']['bin'],'i2s')+'  '+os.path.basename(i2s_in_daily)+' > '+i2s_in_daily+'.out'+'\n') #create cluster opus
        jobs.append(os.path.join(gggconfig['ggg2020.config']['bin'],'i2s') + ' '+os.path.basename(i2s_in_daily)+' > '+i2s_in_daily+'.out')
    logger.info('i2s running ... with npool = %i'%npool)
    pool=multiprocessing.Pool(processes=min(npool,len(jobs)))
    #print (jobs)
    results=pool.imap(commandstar,jobs);
    bar=progressbar.ProgressBar(widgets=[progressbar.Percentage(),' ---> I2S running for %s days|'%(len(i2s_in_days)),progressbar.SimpleProgress(),'|',progressbar.ETA()],maxval=len(i2s_in_days)).start()
    for i,job in enumerate(jobs): 
      try: next(results)
      except Exception as e: 
        logger.error('%s produced an exception: %s'%(job[1][0],str(e)));errid=1;
      except KeyboardInterrupt: 
        logger.error('Stop requested by user');pool.terminate();
        break
      finally: bar.update(i) 
    ###subprocess.call("parallel -j%s --will-cite < %s"%(npool,listcommand),shell=True)###output spectra data   
    ### mv all the related files to the corresponding folders
    days=0
    while stime+dt.timedelta(days) <= etime:
      mtime = stime+dt.timedelta(days)
      if len(glob.glob('*%s*.in'%mtime.strftime('%Y%m%d'))):
        subprocess.call('chmod 660 *%s*.in; mv *%s*.in ./input'%(mtime.strftime('%Y%m%d'),mtime.strftime('%Y%m%d')),shell=True)
        subprocess.call('chmod 660 *%s*.out; mv *%s*.out ./log'%(mtime.strftime('%Y%m%d'),mtime.strftime('%Y%m%d')),shell=True)
      days +=1
    subprocess.call('chmod 660 *%s*%s*commands.txt;mv *%s*%s*commands.txt ./command'%(stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d'),stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d')),shell=True)  
    logger.info('I2S has finished !')
    ### change the access
    days=0
    while stime+dt.timedelta(days) <= etime:
      mtime = stime+dt.timedelta(days)
      newfiles = glob.glob(os.path.join(i2s_spectra_output,mtime.strftime(gggconfig[instrument]['i2sfmt']))+'*')
      if len(newfiles): commandstar('chmod 660 %s'%(os.path.join(i2s_spectra_output,mtime.strftime(gggconfig[instrument]['i2sfmt']))+'*')) ### remove the previous spectra
      days += 1 ## each time one 
  ### return the speclist after i2s and the meteo info
  out = OrderedDict()
  while 1:
    tempstring= _random_string(5)
    speclinkfolder = os.path.join(gggconfig['ggg2020.config']['gggpath'],'config',tempstring)
    if not os.path.isdir(speclinkfolder): break
	
  speclogfile = os.path.join(gggconfig['ggg2020.config']['gggpath'],'config/data_part.lst') ## this is hard coded in the ggg2020
  while stime <= etime:  
    i2s_spectra_output = './%s/'%(spectype)+stime.strftime('%Y/%m/%d')+'/' 
    files = glob.glob(os.path.join(i2s_spectra_output,stime.strftime(gggconfig[instrument]['i2sfmt']))+'*')
    if not len(files): stime += dt.timedelta(1) ; continue
    filelist = create_filelist(instrument, stime) ## raw spectra
    infile = './input/'+stime.strftime('opus_i2s_%Y%m%d.in')
    specinfo = get_spec_info(instrument,filelist,logger=logger)

    for f in sorted(files):
      basef = os.path.basename(f)
      out[basef]={}
      nk = int(basef.split('.')[-1]) if mod(int(basef.split('.')[-1]),2) else int(basef.split('.')[-1])-1
      fid = open(infile,'r')
      for line in fid:
        if ':' in line: continue
        if len(line.split())<10 : continue
        if line.split()[4] == str(nk) and stime.strftime(gggconfig[instrument]['specfmt']) in line:
          for key in specinfo[line.split()[0]].keys(): 
            out[basef][key] = specinfo[line.split()[0]][key]
            
          break
      fid.close()
      out[basef]['file']=os.path.abspath(f)
      out[basef]['fsf']=0.99999999
      out[basef]['lasf']=15798.014   ### fixed ? ### TBD

    # ### add the i2s folder to config/data_list
    # i2s_spectra_output = os.path.dirname(out[basef]['file'])+'/'
    # with open(speclogfile,'r') as f: 
      # lines = ''.join(f.readlines())
      # if i2s_spectra_output not in lines:
        # with open(speclogfile,'a+') as f: 
          # f.write(i2s_spectra_output+'\n')
    stime += dt.timedelta(1)   
  if os.path.isdir(speclinkfolder): commandstar('rm -r %s'%(speclinkfolder))
  commandstar('mkdir -p %s; chmod -R 775 %s'%(speclinkfolder,speclinkfolder))
  for basef in out:
   commandstar('ln -s %s %s'%(out[basef]['file'],speclinkfolder))
  
  ### add the i2s folder to config/data_list
  i2s_spectra_output = speclinkfolder+'/'
  with open(speclogfile,'r') as f: 
    lines = ''.join(f.readlines())
    if i2s_spectra_output not in lines:
      with open(speclogfile,'a+') as f: 
        f.write(i2s_spectra_output+'\n')
  return out,speclinkfolder
  
def create_mod(instrument,stime,etime,quiet=True,logger=rootlogger):
  """download the a priori profile from the caltech serve
  arguments:
      -instrument  such as 'bruker125hr@xianghe'
      -stime: the starttime dt.datetime formate
      -etime: the endtime dt.datetime formate
  outputs:
      -Nonthing   
  """
  logger=getlogger(logger,'create_mod')
  logger.info('Downloading the FPIT models')
  wkdir = os.path.join(gggconfig['ggg2020.config']['modpath'],instrument.split('@')[1],'mod')
  if not os.path.isdir(wkdir): subprocess.call('mkdir -p %s'%wkdir,shell=True)
  os.chdir(wkdir)
  instrument = instrument.lower()
  etime += dt.timedelta(1)
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  site = gggconfig[instrument]['pro']
  if gggconfig[instrument].get('tcconsite',False):
    ### direct download from caltech
    subprocess.call('$GGGPATH/utils/python/list_mod_vmr_links %s %s %s > links.txt'%(site, stime.strftime('%Y%m%d'), etime.strftime('%Y%m%d')),shell=True)
    subprocess.call('wget %s --user=anonymous --password=mahesh.sha@aeronomie.be -i links.txt '%('--quiet' if quiet else ''),shell=True)
    for gzfile in glob.glob('./*.tgz'):
      subprocess.call('tar -zxf %s %s'%(gzfile, '>/dev/null' if quiet else ''),shell=True)
      subprocess.call('rm %s'%gzfile,shell=True)
    ## move these files to good position 
    commandstar('chmod 660 ./*.mod; mv ./*.mod $GGGPATH/models/gnd')
    commandstar('chmod 660 ./*.vmr; mv ./*.vmr $GGGPATH/vmrs/gnd')
    commandstar('cat links.txt >> finish.txt')
  elif 'maido' in instrument:
    ## although maido is not the standard TCCON site, we can use the stdenis a priori data for maido site
    subprocess.call('$GGGPATH/utils/python/list_mod_vmr_links ra %s %s > links.txt'%(stime.strftime('%Y%m%d'), etime.strftime('%Y%m%d')),shell=True)
    subprocess.call('wget %s --user=anonymous --password=mahesh.sha@aeronomie.be -i links.txt '%('--quiet' if quiet else ''),shell=True)
    for gzfile in glob.glob('./*.tgz'):
      subprocess.call('tar -zxf %s %s'%(gzfile, '>/dev/null' if quiet else ''),shell=True)
      subprocess.call('rm %s'%gzfile,shell=True)
    ## move these files to good position 
    commandstar('chmod 660 ./*.mod; mv ./*.mod $GGGPATH/models/gnd')
    commandstar('chmod 660 ./*.vmr; mv ./*.vmr $GGGPATH/vmrs/gnd')
    commandstar('cat links.txt >> finish.txt')
  else:
    ### the outside the TCCON community should download the GFIP a priori data yourself
    raise('you need to download the a priori profile by your self.')
    pass


def get_si_filter(instrument='bruker125hr@xianghe'):
  """ filter the spectra due to the Solar intensity 
  it is applied only for the Xianghe TCCON spectra before 31-05-2019 
  See Yang et al., 2020 ESSD
  """
  badfile = '/bira-iasb/projects/FTIR/retrievals/data/i2s/xianghe/bruker125hr/ingaas/badlist.log'
  if os.path.isfile(badfile): 
    fid = open(badfile, 'r')
    badlist = [x.strip() for x in fid.readlines()]
    fid.close()
  else:
    ## to create the badlist file ### for the first time
    badlist=[]
    speclist = i2s(instrument,stime=dt.datetime(2018,6,1),etime=dt.datetime(2019,5,31),npool=8,skipi2s=True,filelist=None)
    fid=open(badfile,'a+')
    for spec in list(speclist.keys()):
      ### load the measurement time of the i2s spectra
      _temp = o.Opus(speclist[spec]['file']); _temp.get_data()
      tblock = [x for x in _temp.param if 'TIM' in x][0]
      stime = dt.datetime.strptime(tblock['DAT']+tblock['TIM'][0:8],'%d/%m/%Y%H:%M:%S')
      tblock = [x for x in _temp.param if 'DUR' in x][0]
      etime = stime+dt.timedelta(seconds=tblock['DUR'])
      ### load the meteo solar intensity data
      mday = stime
      if stime.hour>=16: mday = stime + dt.timedelta(1)
      meteof = os.path.join(mday.strftime(gggconfig[instrument]['data']),'..',stime.strftime('%Y%m%dMeteo125HR.xls'))
      if not os.path.isfile(meteof): continue ## do nothing
      else:
        meteod = pd.read_csv(meteof,skiprows=lambda x: x in range(1,9),delimiter='\t',encoding= 'unicode_escape',usecols=['Time','Sun Direct'])
        shour = stime.strftime('%H:%M:%S'); ehour = etime.strftime('%H:%M:%S')
        if shour<ehour: indx = where((meteod['Time']>=shour)&(meteod['Time']<=ehour))[0]
        else: indx = where((meteod['Time']>=shour)|(meteod['Time']<=ehour))[0]
        solar = array([float(x.replace(',','.')) for x in meteod['Sun Direct'][indx]])
        if not len(solar) : continue
        if len(solar[solar<solar.max()*0.9]): 
          badlist.append(spec)
          fid.write('%s\n'%spec)
      del _temp, meteod
    fid.close()
  return badlist

def create_gop(instrument,speclist,stime,etime,sifilter=True, logger=rootlogger):
  """
  create the gop file <be ready for sunrun>
  arguments:
      -instrument  such as 'bruker125hr@xianghe'
      -speclist  the dict of spectra after i2s (the key is the )  speclist=i2s(...)
      -stime: the starttime dt.datetime formate
      -etime: the endtime dt.datetime formate
  optional arguments:
      -sifilter T/F if instrument = 'bruker125hr@xianghe' and mtime < 2020-06-01; we apply the filter based on the Solar Intensity
  outputs:
      -gopfile 
  """
  logger=getlogger(logger,'create_gop')
  logger.info('Create gop')
  instrument = instrument.lower()
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  gopfile=os.path.join(gggconfig['ggg2020.config']['gggpath'],'sunruns/gnd','%s%s_%s.gop'%(gggconfig[instrument]['pro'],stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d')))
  with open(gopfile,'w') as f:
    line0='{:10s}{:2d}{:10s}{:12d}'.format('',3,'',23) ### 3 23
    f.write(line0+'\n')
    line1=' create_sunrun     Version 2.14     2019-07-04     GCT  '
    f.write(line1+'\n')
    line2=' Spectrum_File_Name                     Obj   tcorr   oblat   oblon   obalt   tins   pins   hins  tout  pout   hout    sia    fvsi   wspd   wdir   Nus    Nue      FSF      lasf     wavtkr   AIPL   TM'
    f.write(line2+'\n')
  badlist = []
  if (instrument =='bruker125hr@xianghe') and sifilter and 'ingaas' in gggconfig[instrument]['spectype']: badlist = get_si_filter()
  flist = [x for x in list(speclist.keys()) if x not in badlist]
  if not len(flist): logger.info('No spectra left after i2s'); return 1
  for i2spt in flist:
    ### load the meteo info from the i2s input file
    line='{:1s}{:57s}{:1s}{:2d}{:7d}{:1s}{:9.4f}{:10.4f}{:7.3f}{:6.1f}{:8.2f}{:6.1f}{:6.1f}{:8.2f}{:6.1f}{:7.1f}{:7.3f}{:6.1f}{:5.0f}{:1s}{:1s}{:6.0f}{:1s}{:6.0f}{:1s}{:11.8f}{:11.3f}{:6.0f}{:1s}{:7.3f}{:6.2f}'.\
    format('',os.path.basename(i2spt),'',2,speclist[i2spt]['Tcorr'],'.', \
    gggconfig[instrument]['lat'],gggconfig[instrument]['lon'],float(gggconfig[instrument]['alt'])*1e-3, \
    speclist[i2spt]['Tins'],speclist[i2spt]['Pins'],speclist[i2spt]['Hins'], \
    speclist[i2spt]['Tout'],speclist[i2spt]['Pout'],speclist[i2spt]['Hout'], \
    speclist[i2spt]['SIA'],speclist[i2spt]['FVSI'],speclist[i2spt]['WSPD'], speclist[i2spt]['WDIR'],\
    '','',gggconfig[instrument]['nus'],'',gggconfig[instrument]['nue'],'',0.9999990,speclist[i2spt]['LWN'],9900.0,'.',0.002,1.0)
    with open(gopfile,'a') as f: f.write(line+'\n')   
  commandstar('chmod 660 %s'%gopfile)
  logger.info('finished gop created')
  return gopfile  

def run_grl(instrument='bruker125hr@xianghe',stime=None,etime=None,gopfile=None,windows=1):
  """
  run_grl (as the input before GGG2020; GFIT)
  arguments: 
      -instrument  such as 'bruker125hr@xianghe'
      -stime: the starttime dt.datetime formate
      -etime: the endtime dt.datetime formate
      -gopfile: the gop file create by the create_gop function
      -windows: the retrieval windows; default =1
           1 tccon.gnd             Standard TCCON windows
           2 hcl_cell.gnd          HCl cell windows only
           3 tccon_insb.gnd        Suggested InSb windows
           4 tccon_ingaas_insb.gnd Standard TCCON windows plus suggested InSb windows
           5 w4790.gnd             CO2 NDACC windows          
  """
  run_log_dirnm = os.path.join(gggpath,'runlogs/gnd/runlogs.men')
  commandstar("$GGGPATH/bin/create_runlog %s"%os.path.basename(gopfile))
  grlfile = pro+'%s_%s'%(stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d'))+'.grl'
  with open (os.path.join(gggpath,'runlogs/gnd',grlfile),'r') as f: 
    specnum=len(f.readlines())-4
  with open(run_log_dirnm,'w') as f: 
    f.write('Runlog          Description'+'\n')
    f.write(grlfile+'   '+str(specnum)+' spectra from '+instrument.split('@')[1]+'\n')
  with open(run_log_dirnm,'r') as f: k=len(f.readlines())-1 
  ####input for gsetup
  with open('gsetup.input','w')as f: f.write('g\n%s\n5\n%d\ny\n'%(str(k),windows))
  subprocess.call("$GGGPATH/bin/gsetup < gsetup.input",shell=True)
  
def _clean(stime,etime,pro,logger=rootlogger):
  ## clean the files after GFIT running
  logger=getlogger(logger,'_clean')
  logger.info('clean some not useful files')
  lsefile = os.path.join(gggpath,'lse/gnd','%s%s_%s.lse'%(pro,stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d')))
  if os.path.isfile(lsefile): commandstar('chmod 660 %s'%lsefile)
  grlfile = os.path.join(gggpath,'runlogs/gnd','%s%s_%s.grl'%(pro,stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d')))
  if os.path.isfile(grlfile): commandstar('chmod 660 %s'%grlfile)
  if os.path.isdir(speclinkfolder): commandstar('rm -r %s'%speclinkfolder)
  ### remove the link folder in the config/data_list
  speclogfile = os.path.join(gggconfig['ggg2020.config']['gggpath'],'config/data_part.lst') ## this is hard coded in the ggg2020
  fid = open(speclogfile,'r')  
  lines = fid.readlines()
  newlines=[x for x in lines if not speclinkfolder in x]
  fid.close()
  fid = open(speclogfile,'w')  
  fid.writelines(newlines)
  fid.close()

  

def change_gggfile(savespt=False):
  """
  optional arguments
    save the spt during thg gfit running or not
  """
  for f in glob.glob('*.ggg'):
    fid = open(f,'r')
    lines = fid.readlines()
    for il, line in enumerate(lines):
      if (not savespt) and ('spt/z' in line): lines[il] =  lines[il].strip() +' 0\n'
    fid = open(f,'w')
    fid.writelines(lines)
    fid.close()    


######## main function #######

def main(instrument,stime,etime,skipmod=False,skipi2s=False,npool=8,simulation=True, quiet=True,
    savespt=False,windows=1,logger=rootlogger):
  """run the gggcode
     arguments:
      -instrument  such as 'bruker125hr@xianghe'
      -npool: the number of core, suggest use 8-12 for the local machine
      -stime: the starttime dt.datetime format
      -etime: the endtime dt.datetime format
      -skipmod: skip the a priorprofile creation; if you already download the mode files
      -skipi2s: skip the i2s; if you already run the i2s before
      -simulation: to run the gfit or not
      -savespt: save the spt or not
      -windows: 
           1 tccon.gnd             Standard TCCON windows
           2 hcl_cell.gnd          HCl cell windows only
           3 tccon_insb.gnd        Suggested InSb windows
           4 tccon_ingaas_insb.gnd Standard TCCON windows plus suggested InSb windows
           5 w4790.gnd             CO2 NDACC windows      
           
  one example:
    import ggg3.retrieva as r
    import datetime as dt
    stime=dt.datetime(2019,11,14);etime=dt.datetime(2019,11,16); instrument='bruker125hr@stdenis'
    r.main(instrument,stime,etime,skipi2s=False,simulation=False,skipmod=False)

  """
  instrument = instrument.lower()
  if check_strategy(instrument): return 1
  if not windows: windows = gggconfig[instrument].get('windows',windows)
  if not windows: raise('ERROR: please set windows')
  global gggpath, pro , lat, lon, alt
  global speclinkfolder
  gggpath = gggconfig['ggg2020.config']['gggpath']; pro = gggconfig[instrument]['pro']
  ##logfile
  if quiet:
    logpath=os.path.join('/bira-iasb/projects/FTIR/retrievals/working',user,'ggg2020',instrument.split('@')[1],instrument.split('@')[0],'log')
    if not os.path.isdir(logpath): subprocess.call('mkdir -p %s'%logpath,shell=True)
    logfile=os.path.join(logpath,'log_%s_%s.txt'%(stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d')))
    ft.create_log_file(logger,logfile)
  ### step 1: run I2S 
  filelist = create_filelist(instrument, stime,etime)
  if not len(filelist): logger.warning('no spectra found'); return 1
  speclist,speclinkfolder=i2s(instrument,stime,etime,npool=npool,skipi2s=skipi2s)
  if not len(list(speclist.keys())): logger.warning('no i2s spectra found'); return 1
  ### step 2: prepare mod files ## a priori data
  if not skipmod: create_mod(instrument,stime,etime, quiet=quiet)
  ### step 3: prepare gop file
  gopfile = create_gop(instrument,speclist,stime,etime)
  logger.info('create gopfile %s'%gopfile)
  ### step 4: create work folder
  wkdir = os.path.join('/bira-iasb/projects/FTIR/retrievals/working',user,'ggg2020',instrument.split('@')[1],instrument.split('@')[0],'%s_%s'%(stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d')))
  if not os.path.isdir(wkdir): subprocess.call('mkdir -p %s'%wkdir,shell=True)
  os.chdir(wkdir)
  logger.info('wkdir = %s'%wkdir)
  ### step 5: run grl 
  run_grl(instrument,stime,etime,gopfile,windows)
  ### change the data_part and disable the spectral output (F/T)
  change_gggfile(savespt=savespt)
  
  ### step 6: run gfit 
  if not simulation:
    logger.info('running GFIT ...')
    STDOUT = '' if not quiet else '>> %s'%logfile 
    try: subprocess.call('parallel -j%i -t --delay 2 < multiggg.sh %s'%(npool, STDOUT),shell=True)
    except KeyboardInterrupt: raise KeyboardInterruptError()
    logger.info('running post processing ...')
    subprocess.call('bash post_processing.sh %s'%STDOUT,shell=True)
    output_filelist_dir= os.path.join(wkdir,'../filelists')
    if not os.path.isdir(output_filelist_dir): commandstar('mkdir -p %s; chmod -R 775 %s'%(output_filelist_dir,output_filelist_dir))
    output_filelist = os.path.join(output_filelist_dir,'filelist_%s_%s.txt'%(stime.strftime('%Y%m%d'),etime.strftime('%Y%m%d')))
    commandstar('chmod -R 775 %s'%(output_filelist))
    with open(output_filelist, 'w') as fid:
      fid.writelines('\n'.join(filelist)+'\n');
  _clean(stime,etime,pro)
  maillogger.info('%s - %s ggg2020 finished'%(stime.strftime('%Y%m%s'),etime.strftime('%Y%m%d')))

#if __name__ == '__main__':
  
