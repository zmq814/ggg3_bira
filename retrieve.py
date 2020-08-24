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
from numpy import *
import ftir.opus as o
import multiprocessing
import subprocess
rootlogger=logging.getLogger(__name__)
from importlib import reload


#### BASIC FUNCTIONS AND CLASSES
def getlogger(logger,name):
  """Creates a new child of logger with name "name" """
  try: newlogger=logging.getLogger(logger.name+'.'+name)
  except AttributeError: newlogger=logger
  return newlogger;


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
  windows = gggconfig[instrument].get('windows',1)
  if  windows!=1:
    logger.info('The retrieval window is %s (not standard TCCON retrieval windows); are you sure?'%windows)
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

def get_spec_info(instrument,file_list,logger=rootlogger):
  """get the information based on the instrument and spectra name

  Arguments:
      instrument -- such as 'bruker125hr@xianghe'
      file_list --  create_filelist(...) 
  outputs:
      specinfo -- the OrderedDict where the information of the spectra are included
  """
  logger=getlogger(logger,'get_spec_info')
  instrument = instrument.lower()
  out = OrderedDict()
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  if 'barcos' in gggconfig[instrument]: 
    for specf in file_list:
      logger.debug('reading parameters from BARCOS file')
      basef = os.path.basename(specf)
      out[basef]={}
      basefmt = gggconfig[instrument]['specfmt']
      out[basef]['date'] = dt.datetime.strptime(re.findall('[0-9]+',basef.split('.')[0])[0],basefmt)
      for key in ('Pins','Tins','Hins','Pout','Tout','Hout', 'WSPD', 'WDIR','SNR'): 
        out[basef][key] = nan ## default values
      out[basef]['SIA'] = 2190.0 
      out[basef]['FVSI'] = 0.0074
      bfile = os.path.join(out[basef]['date'].strftime(gggconfig[instrument]['barcos']),out[basef]['date'].strftime('%Y%m%d.hdf'))
      if not os.path.isfile(bfile): logger.error('The barcos file is missing: %s'%bfile); return 1
      fid = h5py.File(bfile,'r')
      #print (fid['OpusFileOriginal'][...][0])
      specs = [x.decode('utf-8').strip().split('/')[-1] for x in fid['OpusFileOriginal'][...]]
      if not basef in specs: 
        logger.warning('%s is not in the barcos file'%basef); 
        _temp = o.Opus(specf); _temp.get_data()
        out[basef]['stime']= out[basef]['date']+(dt.datetime(1,1,1,*[int(x) for x in _temp.param[-1]['TIM'][0:8].split(':')])-dt.datetime(1,1,1))         
        out[basef]['LWN']= float(_temp.param[0]['LWN'])        
        continue
      indx = specs.index(basef)
      out[basef]['Pins'] = fid['Inst']['PRS'][...][indx]
      out[basef]['Pout'] = fid['Meteo/P']['Average'][...][indx]
      out[basef]['Tins'] = fid['Inst']['TSC'][...][indx]
      out[basef]['Tout'] = fid['Meteo/T']['Average'][indx]
      out[basef]['Hins'] = 0.0 ### no detector inside the instrument for the humidity ### TBD
      out[basef]['Hout'] = fid['Meteo/RelativeHumidity']['Average'][indx]
      out[basef]['WSPD'] = fid['Meteo/WindSpeed']['Average'][indx]  
      out[basef]['WDIR'] = fid['Meteo/WindDirection']['Average'][indx]        
      out[basef]['SIA'] = fid['Meteo/Sun']['Average'][indx]  
      out[basef]['FVSI'] = fid['Meteo/Sun']['StandardDeviation'][indx]/fid['Meteo/Sun']['Average'][indx]*1e-2
      out[basef]['stime']= out[basef]['date']+(dt.datetime(1,1,1,*fid['StartTime'][indx])-dt.datetime(1,1,1)) 
      out[basef]['SNR']= fid['Spectra']['SNR'][indx]
      out[basef]['LWN']= fid['Inst']['LWN'][indx]
  else:
   ####### TBD
    for specf in file_list:
      logger.info('reading parameters from other meteo file and so on ...')  
      basef = os.path.basename(specf)
      for key in ('Pins','Tins','Hins','Pout','Tout','Hout', 'WSPD', 'WDIR'): 
        out[basef][key] = nan ## default values
      out[basef]['SIA'] = 2190.0 
      out[basef]['FVSI'] = 0.0074
      out[basef]['LWN'] =15798.014
  #OrderedDict(sorted(out.items(),key=lambda x:x[1]['stime']))
  return OrderedDict(sorted(out.items(),key=lambda x:x[1]['stime']))


def i2s(instrument,stime=None,etime=None,npool=4,skipi2s=False,logger=rootlogger):
  """
  prepare all the input files before the i2s run and lauch the jobs
  
  
  Arguments:
      instrument -- such as 'bruker125hr@xianghe'
      stime  --  start time (datetime)
      etime  --  end time (datetime)
      npool  -- the number of pools for multiprocess
      skipi2s -- T/F skip the i2s process if you already run i2s before
  
  output:
      speclist ---  <the spectral list after i2s>
  
  """
  logger=getlogger(logger,'i2s')
  logger.info('RUNING i2S ..., and return the specinfo')
  instrument = instrument.lower()
  if not instrument in gggconfig: raise('The instrument %s is not defined in the ggg.config'%instrument); return 1
  spectype =gggconfig[instrument]['spectype']
  if not type(spectype)==str: spectype=''.join(spectype)
  i2stemp = os.path.join(gggconfig['ggg2020.config']['i2s_temp_folder'], gggconfig[instrument]['i2s_temp'])
  if not os.path.join(i2stemp): logger.error('the i2s temp is not exsited: %s'%i2stemp); return 1
  outpath = os.path.join(gggconfig['ggg2020.config']['i2spath'],'i2s',instrument.split('@')[1],instrument.split('@')[0])
  if not os.path.isdir(outpath): subprocess.call('mkdir -p %s'%outpath, shell=True)
  lat = gggconfig[instrument]['lat']
  lon = gggconfig[instrument]['lon']
  alt = gggconfig[instrument]['alt']
  filelist = create_filelist(instrument, stime,etime)
  if not len(filelist): logger.warning('There is no spectra between %s and %s'%(stime,etime)); return 1
  subprocess.call('ln -sf %s %s'%(os.path.join(gggconfig['ggg2020.config']['i2s_temp_folder'],'flimit.i2s*%s'%pro), outpath),shell=True)
  listcommand = os.path.join(outpath,stime.strftime('%Y%m%d')+'_'+etime.strftime('%Y%m%d')+'_list_of_commands.txt')  
  i2s_in_days=[]
  MNYMXY_AC = gggconfig['i2s.input']['mnymxy_ac']
  MNYMXY_DC = gggconfig['i2s.input']['mnymxy_dc']
  specinfo=None
  os.chdir(outpath)
  if not skipi2s:
    ###  craete the i2s.in for each day
    days=0
    while stime+dt.timedelta(days) <= etime:
      mtime = stime+dt.timedelta(days)
      filelist = create_filelist(instrument, mtime)
      if not len(filelist): days += 1 ; continue
      specinfo = get_spec_info(instrument,filelist,logger=logger)
      ### read the template i2s input and generate a new one
      with open(i2stemp,'r+') as fid : lines  = fid.readlines()
      raw_data = os.path.dirname(filelist[0])+'/'
      i2s_spectra_output = './%s/'%(spectype)+mtime.strftime('%Y/%m')+'/' 
      if not os.path.isdir(i2s_spectra_output): subprocess.call('mkdir -p %s'%i2s_spectra_output, shell=True)
      i2s_in_daily = os.path.join(outpath,mtime.strftime('opus_i2s_%Y%m%d.in'))
      for folder in ('input','log','command'):
        if not os.path.isdir(os.path.join(outpath,folder)):subprocess.call('mkdir -p %s'%os.path.join(outpath,folder),shell=True)
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
    subprocess.call('mv *.in ./input',shell=True)
    subprocess.call('mv *.out ./log',shell=True)
    subprocess.call('mv *commands.txt ./command',shell=True)  
    logger.info('I2S has finished !')
    ### add the i2s folder to config/data_list
    i2s_spectra_output = os.path.abspath(i2s_spectra_output)+'/'
    with open(os.path.join(gggconfig['ggg2020.config']['gggpath'],'config/data_part_list_maker.lst'),'r') as f: 
      lines = ''.join(f.readlines())
      if i2s_spectra_output not in lines:
        with open(os.path.join(gggconfig['ggg2020.config']['gggpath'],'config/data_part_list_maker.lst'),'a+') as fid: 
          fid.write(i2s_spectra_output+'\n')
    with open(os.path.join(gggconfig['ggg2020.config']['gggpath'],'config/data_part.lst'),'r') as f: 
      lines = ''.join(f.readlines())
      if i2s_spectra_output not in lines:
        with open(os.path.join(gggconfig['ggg2020.config']['gggpath'],'config/data_part.lst'),'a+') as fid: 
          fid.write(i2s_spectra_output+'\n')
  ### return the speclist after i2s and the meteo info
  out = OrderedDict()
  while stime <= etime:  
    i2s_spectra_output = './%s/'%(spectype)+stime.strftime('%Y/%m')+'/' 
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
          out[basef] = specinfo[line.split()[0]]
          break
      fid.close()
      out[basef]['file']=os.path.abspath(f)
      #tmp = o.Opus(out[basef]['file']); tmp.get_data();
      out[basef]['fsf']=0.99999999
      out[basef]['lasf']=15798.014   ### fixed ? ### TBD
    stime += dt.timedelta(1)   
  return out
  
def create_mod(instrument,stime,etime,logger=rootlogger):
  """download the a priori profile from the caltech serve
  arguments:
      -instrument  such as 'bruker125hr@xianghe'
      -stime: the starttime dt.datetime formate
      -etime: the endtime dt.datetime formate
  outputs:
      -Nonthing   
  """
  logger=getlogger(logger,'create_mod')
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
    subprocess.call('wget --user=anonymous --password=mahesh.sha@aeronomie.be -i links.txt > /dev/null',shell=True)
    for gzfile in glob.glob('./*.tgz'):
      subprocess.call('tar zxvf %s'%gzfile,shell=True)
      subprocess.call('rm %s'%gzfile,shell=True)
    ## move these files to good position 
    commandstar('chmod 775 ./*.mod; mv ./*.mod $GGGPATH/models/gnd')
    commandstar('chmod 775 ./*.vmr; mv ./*.vmr $GGGPATH/vmrs/gnd')
    commandstar('cat links.txt >> finish.txt')
  elif 'maido' in instrument:
    ## although maido is not the standard TCCON site, we can use the stdenis a priori data for maido site
    subprocess.call('$GGGPATH/utils/python/list_mod_vmr_links ra %s %s > links.txt'%(stime.strftime('%Y%m%d'), etime.strftime('%Y%m%d')),shell=True)
    subprocess.call('wget --user=anonymous --password=mahesh.sha@aeronomie.be -i links.txt',shell=True)
    for gzfile in glob.glob('./*.tgz'):
      subprocess.call('tar zxvf %s'%gzfile,shell=True)
      subprocess.call('rm %s'%gzfile,shell=True)
    ## move these files to good position 
    commandstar('chmod 775 ./*.mod; mv ./*.mod $GGGPATH/models/gnd')
    commandstar('chmod 775 ./*.vmr; mv ./*.vmr $GGGPATH/vmrs/gnd')
    commandstar('cat links.txt >> finish.txt')
  else:
    ### the outside the TCCON community should download the GFIP a priori data yourself
    pass
    
def create_gop(instrument,speclist,stime,etime, logger=rootlogger):
  """
  create the gop file <be ready for sunrun>
  arguments:
      -instrument  such as 'bruker125hr@xianghe'
      -speclist  the dict of spectra after i2s (the key is the )  speclist=i2s(...)
      -stime: the starttime dt.datetime formate
      -etime: the endtime dt.datetime formate
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
  flist = list(speclist.keys())
  if not len(flist): logger.info('No spectra left after i2s'); return 1
  for i2spt in flist:
    ### load the meteo info from the i2s input file
    line='{:1s}{:57s}{:1s}{:2d}{:7d}{:1s}{:9.4f}{:10.4f}{:7.3f}{:6.1f}{:8.2f}{:6.1f}{:6.1f}{:8.2f}{:6.1f}{:7.1f}{:7.3f}{:6.1f}{:5.0f}{:1s}{:1s}{:6.0f}{:1s}{:6.0f}{:1s}{:11.8f}{:11.3f}{:6.0f}{:1s}{:7.3f}{:6.2f}'.\
    format('',os.path.basename(i2spt),'',2,0,'.', \
    gggconfig[instrument]['lat'],gggconfig[instrument]['lon'],float(gggconfig[instrument]['alt'])*1e-3, \
    speclist[i2spt]['Tins'],speclist[i2spt]['Pins'],speclist[i2spt]['Hins'], \
    speclist[i2spt]['Tout'],speclist[i2spt]['Pout'],speclist[i2spt]['Hout'], \
    speclist[i2spt]['SIA'],speclist[i2spt]['FVSI'],speclist[i2spt]['WSPD'], speclist[i2spt]['WDIR'],\
    '','',gggconfig[instrument]['nus'],'',gggconfig[instrument]['nue'],'',0.9999990,speclist[i2spt]['LWN'],9900.0,'.',0.002,1.0)
    with open(gopfile,'a') as f: f.write(line+'\n')   
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
  

def disable_spt():
  for f in glob.glob('*.ggg'):
    fid = open(f,'r')
    lines = fid.readlines()
    for il, line in enumerate(lines):
      if 'spt/z' in line: lines[il] =  lines[il].strip() +' 0\n'
    fid = open(f,'w')
    fid.writelines(lines)
    fid.close()    

def main(instrument='bruker125hr@xianghe',stime=None,etime=None,skipmod=False,skipi2s=False,npool=8,simulation=True,savespt=False,windows=1,logger=rootlogger):
  """run the gggcode
     arguments:
      -instrument  such as 'bruker125hr@xianghe'
      -npool: the number of core, suggest use 8-12 for the local machine
      -stime: the starttime dt.datetime formate
      -etime: the endtime dt.datetime formate
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
  windows = gggconfig[instrument].get('windows',windows)
  global gggpath, pro , lat, lon, alt
  gggpath = gggconfig['ggg2020.config']['gggpath']; pro = gggconfig[instrument]['pro']
  ### step 1: run I2S 
  speclist=i2s(instrument,stime,etime,npool=npool,skipi2s=skipi2s)
  if not len(list(speclist.keys())): logger.warning('no spectra found'); return 1
  ### step 2: prepare mod files ## a priori data
  if not skipmod: create_mod(instrument,stime,etime)
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
  ### disable the spectral output
  if not savespt: disable_spt()
  ### step 6: run gfit 
  if not simulation:
    logger.info('running GFIT ...')
    try: subprocess.call('parallel -j%i -t --delay 2 < multiggg.sh'%npool,shell=True)
    except KeyboardInterrupt: raise KeyboardInterruptError()
    logger.info('running post processing ...')
    subprocess.call('bash post_processing.sh',shell=True)
    

#if __name__ == '__main__':
