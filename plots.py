# -*- coding: utf-8 -*-

### the tools is used to create all the ggg2020 plots
### in generall， we have the outputs for each spectrum and also the all results for a bunch of data

 
import matplotlib.pyplot as plt
from numpy import *
import logging,os,glob
import xarray as xr
import datetime as dt


rootlogger=logging.getLogger(__name__)
logger=rootlogger

def create_ggg2020_retrieve_plot(f,plots=['spc','ak']):
  #fs=glob.glob('/bira-iasb/projects/FTIR/retrievals/operational/tools/ggg2020/spt/*20130412*')
  ### read f
  #for f in fs:
  fid = open(f,'r')
  out={}
  for i,line in enumerate(fid):
    if i<2: continue
    if i ==2: 
      keys= line.split(); 
      for key in keys: out[key]=[]
      continue
    if len(line.split())>2:
      for ik,key in enumerate(keys):
        out[key].append(line.split()[ik])
  for key in keys:
    out[key]=array(out[key],dtype=float)
  fid.close()
  
  fig=plt.figure(figsize=(8,4))
  ax=plt.axes([.15,0.15,.65,.55]);axdiff=plt.axes([.15,.71,.65,.17],sharex=ax)
  axdiff.plot(out['Freq'],out['Tm']-out['Tc'],'k') 
  axdiff.set_ylabel('Obs-Cal'); #axdiff.set_xticklabels([])
  for key in keys[4:]:
    ax.plot(out['Freq'],out[key],label=key) 
  ax.legend()
  ax.set_xlabel('Wavenumber $cm^{-1}$')
  axdiff.set_title(os.path.basename(f))
  plt.setp(axdiff.get_xticklabels(),visible=False)
    #fig.savefig('/home/minqiang/projects/FTIR/retrievals/working/minqiang/ggg2020/maido/bruker125hr/20190420_20190421/plots/%s.png'%os.path.basename(f),dpi=300)

def plot_ak(f,plots=['spc','ak']):
  fid = open(f,'r')
  kp=[]; lk=[]
  for i,line in enumerate(fid):
    if i == 0: nk, np, nm = [int(x) for x in line.split()]
    elif i <= np: kp.append(line.split());
    elif i == np+1: nk, nl = [int(x) for x in line.split()]
    elif i <= np+nl+1 : lk.append(line.split())
    else: break
  kp = array(kp,dtype=float)
  lk = array(lk,dtype=float)
  ak = kp.dot(lk.transpose())
  print (ak)



def diagnose_nc(fpath,logger=rootlogger):
  ### diagnose the ggg2020 outputs
  f = glob.glob(os.path.join(fpath,'*.nc'))[0]
  fid = xr.open_dataset(f)
  flag = fid['flag']
  logger.info('good retrieval is %d out of %d total retrieval'%(len(flag[flag==0]),len(flag)))
  keys = [x  for x in fid.keys() if 'vsf' in x[0:3] and 'error' not in x]
  fig,axes=plt.subplots(len(keys),1,figsize=(5,10),sharex=True)
  for ik,(ax,key) in enumerate(zip(iter(axes),keys)):
    if ik ==0: ax.set_title('%d good out of %d total retrieval'%(len(flag[flag==0]),len(flag)))
    ax.plot(fid['time'][flag==0],fid[key][flag==0],'o')
    ax.set_ylabel(key)
  #ax.set_xticklabels(ax.get_xticklabels(),rotation=45)
  for label in ax.get_xticklabels():
    label.set_rotation(45)


  keys = [x  for x in fid.keys() if 'column' in x[0:7] and 'error' not in x]
  fig,axes=plt.subplots(len(keys),1,figsize=(5,10),sharex=True)
  for ik,(ax,key) in enumerate(zip(iter(axes),keys)):
    if ik ==0: ax.set_title('xgas before ada and scaling correction')
    ax.plot(fid['time'][flag==0],fid[key][flag==0]/fid['column_o2']*0.2095,'ko')
    ax.set_ylabel('x'+key.split('_')[1])
  #ax.set_xticklabels(ax.get_xticklabels(),rotation=45)
  for label in ax.get_xticklabels():
    label.set_rotation(45)


def plot_vav_co2(f):
  fid = open(f,'r')
  mtime=[]; 
  air =[]; co2=[]; co2_err=[]
  for il,line in enumerate(fid): 
    if il <=6: continue
    elif il ==7:
      keys = line.split()
      indx_co2 = keys.index('zco2')
      indx_air = keys.index('luft')
    else:
      mtime.append(dt.datetime(int(float(line.split()[1])),1,1)+dt.timedelta(float(line.split()[2])))
      co2.append(float(line.split()[indx_co2]))
      co2_err.append(float(line.split()[indx_co2+1]))     
      air.append(float(line.split()[indx_air]))
  fid.close()
  fig=plt.figure(figsize=(8,4))
  ax=plt.axes([.15,0.15,.75,.7])
  xco2 = array(co2)/array(air)*1e6
  xco2_err = array(co2_err)/array(air)*1e6
  mtime = array(mtime)
  ax.plot(mtime[xco2_err<4],xco2[xco2_err<4],'ko')
   


