# -*- coding: utf-8 -*-

### the tools is used to create all the ggg2020 plots
### in generall， we have the outputs for each spectrum and also the all results for a bunch of data

 
import matplotlib.pyplot as plt
from numpy import *
import logging,os,glob
import xarray as xr
import datetime as dt
from ftir.tools import sort_data_by_timeunit
from bira3.ch4_sodan import get_collocated_data
import ftir.trend as tr
from ftir.val.cams import divar
import datetime
from matplotlib.dates import DateFormatter


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


def plot_vav_co2(f,M):

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
  ax.plot(mtime[xco2_err<3],xco2[xco2_err<3],'ko',label='Maido W4790')
  ax.plot(M['TCCON']['time'],M['TCCON']['xco2_ppm'],'ro',label='Stdenis TCCON')
  ax.legend()
  ax.set_ylabel('XCO2 [ppm]')
  #ax.set_xlim(dt.datetime(2013,1,1),dt.datetime(2015,1,1))
  
  indx = where((M['TCCON']['time']>dt.datetime(2013,1,1))&(M['TCCON']['time']<dt.datetime(2015,1,1)))
  
  ## box plot
  fig1, ax1 = plt.subplots()
  ax1.set_title('Box plot')
  data=[xco2[xco2_err<3], M['TCCON']['xco2_ppm'][indx]]
  ax1.boxplot(data)
  ax1.set_xticklabels(['Maido','Stdenis'])
  
  ## dirunal variation
  fig=plt.figure(figsize=(6,5))
  ax=fig.add_axes([0.12,0.1,0.8,0.8])
  color='r'
  t_f,d_f,d_f_std,diff_f,diff_f_std,idx_f=divar(M['TCCON']['time'][indx],M['TCCON']['xco2_ppm'][indx])
  t_f = [datetime.datetime(2000,1,1)+x for x in array(t_f) - datetime.datetime(t_f[0].year,t_f[0].month,t_f[0].day)]
  temp=diff_f[-4:];diff_f[4:]=diff_f[0:-4] ;diff_f[0:4]=temp
  temp=diff_f_std[-4:];diff_f_std[4:]=diff_f_std[0:-4] ;diff_f_std[0:4]=temp
  ax.plot(t_f,diff_f,color=color,linewidth=2.5);ax.fill_between(t_f,diff_f-diff_f_std,diff_f+diff_f_std,where=~isnan(diff_f_std),alpha=0.7,facecolor=color,color=color,label='Stdenis')
  color='k'
  t_f,d_f,d_f_std,diff_f,diff_f_std,idx_f=divar(mtime[xco2_err<3],xco2[xco2_err<3])
  t_f = [datetime.datetime(2000,1,1)+x for x in array(t_f) - datetime.datetime(t_f[0].year,t_f[0].month,t_f[0].day)]
  temp=diff_f[-4:];diff_f[4:]=diff_f[0:-4] ;diff_f[0:4]=temp
  temp=diff_f_std[-4:];diff_f_std[4:]=diff_f_std[0:-4] ;diff_f_std[0:4]=temp
  ax.plot(t_f,diff_f,color=color,linewidth=2.5);ax.fill_between(t_f,diff_f-diff_f_std,diff_f+diff_f_std,where=~isnan(diff_f_std),alpha=0.7,facecolor=color,color=color,label='Maido')
  ax.set_ylabel('$\Delta$ XCO2 [ppm]')
  ax.legend()
  
  ## relationship
  t1,data1,std1=sort_data_by_timeunit(mtime[xco2_err<3],xco2[xco2_err<3],day=1,unit='days',std=True)  #'hours'
  t2,data2,std2=sort_data_by_timeunit(M['TCCON']['time'][indx],M['TCCON']['xco2_ppm'][indx],day=1,unit='days',std=True)  
  t,d1,d2,s1,s2 = get_collocated_data(t1,t2,data1,data2,std1=std1,std2=std2)  
  tr.plot_scatters(d2,d1,s2,s1,figsize=(4,3),title='',xlabel='Stdenis XCO2 [ppm]',ylabel='Maido XCO2 [ppm]',onetooneflag=True,color='k',same_scale=True,crosszero=True)        

   
def plot_vav_co2_zco2(fs):
  badlist = '/bira-iasb/projects/FTIR/retrievals/data/i2s/stdenis/bruker125hr/tccon_dual/badlist'
  badspec = [x.strip() for x in open(badlist,'r').readlines()]
  print (badspec)
  if type(fs) == str: fs = [fs,]
  mtime=[]; 
  air =[]; zco2=[]; zco2_err=[];co2=[]; co2_err=[];o2=[]; o2_err=[];names=[]
  for f in fs:
    fid = open(f,'r')
    for il,line in enumerate(fid): 
      if il <=6: continue
      elif il ==7:
        keys = line.split()
        indx_zco2 = keys.index('zco2')
        indx_air = keys.index('luft')
        indx_co2 = keys.index('co2')
        indx_o2 = keys.index('o2')
      else:
        if line.split()[0] in badspec: continue
        mtime.append(dt.datetime(int(float(line.split()[1])),1,1)+dt.timedelta(float(line.split()[2])-1))
        zco2.append(float(line.split()[indx_zco2]))
        zco2_err.append(float(line.split()[indx_zco2+1]))    
        co2.append(float(line.split()[indx_co2]))
        co2_err.append(float(line.split()[indx_co2+1]))    
        o2.append(float(line.split()[indx_o2]))
        o2_err.append(float(line.split()[indx_o2+1]))     
        air.append(float(line.split()[indx_air]))
        names.append(line.split()[0])
  fid.close()
  fig=plt.figure(figsize=(8,4))
  ax=plt.axes([.15,0.15,.75,.7])
  xzco2 = array(zco2)/array(air)*1e6
  xzco2_err = array(zco2_err)/array(air)*1e6
  xco2 = array(co2)/array(air)*1e6
  xco2_err = array(co2_err)/array(air)*1e6
  xco2_o2 = array(co2)/array(o2)*1e6*0.2095
  xo2_err = array(o2_err)/array(air)
  mtime = array(mtime)
  ax.plot(mtime,xzco2,'ko',label='W4790')
  #ax.plot(mtime[xco2_err<5],xco2[xco2_err<5],'ro',label='W6300')  
  line, = ax.plot(mtime,xco2,'ro',label='W6300',picker=5)  
  ax.legend()
  ax.set_ylabel('XCO2 [ppm]')


  def onpick(event):
      if event.artist!=line: return True
      N = len(event.ind)
      if not N: return True
      for subplotnum, dataind in enumerate(event.ind):
        print (names[dataind])
      return True
  def click(event):
    print (event.xdata)

  fig.canvas.mpl_connect('pick_event', onpick)
  #fig.canvas.mpl_connect('button_release_event', click) 
  ## box plot
  fig1, ax1 = plt.subplots()
  ax1.set_title('Box plot')
  data=[xzco2,xco2]
  ax1.boxplot(data)
  ax1.set_xticklabels(['w4790','w6300'])
  
  ## dirunal variation
  fig=plt.figure(figsize=(6,5))
  ax=fig.add_axes([0.12,0.1,0.8,0.8])
  color='k'
  t_f,d_f,d_f_std,diff_f,diff_f_std,idx_f=divar(mtime, xzco2,fine_unit='minutes:20',coarse_threshold=250)
  t_f = [datetime.datetime(2000,1,1)+x for x in array(t_f) - datetime.datetime(t_f[0].year,t_f[0].month,t_f[0].day)]
  ax.plot(t_f,diff_f,color=color,linewidth=2.5);ax.fill_between(t_f,diff_f-diff_f_std,diff_f+diff_f_std,where=~isnan(diff_f_std),alpha=0.7,facecolor=color,color=color,label='w4790')
  color='r'
  t_f,d_f,d_f_std,diff_f,diff_f_std,idx_f=divar(mtime, xco2,fine_unit='minutes:20',coarse_threshold=250)
  t_f = [datetime.datetime(2000,1,1)+x for x in array(t_f) - datetime.datetime(t_f[0].year,t_f[0].month,t_f[0].day)]
  ax.plot(t_f,diff_f,color=color,linewidth=2.5);ax.fill_between(t_f,diff_f-diff_f_std,diff_f+diff_f_std,where=~isnan(diff_f_std),alpha=0.7,facecolor=color,color=color,label='w6300')
  myFmt = DateFormatter("%H")
  ax.xaxis.set_major_formatter(myFmt)
  ax.set_xlabel('Hours (UTC)')
  
  ax.set_ylabel('$\Delta$ XCO2 [ppm]')
  ax.legend()
  
  ## relationship
  t1,data1,std1=sort_data_by_timeunit(mtime,xzco2,day=1,unit='days',std=True)  #'hours'
  t2,data2,std2=sort_data_by_timeunit(mtime,xco2,day=1,unit='days',std=True)  
  t,d1,d2,s1,s2 = get_collocated_data(t1,t2,data1,data2,std1=std1,std2=std2)  
  tr.plot_scatters(d2,d1,s2,s1,figsize=(4,3),title='',xlabel='w6300 XCO2 [ppm]',ylabel='w4790 XCO2 [ppm]',onetooneflag=True,color='k',same_scale=True,crosszero=True)        
  

