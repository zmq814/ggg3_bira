# -*- coding: utf-8 -*-

### the tools is used to create all the ggg2020 plots
### in generall， we have the outputs for each spectrum and also the all results for a bunch of data

 
import matplotlib.pyplot as plt
from numpy import *
import logging,os,glob

def create_ggg2020_retrieve_plot(f,plots=['spc']):
  
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
  ax=plt.axes([.15,0.15,.65,.55]);axdiff=plt.axes([.15,.725,.65,.17])
  axdiff.plot(out['Freq'],out['Tm']-out['Tc'],'k') 
  axdiff.set_ylabel('Obs-Cal'); axdiff.set_xticks([])
  for key in keys[4:]:
    ax.plot(out['Freq'],out[key],label=key) 
  ax.legend()
  ax.set_xlabel('Wavenumber $cm^{-1}$')
    #fig.savefig('/home/minqiang/projects/FTIR/retrievals/working/minqiang/ggg2020/maido/bruker125hr/20190420_20190421/plots/%s.png'%os.path.basename(f),dpi=300)