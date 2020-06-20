import time
import csv
import os
import sys
import signal
from datetime import datetime

import instaloader
from instaloader.exceptions import *

from global_variables import *


##############    INIT    ################
def make_dir():
    '''Make <data> and <log> if not exist.'''
    path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    try:
        os.mkdir(f"{path}/data")
        print("Making Folder: data")
    except:
        pass
    
    try:
        os.mkdir(f"{path}/log")
        print("Making Folder: log")
    except:
        pass

def init_output_file(filepath,fieldnames):
    '''initiate the output file and set columns to fieldnames'''
    if not (os.path.isfile(filepath)):
        with open(filepath, 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

##############  FILE WRITER  ##################
def write_brpt(user_idx, user, post_idx,brptfile):
    with open(brptfile,'w') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+'\n')

def write_fail(user_idx,user,post_idx,exception,failfile):
    with open(failfile,'a') as fail:
        fail.write(str(user_idx)+','+user+','+str(post_idx)+','+exception.__class__.__name__+','+str(exception)+'\n')

def write_record(user_idx,user,cnt,recordfile):
    # cnt: total number of posts collected for the user
    with open(recordfile, 'a', encoding='utf-8', newline='') as rf:
        writer = csv.writer(rf)
        writer.writerow([user_idx, user, cnt])

def write_log(text,logfile):
    with open(logfile,'a') as log:
        log.write(text+'\n')

def write_proxy_log(proxy,status,proxylogfile):
    with open(proxylogfile, 'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([proxy, status])

##############   HANDLER   ################
def signal_handler_post(sig, frame):
    print('post.py: SIGINT RECEIVED')
    print('Downloading Paused')
    # record the last successfully downloaded post
    write_log("Downloading Paused",POST_LOG)
    if (USR_g!=''):
        print("Paused at:"+USR_g+','+str(IDX_g))
        write_brpt(USR_IDX_g,USR_g,IDX_g,POST_BR_PT)
    # exit the program
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

def signal_handler_profile(sig, frame):
    print('profile.py: SIGINT RECEIVED')
    print('Downloading Paused')
    # record the last successfully downloaded post
    write_log("Downloading Paused",PROF_LOG)
    if (USR_g!=''):
        write_brpt(USR_IDX_g,USR_g,-1,PROF_BR_PT)
    # exit the program
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

def general_exception_handler(user_idx,user,post_idx,exception,logfile,failfile):
    write_log(str(user_idx)+','+user+','+str(post_idx)+','+str(exception),logfile)
    write_fail(user_idx,user,post_idx,exception,failfile)

def ConnectionException_handler(user_idx,user,post_idx,exception,logfile,failfile):
    write_log(str(user_idx)+','+user+','+str(post_idx)+','+str(exception),logfile)
    write_fail(user_idx,user,post_idx,exception,failfile)
    print('exit,'+str(user_idx)+','+user+','+str(post_idx)+',ConnectionException')
    # exit the program
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
    




