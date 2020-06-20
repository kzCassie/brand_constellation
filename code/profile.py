import time
import csv
import os
import sys
import signal
from datetime import datetime

import instaloader
from instaloader.exceptions import *

from global_variables import *
from common import *

##############   DOWNLOADER   ################
def download_profile(L,user_idx,user,outputfile,logfile,brptfile,failfile):
    # download profile data to outputfie
    USR_IDX_g = user_idx
    USR_g     = user
    profile   = instaloader.Profile.from_username(L.context,user)

    timestamp    = datetime.now()
    is_private   = profile.is_private
    mediacount   = profile.mediacount
    followers    = profile.followers
    followees    = profile.followees
    external_url = profile.external_url
    biography    = profile.biography
    is_verified  = profile.is_verified
    fullname     = profile.full_name
    profile_pic  = profile.profile_pic_url
    L.download_pic(PROF_PIC+str(user_idx)+'_'+user, profile_pic, datetime.now(), filename_suffix=None, _attempt=1)

    with open(outputfile, 'a', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([user_idx, user, timestamp, is_private, 
            mediacount, followers, followees, external_url,
            biography, is_verified, fullname, profile_pic])

    write_log(str(user_idx)+','+user+',Success', logfile)
    write_brpt(user_idx, user, -1, brptfile)


def profile_crawler(vm_rank, vm_total):    
    # initiation
    signal.signal(signal.SIGINT, signal_handler_profile)
    signal.signal(signal.SIGTERM, signal_handler_profile)

    make_dir()
    init_output_file(PROF_DATA, PROF_FIELD)
    L = instaloader.Instaloader()
    try:
        os.mkdir(PROF_PIC)
        print("Making Folder: data/profile_pic")
    except:
        pass

    # find previous downloading breakpoint
    user_pre = ''
    if (os.path.isfile(PROF_BR_PT)):
        with open(PROF_BR_PT, 'r') as fp:
            br_pt = fp.read().split(',')
            user_pre = br_pt[1]
        crawled = True  #whether the first row has been crawled
    else:
        crawled = False

    # resume downloading
    with open(INPUT, encoding='utf-8', newline='') as input_file:
        input_reader = csv.reader(input_file)
        next(input_reader, None) #skip header

        for row in input_reader:
            user_idx = int(row[0])
            user = row[1]
            mod = user_idx % vm_total
            is_rank_job = ((mod == vm_rank) or (mod == 0 and vm_rank == vm_total))

            try:
                if not crawled and is_rank_job:
                    download_profile(L,user_idx,user,PROF_DATA,PROF_LOG,PROF_BR_PT,PROF_FAIL)
                elif crawled and user == user_pre:
                    write_log("download paused at "+user+" last time.",PROF_LOG)
                    if is_rank_job:
                        download_profile(L,user_idx,user,PROF_DATA,PROF_LOG,PROF_BR_PT,PROF_FAIL)
                    crawled = False

            except (TooManyRequestsException,ProxyInvalidException) as e:
                # IP banned, raise the error to the caller to rotate IP
                general_exception_handler(user_idx,user,-1,e,logfile,failfile)
                raise e
            except instaloader.exceptions.ConnectionException as e:
                ConnectionException_handler(user_idx,user,-1,e,logfile,failfile) #exit the program
            except (ProfileNotExistsException,Exception) as e:
                general_exception_handler(user_idx,user,-1,e,logfile,failfile)



if __name__ == '__main__':
    VM_RANK = int(sys.argv[1])
    VM_TOTAL = int(sys.argv[2])
    profile_crawler(VM_RANK,VM_TOTAL)

