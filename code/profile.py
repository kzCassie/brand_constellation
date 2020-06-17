import instaloader
import time
import csv
import os
import multiprocessing
import sys
import signal
from datetime import datetime
from post import *

# download profile data to outputfiel
def download_profile(L,user_idx,user,outputfile,logfile,brptfile,failfile):
    global USR_g
    global USR_IDX_g
    USR_g = user
    USR_IDX_g = user_idx
    profile = instaloader.Profile.from_username(L.context,user)

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
    L.download_pic("../data/profile_pic/"+str(user_idx)+'_'+user, profile_pic, datetime.now(), filename_suffix=None, _attempt=1)

    with open(outputfile, 'a', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([user_idx, user, timestamp, is_private, 
            mediacount, followers, followees, external_url,
            biography, is_verified, fullname, profile_pic])
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+',Success\n')
    write_brpt(user_idx,user,0,brptfile)
    # time.sleep(1.5)


##############   HANDLER   ################
def signal_handler_profile(sig, frame):
    global USR_g
    global USR_IDX_g
    print('profile.py: SIGINT RECEIVED')
    print('Downloading Paused')
    # record the last successfully downloaded post
    with open(LOG,'a') as fp1:
        fp1.write('Downloading Paused\n')
    if (USR_g!=''):
        write_brpt(USR_IDX_g,USR_g,0,BR_PT)
    # exit the program
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)



def profile_crawler(vm_rank, vm_total):    
    # initiation
    signal.signal(signal.SIGINT, signal_handler_profile)
    signal.signal(signal.SIGTERM, signal_handler_profile)

    make_dir()
    init_output_file(PROFLE_FILE, PROFILE_FIELD)
    L = instaloader.Instaloader()
    try:
        os.mkdir('../data/profile_pic/')
        print("Making Folder: data/profile_pic")
    except:
        pass
    # login_new(L)

    # find previous downloading breakpoint
    user_pre = ''
    if (os.path.isfile(BR_PT)):
        with open(BR_PT, 'r') as fp:
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
                    download_profile(L,user_idx,user,PROFLE_FILE,LOG,BR_PT,FAIL_FILE)
                elif crawled and user == user_pre:
                    with open(LOG,'a') as lf:
                        lf.write("download paused at "+user+" last time. \n")
                    if is_rank_job:
                        download_profile(L,user_idx,user,PROFLE_FILE,LOG,BR_PT,FAIL_FILE)
                    crawled = False
            except instaloader.exceptions.ProfileNotExistsException as e:
                ProfileNotExistsException_handler(user_idx,user,-1,e,LOG,FAIL_FILE)
            except instaloader.exceptions.ConnectionException as e:
                ConnectionException_handler(user_idx,user,-1,e,LOG,FAIL_FILE)
            except Exception as e:
                OtherException_handler(user_idx,user,-1,e,LOG,FAIL_FILE)
            # finally:
            #     finally_handler(user_idx,user,-1)


if __name__ == '__main__':
    # CONSTANTS
    VM_RANK = int(sys.argv[1])
    VM_TOTAL = int(sys.argv[2])
    INPUT  = "../input/20_brand_list.csv"    # CHANGE INPUT FILE NAME
    ACCOUNT = "../input/account.csv"
    LOG    = "../log/log2.log"
    BR_PT  = "../log/breakpoint2.txt"
    FAIL_FILE = "../log/fail2.txt"
    PROFLE_FILE = "../data/profile.csv"
    PROFILE_FIELD = ['user_idx','user', 'timestamp', 'is_private', 
                'mediacount', 'followers', 'followees', 'external_url',
                'biography', 'is_verified', 'fullname', 'profile_pic']

    # global var
    USR_g = ''
    USR_IDX_g = 0
    ERR_CNT = 0 
    ERR_MAX = 5 # tolerance of errs

    # login info
    LIMIT = 300  #number of posts per login
    PROFILE_NUM = 0  #number of profiles that have been downloaded
    ACCOUNT_IDX = 2

    profile_crawler(VM_RANK,VM_TOTAL)

