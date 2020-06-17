import time
import csv
import os
import multiprocessing
import sys
import signal
from datetime import datetime

import instaloader
from instaloader.exceptions import *


from global_variables import USR_g,USR_IDX_g,IDX_g,ERR_CNT,ERR_MAX,\
    INPUT,POST_LOG,POST_BR_PT,POST_FAIL,POST_RECORD,POST_DATA,POST_FIELD 


##############    INIT    ################
def make_dir():
    try:
        os.mkdir('../data')
        print("Making Folder: data")
    except:
        pass
    
    try:
        os.mkdir('../log')
        print("Making Folder: log")
    except:
        pass

# def login_new(L):
#     global ACCOUNT_IDX
#     global POST_NUM
#     POST_NUM = 0
#     reached_end = False

#     with open(ACCOUNT, 'r') as pf:
#         reader = csv.reader(pf)
#         row = next(reader,None) #skip header
#         for i in range(ACCOUNT_IDX):
#             row = next(reader,None)
#             if row == None:
#                 reached_end = True
#                 break
    
#         if reached_end:
#             ACCOUNT_IDX = 1
#             pf.seek(0)
#             next(reader)
#             row = next(reader,None)
#             if row==None:
#                 print("Need to provide at least one login info!")
#                 sys.exit(0)

#         L.login(row[1],row[2])
#         ACCOUNT_IDX += 1
    
#     with open(LOG,'a') as logfile:
#         logfile.write('Logged in as: '+row[1]+','+row[2]+'\n')
#     print('Logged in as: '+row[1]+','+row[2])

# initiate the output file and set columns to fieldnames
def init_output_file(filepath,fieldnames):
    # initiate_output_file
    if not (os.path.isfile(filepath)):
        with open(filepath, 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

##############  FILE WRITER  ##################
def write_brpt(user_idx, user, post_idx,brptfile):
    with open(brptfile,'w') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+'\n')

def write_fail(user_idx,user,cnt,exception,failfile):
    with open(failfile,'a') as fail:
        fail.write(str(user_idx)+','+user+','+str(cnt)+','+exception+'\n')

##############   HANDLER   ################
def signal_handler_post(sig, frame):
    global USR_g
    global USR_IDX_g
    global IDX_g
    print('post.py: SIGINT RECEIVED')
    print('Downloading Paused')
    # record the last successfully downloaded post
    with open(POST_LOG,'a') as fp1:
        fp1.write('Downloading Paused\n')
    if (USR_g!=''):
        print("Paused at:"+USR_g+','+str(IDX_g))
        write_brpt(USR_IDX_g,USR_g,IDX_g,POST_BR_PT)
    # exit the program
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

def ProfileNotExistsException_handler(user_idx,user,post_idx,e,logfile,failfile):
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,'ProfileNotExistsException',failfile)

def ConnectionException_handler(user_idx,user,post_idx,e,logfile,failfile):
    # increment error count
    # global ERR_CNT
    # ERR_CNT+=1

    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,'ConnectionException',failfile)
    
    print('exit,'+user+','+str(post_idx)+',ConnectionException')
    # exit the program
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
    
    # try:
    #     print('try new log in...')
    #     login_new()
    #     return
    # except:
    #     print('exit,'+user+','+str(post_idx)+',ConnectionException')
    #     # exit the program
    #     try:
    #         sys.exit(0)
    #     except SystemExit:
    #         os._exit(0)

def OtherException_handler(user_idx,user,post_idx,e,logfile,failfile):
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,str(e),failfile)
    print('continue,'+user+','+str(post_idx)+','+str(e))
    
    # # increment error count
    # global ERR_CNT
    # ERR_CNT+=1

def finally_handler(user_idx,user,post_idx):
    global ERR_CNT
    global ERR_MAX
    global POST_NUM
    global LIMIT

    if (ERR_CNT>ERR_MAX):
        print("Too many errors! Exit.")
        with open(POST_LOG,'a') as fp:
            fp.write("too many errors! Exit.\n")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


##############   DOWNLOADER   ################
# download post data to outputfile. Record breakpoint info in brptfile
def download_post(L,user_idx,user,outputfile,logfile,brptfile,failfile,recordfile,idx=0):
    global USR_g
    global USR_IDX_g
    global IDX_g
    global POST_NUM
    USR_g = user
    USR_IDX_g = user_idx
    IDX_g = idx
    cnt=0
    idx = max(idx,0)
    
    try:
        profile = instaloader.Profile.from_username(L.context,user)
        
        for post in profile.get_posts():
            if cnt<idx:
                cnt += 1
            else:
                try:
                    timestamp    = datetime.now()
                    date_utc     = post.date_utc
                    shortcode    = post.shortcode
                    url          = post.url
                    typename     = post.typename

                    caption      = post.caption
                    cap_tags     = post.caption_hashtags
                    cap_mentions = post.caption_mentions
                    tagged_usrs  = post.tagged_users
                    likes_num    = post.likes
                    comments_num = post.comments   # including answers

                    video_view_cnt = post.video_view_count
                    video_duration = post.video_duration
                    location     = post.location # require logging in

                    # comment this out because want to collect all post data of brands -20200602
                    # if (date_utc<datetime(2019, 1, 1)):
                    #     with open(logfile,'a') as fp:
                    #         fp.write("Post exceeds time range, post time: "+str(date_utc))
                    #     break
                    # elif (date_utc>=datetime(2020, 1, 1)):
                    #     continue
                    
                    cnt += 1

                    with open(outputfile, 'a', encoding='utf-8', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([user_idx,user, cnt, timestamp, date_utc, 
                            shortcode, url, typename, 
                            caption, cap_tags, cap_mentions, tagged_usrs, 
                            likes_num, comments_num,
                            video_view_cnt, video_duration,location])    
                    idx += 1
                    IDX_g += 1  # ! race condition might happen here
                    
                    with open(logfile,'a') as fp:
                        fp.write(str(user_idx)+','+str(user)+',Success,'+str(idx)+'\n')

                    write_brpt(user_idx, user, idx, brptfile)
                    # POST_NUM += 1
                    # if POST_NUM>=LIMIT:
                    #     login_new()
                except (QueryReturnedBadRequestException,QueryReturnedForbiddenException,BadResponseException,PostChangedException,QueryReturnedNotFoundException) as e:
                    write_fail(user_idx,user,cnt,e.__class__.__name__,failfile)
                    cnt += 1
                    idx += 1
                    IDX_g += 1
        
        # finished all posts of this influencer
        with open(recordfile, 'a', encoding='utf-8', newline='') as rf:
            writer = csv.writer(rf)
            writer.writerow([user_idx, user, idx])
    
    except (TooManyRequestsException,ProxyInvalidException) as e:
        # IP banned, raise the error to the caller to rotate IP
        write_fail(user_idx,user,cnt,e.__class__.__name__,failfile)
        raise e
    except instaloader.exceptions.ProfileNotExistsException as e:
        ProfileNotExistsException_handler(user_idx,user,cnt,e,logfile,failfile)
    except instaloader.exceptions.ConnectionException as e:
        ConnectionException_handler(user_idx,user,cnt,e,logfile,failfile)
    except Exception as e:
        OtherException_handler(user_idx,user,cnt,e,logfile,failfile)
    # finally:
    #     finally_handler(user_idx,user,cnt)



def post_crawler(vm_rank, vm_total):
    # initiation
    signal.signal(signal.SIGINT, signal_handler_post)
    signal.signal(signal.SIGTERM, signal_handler_post)

    make_dir()
    init_output_file(POST_DATA, POST_FIELD)
    init_output_file(POST_RECORD,['user_idx', 'user', 'post_idx'])
    L = instaloader.Instaloader()
    # login_new(L)

    # find previous downloading breakpoint
    if (os.path.isfile(POST_BR_PT)):
        with open(POST_BR_PT, 'r') as fp:
            br_pt = fp.read().split(',')
            user_idx_pre = int(br_pt[0])
            user_pre = br_pt[1]
            idx_pre  = int(br_pt[2])
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

            if not crawled and is_rank_job:
                download_post(L,user_idx,user,POST_DATA,POST_LOG,POST_BR_PT,POST_FAIL,POST_RECORD)
            elif crawled and user_idx == user_idx_pre:
                with open(POST_LOG,'a') as lf:
                    lf.write(user+',partially crawled\n')
                if is_rank_job:
                    download_post(L,user_idx,user,POST_DATA,POST_LOG,POST_BR_PT,POST_FAIL,POST_RECORD,idx_pre)
                crawled = False



if __name__ == '__main__':
    # command line arguments
    VM_RANK = int(sys.argv[1])
    VM_TOTAL = int(sys.argv[2])    
    post_crawler(VM_RANK, VM_TOTAL)



