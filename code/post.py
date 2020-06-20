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
def download_post(L,user_idx,user,outputfile,logfile,brptfile,failfile,recordfile,
    idx=0,start_date=None,end_date=None):
    '''Download post data given user_idx and user IG account to outputfile.
        logfile    - log file
        brptfile   - file to record the breakpoint
        failfile   - file to record crawling failures
        recordfile - file to record total number of posts that have been crawled per the user
        idx        - the index of the user's posts to start crawling
        start_date - <"%Y-%m-%d"> starting date, from which posts will be collected.
        end_date   - <"%Y-%m-%d"> ending date of posts, starting from which the posts will 
                     not be collected. If None, then crawl posts up to now.
    '''
    USR_IDX_g = user_idx
    USR_g     = user
    IDX_g     = idx
    cnt=0
    
    try:
        profile = instaloader.Profile.from_username(L.context,user)
        
        for post in profile.get_posts():
            if cnt<idx:
                #skip reviously crawled posts
                cnt += 1
            else:
                try:
                    # check whether the post is within the targeted time range
                    date_utc     = post.date_utc
                    if start_date and (date_utc < datetime.strptime(start_date,"%Y-%m-%d")):
                        write_log("Post exceeds time range, post time: "+str(date_utc), logfile)
                        break
                    elif end_date and (date_utc >= datetime.strptime(end_date,"%Y-%m-%d")):
                        continue

                    timestamp    = datetime.now()
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
                    location       = post.location # require logging in

                    with open(outputfile, 'a', encoding='utf-8', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([user_idx,user, cnt, timestamp, date_utc, 
                            shortcode, url, typename, 
                            caption, cap_tags, cap_mentions, tagged_usrs, 
                            likes_num, comments_num,
                            video_view_cnt, video_duration,location])    
                    idx  += 1
                    IDX_g = idx  # ! race condition might happen here
                    
                    write_log(str(user_idx)+','+str(user)+',Success,'+str(idx), logfile)
                    write_brpt(user_idx, user, idx, brptfile)

                except (QueryReturnedBadRequestException,QueryReturnedForbiddenException,BadResponseException,PostChangedException,QueryReturnedNotFoundException) as e:
                    # ignore post level exceptions and keep going
                    write_fail(user_idx,user,idx,e,failfile)
                    idx  += 1
                    IDX_g = idx
        
        # record number of posts collected of this user
        write_record(user_idx,user,idx,recordfile)
    
    except (TooManyRequestsException,ProxyInvalidException) as e:
        # IP issues, raise the error to driver.py to rotate IP
        general_exception_handler(user_idx,user,idx,e,logfile,failfile)
        raise e
    except ConnectionException as e:
        ConnectionException_handler(user_idx,user,idx,e,logfile,failfile) #exit the program
    except (ProfileNotExistsException,Exception) as e:
        general_exception_handler(user_idx,user,idx,e,logfile,failfile)


def post_crawler(vm_rank, vm_total):
    # initiation
    signal.signal(signal.SIGINT, signal_handler_post)
    signal.signal(signal.SIGTERM, signal_handler_post)

    make_dir()
    init_output_file(POST_DATA, POST_FIELD)
    init_output_file(POST_RECORD,['user_idx', 'user', 'post_idx'])
    L = instaloader.Instaloader()

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
                download_post(L,user_idx,user,POST_DATA,POST_LOG,POST_BR_PT,POST_FAIL,POST_RECORD,0,START_DATE,END_DATE)
            elif crawled and user_idx == user_idx_pre:
                write_log(user+',partially crawled',POST_LOG)
                if is_rank_job:
                    download_post(L,user_idx,user,POST_DATA,POST_LOG,POST_BR_PT,POST_FAIL,POST_RECORD,idx_pre)
                crawled = False



if __name__ == '__main__':
    VM_RANK = int(sys.argv[1])
    VM_TOTAL = int(sys.argv[2])    
    post_crawler(VM_RANK, VM_TOTAL)



