import instaloader
import time
import csv
import os
import sys
import signal
from datetime import datetime

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

def write_record(user_idx,user,cnt,status,recordfile):
    with open(recordfile,'a') as record:
        record.write(str(user_idx)+','+user+','+str(cnt)+','+status+'\n')

##############   HANDLER   ################
def signal_handler_brand(sig, frame):
    global BRAND_g
    global BRAND_IDX_g
    global IDX_g
    print('brand.py: SIGINT RECEIVED')
    print('Downloading Paused')
    # record the last successfully downloaded post
    with open(LOG,'a') as fp1:
        fp1.write('Downloading Paused\n')
    if (BRAND_g!=''):
        print("Paused at:"+str(BRAND_IDX_g)+','+BRAND_g+','+str(IDX_g))
        write_brpt(BRAND_IDX_g,BRAND_g,IDX_g,BR_PT)
    # exit the program
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

def ProfileNotExistsException_handler(user_idx,user,post_idx,e,logfile,failfile,recordfile):
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,'ProfileNotExistsException',failfile)
    write_record(user_idx,user,post_idx,'ProfileNotExistsException',recordfile)

def TooManyRequestsException_handler(user_idx,user,post_idx,e,logfile,failfile,recordfile):
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,'TooManyRequestsException',failfile)
    # exit the program
    print('exit,'+user+','+str(post_idx)+',TooManyRequestsException')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

def ConnectionException_handler(user_idx,user,post_idx,e,logfile,failfile,recordfile):
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,'ConnectionException',failfile)
    # exit the program
    print('exit,'+user+','+str(post_idx)+',ConnectionException')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

def InstaloaderException_handler(user_idx,user,post_idx,e,logfile,failfile,recordfile):
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,'InstaloaderException',failfile)
    # exit the program
    print('exit,'+user+','+str(post_idx)+',InstaloaderException')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

def OtherException_handler(user_idx,user,post_idx,e,logfile,failfile,recordfile):
    with open(logfile,'a') as fp:
        fp.write(str(user_idx)+','+user+','+str(post_idx)+','+str(e)+'\n')
    write_fail(user_idx,user,post_idx,'Other Exception',failfile)

##############   DOWNLOADER   ################
def download_brand(L,brand_idx,brand_name,outputfile,logfile,brptfile,failfile,recordfile,idx=0):
    global BRAND_g
    global BRAND_IDX_g
    global IDX_g
    # global POST_NUM
    BRAND_g = brand_name
    BRAND_IDX_g = brand_idx
    IDX_g = idx
    cnt=0
    idx = max(idx,0)        
    
    try:
        profile = instaloader.Profile.from_username(L.context,brand_name)
        
        for post in profile.get_tagged_posts():
            if idx>5000:
                break

            try:
                # only collect posts in 2019
                date_utc     = post.date_utc
                if (date_utc>=datetime(2020, 1, 1)):
                    continue
                elif (date_utc<datetime(2019, 1, 1)):
                    continue

                if cnt<idx:
                    cnt += 1
                else:
                    cnt += 1
                    username     = post.owner_username
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
                    location     = post.location # require logging in

                    with open(outputfile, 'a', encoding='utf-8', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([brand_idx, brand_name, cnt, 
                            username,timestamp, date_utc, shortcode, url, typename, 
                            caption, cap_tags, cap_mentions, tagged_usrs, 
                            likes_num, comments_num,
                            video_view_cnt, video_duration,location])    
                    
                    idx += 1
                    IDX_g += 1  # ! race condition might happen here
                    write_brpt(brand_idx, brand_name, idx, brptfile)
            except instaloader.exceptions.PostChangedException as e:
                pass
            except instaloader.exceptions.QueryReturnedBadRequestException as e:
                pass
            except instaloader.exceptions.QueryReturnedNotFoundException as e:
                pass
            except instaloader.exceptions.QueryReturnedForbiddenException as e:
                pass
            except instaloader.exceptions.BadResponseException as e:
                pass

        # keep record
        write_record(brand_idx,brand_name,idx,'Success',recordfile)

    except instaloader.exceptions.ProfileNotExistsException as e:
        ProfileNotExistsException_handler(brand_idx,brand_name,-1,e,logfile,failfile,recordfile)
    except instaloader.exceptions.TooManyRequestsException as e:
        TooManyRequestsException_handler(brand_idx,brand_name,cnt,e,logfile,failfile,recordfile)
    except instaloader.exceptions.ConnectionException as e:
        ConnectionException_handler(brand_idx,brand_name,cnt,e,logfile,failfile,recordfile)
    # except instaloader.exceptions.QueryReturnedBadRequestException as e:
    #     InstaloaderException_handler(brand_idx,brand_name,cnt,e,logfile,failfile,recordfile)
    # except instaloader.exceptions.QueryReturnedNotFoundException as e:
    #     InstaloaderException_handler(brand_idx,brand_name,cnt,e,logfile,failfile,recordfile)
    # except instaloader.exceptions.QueryReturnedForbiddenException as e:
    #     InstaloaderException_handler(brand_idx,brand_name,cnt,e,logfile,failfile,recordfile)
    # except instaloader.exceptions.BadResponseException as e:
    #     InstaloaderException_handler(brand_idx,brand_name,cnt,e,logfile,failfile,recordfile)
    # except Exception as e:
        # OtherException_handler(brand_idx,brand_name,cnt,e,logfile,failfile)

##############   MAIN   ################
def main():
    # initiation
    signal.signal(signal.SIGINT, signal_handler_brand)
    signal.signal(signal.SIGTERM, signal_handler_brand)

    make_dir()
    init_output_file(POST_FILE, POST_FIELD)
    init_output_file(POST_RECORD,['brand_idx', 'brand_name', 'post_idx','status'])
    L = instaloader.Instaloader()

    # find previous downloading breakpoint
    if (os.path.isfile(BR_PT)):
        with open(BR_PT, 'r') as fp:
            br_pt     = fp.read().split(',')
            brand_pre = br_pt[1]
            idx_pre   = int(br_pt[2])
        crawled = True  #whether the first row has been crawled
    else:
        crawled = False

    # resume downloading
    with open(INPUT, encoding='utf-8', newline='') as input_file:
        input_reader = csv.reader(input_file)
        next(input_reader, None) #skip header

        for row in input_reader:
            brand_idx  = int(row[0])
            brand_name = row[1].strip()
            mod = brand_idx % VM_TOTAL
            is_rank_job = ((mod == VM_RANK) or (mod == 0 and VM_RANK == VM_TOTAL))

            if not crawled and is_rank_job:
                download_brand(L,brand_idx,brand_name,POST_FILE,LOG,BR_PT,FAIL_FILE,POST_RECORD)
            elif crawled and brand_name == brand_pre:
                with open(LOG,'a') as lf:
                    lf.write(brand_name+',partially crawled\n')
                if is_rank_job:
                    download_brand(L,brand_idx,brand_name,POST_FILE,LOG,BR_PT,FAIL_FILE,POST_RECORD,idx_pre)
                crawled = False


if __name__ == '__main__':
    # CONSTANTS
    VM_RANK = int(sys.argv[1])
    VM_TOTAL = int(sys.argv[2])
    INPUT  = "../input/brand.csv"
    LOG    = "../log/log3.log"
    BR_PT  = "../log/breakpoint3.txt"
    FAIL_FILE = "../log/fail3.txt"
    POST_RECORD = "../log/record3.csv"
    POST_FILE = "../data/brand.csv"
    POST_FIELD = ['brand_idx','brand_name','index','usr_name', 'time_collected', 'time_posted', 
                'shortcode', 'url', 'typename', 
                'caption', 'cap_tags', 'cap_mentions', 'tagged_usrs', 
                'likes_num', 'comments_num',
                'video_view_cnt', 'video_duration','location']

    # global var
    BRAND_g     = ''
    BRAND_IDX_g = 0
    IDX_g       = 0

    # execute
    main()

