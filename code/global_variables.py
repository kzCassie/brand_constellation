''' Define Global Variables '''

from os.path import dirname, realpath
path = dirname(dirname(realpath(__file__)))

# CONSTANTS #
USR_g = ''
USR_IDX_g = 0
IDX_g = 0
ERR_CNT = 0 
ERR_MAX = 5 # tolerance of errs

# INPUT INFLUENCER CSV #
INPUT       = f"{path}/input/20_brand_list.csv"

# POST CRAWLER GLOBAL VAR #
POST_LOG    = f"{path}/log/log1.log"
POST_BR_PT  = f"{path}/log/breakpoint1.txt"
POST_FAIL   = f"{path}/log/fail1.txt"
POST_RECORD = f"{path}/log/record1.csv"
POST_DATA   = f"{path}/data/post.csv"
POST_FIELD  = ['user_idx','usr_name', 'index', 'time_collected', 'time_posted', 
            'shortcode', 'url', 'typename', 
            'caption', 'cap_tags', 'cap_mentions', 'tagged_usrs', 
            'likes_num', 'comments_num',
            'video_view_cnt', 'video_duration','location']

# PROFILE CRAWLER GLOBAL VAR #
PROF_LOG    = f"{path}/log/log2.log"
PROF_BR_PT  = f"{path}/log/breakpoint2.txt"
PROF_FAIL   = f"{path}/log/fail2.txt"
PROF_DATA   = f"{path}/data/profile.csv"
PROF_FIELD  = ['user_idx','user', 'timestamp', 'is_private', 
            'mediacount', 'followers', 'followees', 'external_url',
            'biography', 'is_verified', 'fullname', 'profile_pic']

# CREDENTIALS OF REDIS #
REDIS_HOST  = '127.0.0.1'
REDIS_PORT  = 6379
REDIS_PASSWORD = ''


# redis host
# REDIS_HOST = env.str('REDIS_HOST', '127.0.0.1')
# REDIS_PORT = env.int('REDIS_PORT', 6379)
# REDIS_PASSWORD = env.str('REDIS_PASSWORD', None)



