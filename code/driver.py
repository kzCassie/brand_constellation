''' Rotates IP and drives post.py, profile.py to collect IG data. '''
import redis
import random
import sys
import os

from instaloader.exceptions import TooManyRequestsException, ProxyInvalidException
from requests.exceptions import ProxyError

from global_variables import *
from common import *
from post import post_crawler
from profile import profile_crawler


def get_proxy_from_pool():
    '''Get a ramdom proxy from the redis.
       The redis proxy pool is generated using the ProxyPool pkg
       <https://github.com/Python3WebSpider/ProxyPool>

       args:
       returns: a random usable proxy (score=100) from the redis proxy pool
       Exit the program if no proxy available.
    '''
    r       = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    IP_list = r.zrangebyscore("proxies:universal", 100, 100, withscores=True)
    
    if len(IP_list)==0:
        print("No usable proxy in the Redis database! Run ProxyPool pkg to generate usable proxies.")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    return IP_list[random.randint(0,len(IP_list)-1)][0]


def drive_post_crawler(vm_rank,vm_total):
    '''Sets system proxy and calls post_crawler in post.py'''
    make_dir()
    while True:
        try:
            # set system proxy
            proxy = get_proxy_from_pool()
            os.environ['http_proxy']  = "http://"  + proxy 
            os.environ['HTTP_PROXY']  = "http://"  + proxy
            os.environ['https_proxy'] = "https://" + proxy
            os.environ['HTTPS_PROXY'] = "https://" + proxy
            write_log("Switching to system proxy: "+proxy,POST_LOG)
            print("Using system proxy: " + proxy)
            # call post_crawler
            post_crawler(vm_rank, vm_total)
            write_proxy_log(proxy,"Valid",PROXY_LOG)
        except (TooManyRequestsException) as e:
            write_proxy_log(proxy,"TooManyRequestsException",PROXY_LOG)
            print("TooManyRequestsException caught by driver.py")
        except ProxyInvalidException as e:
            write_proxy_log(proxy,"ProxyInvalidException",PROXY_LOG)
            print("ProxyInvalidException caught by driver.py")

def drive_profile_crawler(vm_rank,vm_total):
    '''Sets system proxy and calls profile_crawler in profile.py'''
    make_dir()
    while True:
        try:
            # set system proxy
            proxy = get_proxy_from_pool()
            os.environ['http_proxy']  = "http://"  + proxy 
            os.environ['HTTP_PROXY']  = "http://"  + proxy
            os.environ['https_proxy'] = "https://" + proxy
            os.environ['HTTPS_PROXY'] = "https://" + proxy
            write_log("Switching to system proxy: "+proxy,PROF_LOG)
            print("Using system proxy: " + proxy)
            # call profile_crawler
            profile_crawler(vm_rank, vm_total)
            write_proxy_log(proxy,"Valid",PROXY_LOG)
        except (TooManyRequestsException) as e:
            write_proxy_log(proxy,"TooManyRequestsException",PROXY_LOG)
            print("TooManyRequestsException caught by driver.py")
        except ProxyInvalidException as e:
            write_proxy_log(proxy,"ProxyInvalidException",PROXY_LOG)
            print("ProxyInvalidException caught by driver.py")


if __name__ == '__main__':
    TASK     = sys.argv[1]
    VM_RANK  = int(sys.argv[2])
    VM_TOTAL = int(sys.argv[3])

    '''
    VM_RANK  - The rank number of *this* machine among all (remote) machines
    VM_TOTAL - Total number of (remote) machines crawling using the INPUT.csv
    For example, if VM_TOTAL = 20 and VM_RANK = 3, then this machine will only crawl
                data for influencers with idx 3, 23, 43, 63, ...
    '''
    if TASK == 'post':
        drive_post_crawler(VM_RANK,VM_TOTAL)
    elif TASK == 'profile':
        drive_profile_crawler(VM_RANK,VM_TOTAL)




