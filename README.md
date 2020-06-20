# brand_constellation
Code to download IG post and profile data using IP rotation.

### Required Packages
1. Modified version of instaloader [here](https://github.com/kzCassie/instaloader.git@27340f87be2bb2efa9e04090952aa4d7755bea58#egg=instaloader "GitHub Repo").
2. Redis ProxyPool [reference here](https://github.com/Python3WebSpider/ProxyPool "ProxyPool").

### File Structure
* code
  * driver.py - fetch proxy IP from Redis, set system proxy and drive the post/profile crawler
* input
  * influencer.csv - list of (index, ig_account)
* data (auto generated folder)
* log (auto generated folder)
* _linux_cmd.txt: Useful conmmands when setting up AWS/Azure_
* _main_cron.py: script driven by cron job to sent regular commands to AWS EC2_
* _sftp_script.sh: template shell script to ssh/sftp to remote servers in batch_
* _requirements.txt: pip virtual environment requirements_

### Updates
20200620 - Improve code structure, error handling and and file writing

___
### Todos
1. Add other methods to get more proxy
2. Maintain usable proxy IPs:
	- Update proxypool by removing proxies that are not valid for fetching IG data.
	- Keep proxy that are usable, avoid using them too frequently