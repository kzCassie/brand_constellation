'''
Additional helper functions that are not related to the main driver program.
'''
from sqlalchemy import create_engine
from os.path import dirname, realpath
import pandas as pd
path = dirname(dirname(realpath(__file__)))


# Remote DB Variables
DB_TYPE   = 'mysql'
DB_DRIVER = 'pymysql'
DB_USER   = 'admin'
DB_PASS   = '00000000'
DB_HOST   = 'brandconstellation.ca1myvebu09j.us-east-1.rds.amazonaws.com'
DB_PORT   = '3306'
DB_NAME   = 'brand_constellation'
SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s?charset=utf8mb4' \
        % (DB_TYPE, DB_DRIVER, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)


def get_influencer_list(query, path):
    '''
    Use the query to get the table from the db and save it as an influencer csv in path.
    Format of the influencer.csv:
        - first column: influencer index starting from "1"
        - second column: influencer official IG account name
    '''
    engine   = create_engine(SQLALCHEMY_DATABASE_URI)
    conn     = engine.connect()
    conn.execute("USE %s" % DB_NAME)

    df = pd.read_sql(query, conn)
    df = df.drop_duplicates(ignore_index=True)
    df.index+=1
    df.index.name = 'index'
    print(f"Retrieved DataFrame is of shape: {df.shape}")
    df.to_csv(path,index_label='index')


if __name__ == '__main__':
    sql = "SELECT c.ig_user_name\
            FROM actor as a join actor_association as b on a.actor_id = b.actor_id2\
                    join actor as c on b.actor_id1 = c.actor_id\
            WHERE a.ig_user_name in ('louisvuitton', 'guess', 'columbia1938',\
                                    'americaneagle', 'vetements_official', 'gap', \
                                    'carhartt', 'bape_us', 'ralphlauren', \
                                    'off____white', 'lululemon', 'abercrombie', \
                                    'urbanoutfitters', 'supremenewyork', 'palaceskateboards', \
                                    'nike', 'stussy', 'puma', 'underarmour', 'adidas')"

    get_influencer_list(sql,f"{path}/input/20brand_1st_round.csv")


    