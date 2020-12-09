from . import get_spark
import pyspark.sql.functions as F
from tqdm import tqdm
import pandas as pd
import numpy as np
import datetime

# 2008-07-27 is the first sunday before first post on SO
def add_weekid(D, col_in, col_out='WeekId', start_date=datetime.datetime(year=2008, month=7, day=27)):
    return D.withColumn(
        col_out,
        F.floor((
            F.col(col_in).cast('long') -
            F.unix_timestamp(F.lit(start_date)).cast('long')
	) / 604800) # seconds per week
    )

def get_spark_dataframes(data_dir, reps_from_votes):
    spark = get_spark('gen_timeseries', mem=16, cores=8)
    users = spark.read.format('json').load(f'{data_dir}/Users.json')
    posts = spark.read.format('json').load(f'{data_dir}/Posts.json')
    if reps_from_votes:
        # votes = spark.read.format('json').load(f'{data_dir}/Posts.json')
        # reps = votes.withColumn ... # use votes as reputation changes data
        raise NotImplementedError('TODO: implement reps_from_votes')
    else:
        reps = spark.read.format('json').load(f'{data_dir}/Reps.json')
    return users, posts, reps

def compute_pandas_dataframes(users, posts, reps, n_users):
    U = (
        users
        .select(
            F.col('Id').alias('UserId'),
            F.col('Reputation').alias('UserRep'),
            F.to_timestamp('CreationDate').alias('UserCreationTime'),
            F.to_timestamp('LastAccessDate').alias('UserLastAccessTime'),
        )
        .filter('UserId != -1') # user -1 is not a real user
        .orderBy('UserCreationTime')
        .limit(n_users)
    )
    U = add_weekid(U, 'UserCreationTime', 'UserCreationWeekId')
    U = add_weekid(U, 'UserLastAccessTime', 'UserLastAccessWeekId')

    A = (
        posts
        .filter('PostTypeId = 2')
        .select(
            F.col('OwnerUserId').alias('PostUserId'),
            F.to_timestamp('CreationDate').alias('PostCreationTime'),
        )
    )

    R = (
        reps
        .filter('PostTypeId = 2')
        .select(
            F.col('UserId').alias('RepUserId'),
            F.col('Text').alias('RepText'),
            F.to_timestamp('Time').alias('RepTime'),
        )
    )

    UA = add_weekid(U.join(A, on=U.UserId == A.PostUserId, how='inner'), 'PostCreationTime', 'PostWeekId')
    UR = add_weekid(U.join(R, on=U.UserId == R. RepUserId, how='inner'), 'RepTime', 'RepWeekId')

    users = U.toPandas()
    ua_cnt = UA.groupby('UserId', 'PostWeekId').agg(F.count('PostUserId').alias('Count')).toPandas()
    ur_cnt = UR.groupby('UserId',  'RepWeekId', 'RepText').agg(F.count( 'RepUserId').alias('Count')).toPandas()

    return users, ua_cnt, ur_cnt

def fill_table(ua_cnt, ur_cnt):
    user_ids = sorted(ua_cnt.UserId.unique())
    user_id2i = {d:i for i,d in enumerate(user_ids)}
    user_ids_set = set(user_ids)

    n_weeks = max(ua_cnt.PostWeekId.max(), ur_cnt.RepWeekId.max()) + 1

    print('#users #weeks:', len(user_ids), n_weeks)

    ua_cnt_table = pd.DataFrame(0, index=user_ids, columns=np.arange(n_weeks))
    uu_cnt_table = pd.DataFrame(0, index=user_ids, columns=np.arange(n_weeks))
    ud_cnt_table = pd.DataFrame(0, index=user_ids, columns=np.arange(n_weeks))

    for i, e in tqdm(ua_cnt.iterrows(), total=len(ua_cnt), desc='user x answers'):
        ui = user_id2i[e.UserId]
        wi = e.PostWeekId
        ua_cnt_table.values[ui, wi] = e.Count

    for i, e in tqdm(ur_cnt.iterrows(), total=len(ur_cnt), desc='user x up/downvotes'):
        # even after filtering for upvote&downvote on answers
        # there're still some users that have reps but no answers (e.g. 450949)
        if e.UserId in user_ids_set:
            ui = user_id2i[e.UserId]
            wi = e.RepWeekId
            if e.RepText == 'upvote':
                uu_cnt_table.values[ui, wi] = e.Count
            elif e.RepText == 'downvote':
                ud_cnt_table.values[ui, wi] = e.Count

    return ua_cnt_table, uu_cnt_table, ud_cnt_table

def gen_timeseries(data_dir, n_users=1000000, reps_from_votes=False):
    print('1. reading spark dataframes')
    users, posts, reps = get_spark_dataframes(data_dir, reps_from_votes)

    print('2. computing pandas dataframes') 
    users, ua_cnt, ur_cnt = compute_pandas_dataframes(users, posts, reps, n_users)

    print('>> stats: count of rep change events by category')
    print(ur_cnt['RepText'].value_counts())

    print('3. filling in the table')
    ua_cnt_table, uu_cnt_table, ud_cnt_table = fill_table(ua_cnt, ur_cnt)

    print('4. saving csv files')
    users.to_csv(data_dir + '/users.csv')
    ua_cnt_table.to_csv(data_dir + '/user_answer_timeseries.csv')
    uu_cnt_table.to_csv(data_dir + '/user_upvote_timeseries.csv')
    ud_cnt_table.to_csv(data_dir + '/user_downvote_timeseries.csv')
