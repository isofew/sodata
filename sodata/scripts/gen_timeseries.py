from . import get_spark
import pyspark.sql.functions as F
from tqdm import tqdm
import pandas as pd
import numpy as np
import datetime
import os

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

def compute_pandas_dataframes(users, posts, reps, min_rep):
    U = (
        users
        .select(
            F.col('Id').alias('UserId'),
            F.col('Reputation').alias('UserRep'),
            F.to_timestamp('CreationDate').alias('UserCreationTime'),
            F.to_timestamp('LastAccessDate').alias('UserLastAccessTime'),
        )
        .filter(f'UserId != -1 and UserRep >= {min_rep}') # user -1 is not a real user
        .orderBy('UserCreationTime')
    )
    U = add_weekid(U, 'UserCreationTime', 'UserCreationWeekId')
    U = add_weekid(U, 'UserLastAccessTime', 'UserLastAccessWeekId')

    P = posts.select(
        F.col('PostTypeId'),
        F.col('OwnerUserId').alias('PostUserId'),
        F.to_timestamp('CreationDate').alias('PostCreationTime'),
    )
    
    R = reps.select(
        F.col('PostTypeId'),
        F.col('Delta').alias('RepDelta'),
        F.col('UserId').alias('RepUserId'),
        F.col('Text').alias('RepText'),
        F.to_timestamp('Time').alias('RepTime'),
    )

    UP = add_weekid(U.join(P, on=U.UserId == P.PostUserId, how='inner'), 'PostCreationTime', 'PostWeekId')
    UR = add_weekid(U.join(R, on=U.UserId == R. RepUserId, how='inner'), 'RepTime', 'RepWeekId')

    users_df = U.toPandas()
    posts_df = UP.groupby('UserId', 'PostWeekId', 'PostTypeId').agg(
        F.count('PostUserId').alias('Count'),
    ).toPandas()
    reps_df = UR.groupby('UserId',  'RepWeekId', 'PostTypeId', 'RepText').agg(
        F.count('RepUserId').alias('Count'),
        F.sum('RepDelta').alias('Sum'),
    ).toPandas()

    return users_df, posts_df, reps_df

def fill_table(posts_df, reps_df):
    user_ids = sorted(posts_df.UserId.unique())
    user_id2i = {d:i for i,d in enumerate(user_ids)}
    user_ids_set = set(user_ids)

    n_weeks = max(posts_df.PostWeekId.max(), reps_df.RepWeekId.max()) + 1

    print('#users #weeks:', len(user_ids), n_weeks)

    timeseries = {}
    for k in [
        'question', 'answer',
        'q-upvote', 'q-downvote',
        'a-upvote', 'a-downvote', 'a-accept',
        'repsum',
    ]:
        timeseries[k] = pd.DataFrame(0, index=user_ids, columns=np.arange(n_weeks))

    for i, e in tqdm(posts_df.iterrows(), total=len(posts_df), desc='user x posts'):
        ui = user_id2i[e.UserId]
        wi = e.PostWeekId
        if e.PostTypeId == 1:
            timeseries['question'].values[ui, wi] = e.Count
        elif e.PostTypeId == 2:
            timeseries['answer'].values[ui, wi] = e.Count

    for i, e in tqdm(reps_df.iterrows(), total=len(reps_df), desc='user x reps'):
        # even after filtering for upvote&downvote on answers
        # there're still some users that have reps but no answers (e.g. 450949)
        if e.UserId in user_ids_set:
            ui = user_id2i[e.UserId]
            wi = e.RepWeekId
            if e.PostTypeId == 1:
                if e.RepText == 'upvote':
                    timeseries['q-upvote'].values[ui, wi] = e.Count
                elif e.RepText == 'downvote':
                    timeseries['q-downvote'].values[ui, wi] = e.Count
            elif e.PostTypeId == 2:
                if e.RepText == 'upvote':
                    timeseries['a-upvote'].values[ui, wi] = e.Count
                elif e.RepText == 'downvote':
                    timeseries['a-downvote'].values[ui, wi] = e.Count
                elif e.RepText == 'accept':
                    timeseries['a-accept'].values[ui, wi] = e.Count
            if e.Sum == e.Sum: # not nan
                timeseries['repsum'].values[ui, wi] += e.Sum
    return timeseries

def save_csv(df, path):
    print('saving', path)
    df.to_csv(path)

def gen_timeseries(in_dir, out_dir, min_rep=1000, reps_from_votes=False):
    print('1. reading spark dataframes')
    users, posts, reps = get_spark_dataframes(in_dir, reps_from_votes)

    print('2. computing pandas dataframes')
    users_df, posts_df, reps_df = compute_pandas_dataframes(users, posts, reps, min_rep)

    print('3. filling in the timeseries table')
    timeseries = fill_table(posts_df, reps_df)

    print('4. saving csv files')
    save_csv(users_df, os.path.join(out_dir, 'users.csv'))
    save_csv(posts_df, os.path.join(out_dir, 'posts.csv'))
    save_csv( reps_df, os.path.join(out_dir, 'reps.csv'))
    for k, k_df in timeseries.items():
        save_csv(k_df, os.path.join(out_dir, f'weekly_{k}.csv'))
