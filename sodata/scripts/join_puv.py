from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# Join posts, users and vote counts per type
def join_puv(data_dir, mem=16, cores=8, verbose=True):
    spark = (
        SparkSession
        .builder
        .config('spark.driver.memory', f'{mem}g') 
        .config('spark.sql.session.timeZone', 'UTC')
        .appName('join_puv')
        .master(f'local[{cores}]')
        .getOrCreate()
    )

    if verbose:
        print('loading users')
    users = spark.read.json(f'{data_dir}/Users.json').select(
        F.col('Id').alias('UserId'),
        F.col('Reputation').alias('UserReputation'),
        F.col('UpVotes').alias('UserUpVotes'),
        F.col('DownVotes').alias('UserDownVotes'),
    )

    if verbose:
        print('loading posts')
    posts = spark.read.json(f'{data_dir}/Posts.json').select(
        F.col('AcceptedAnswerId').alias('PostAcceptedAnswerId'),
        F.col('AnswerCount').alias('PostAnswerCount'),
        F.col('ClosedDate').alias('PostClosedDate'),
        F.col('CommentCount').alias('PostCommentCount'),
        F.col('CreationDate').alias('PostCreationDate'),
        F.col('FavoriteCount').alias('PostFavoriteCount'),
        F.col('Id').alias('PostId'),
        F.col('OwnerUserId').alias('PostOwnerUserId'),
        F.col('ParentId').alias('PostParentId'),
        F.col('Score').alias('PostScore'),
        F.col('PostTypeId')
    )

    if verbose:
        print('loading votes')
    votes = spark.read.json(f'{data_dir}/Votes.json').select(
        F.col('CreationDate').alias('VoteCreationDate'),
        F.col('Id').alias('VoteId'),
        F.col('PostId').alias('VotePostId'),
        F.col('UserId').alias('VoteUserId'),
        F.col('VoteTypeId'),
    )
    votes = votes.groupBy(
        'VotePostId',
        'VoteTypeId',
        'VoteCreationDate',
    ).agg(F.count(F.col('VoteId')).alias('VoteCount'))

    if verbose:
        print('joining posts with users and vote counts on posts')

    pufv = (
        posts
        .join(users, F.col('PostOwnerUserId') == F.col('UserId'), 'left')
        .join(votes, F.col('PostId') == F.col('VotePostId'), 'left')
    )

    if verbose:
        print('adding feature columns: timestamp, weekday and time to next post')

    pufv = pufv.withColumn(
        'PostCreationTime',
        F.unix_timestamp(
            'PostCreationDate',
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    pufv = pufv.withColumn(
        'PostCreationWeekDay',
        F.dayofweek('PostCreationDate')
    )

    wnd = (
        Window
        .partitionBy('UserId')
        .orderBy('PostId')
        .rowsBetween(-1, 0)
    )

    pufv = pufv.withColumn(
        'PostTimeSincePrev',
        (  F.max('PostCreationTime').over(wnd)
         - F.min('PostCreationTime').over(wnd) )
    )

    stwnd = (
        Window
        .partitionBy('UserId', 'PostTypeId')
        .orderBy('PostId')
        .rowsBetween(0, 1)
    )

    pufv = pufv.withColumn(
        'PostTimeSincePrevSameType',
        (  F.max('PostCreationTime').over(stwnd)
         - F.min('PostCreationTime').over(stwnd) )
    )

    if verbose:
        print('writing results')

    pufv.write.format('json').save(f'{data_dir}/pufv.json')
