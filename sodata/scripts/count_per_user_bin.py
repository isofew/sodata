from pyspark.sql import functions as F
from . import get_spark

# count posts and votes per user per time bin
def count_per_user_bin(
    data_dir,
    mem=16, cores=8, verbose=True,
    post_filter='PostTypeId = 2',
    user_filter='UserReputation > 50000',
    vote_filter='isNull(VoteTypeId) or VoteTypeId = 1 or VoteTypeId = 2 or VoteTypeId = 3',
    bin_unit='day', allow_same_bin_vote=True,
):
    if verbose:
        print('[count_per_user_bin] config =', dict(
            post_filter = post_filter,
            user_filter = user_filter,
            vote_filter = vote_filter,
            bin_unit = bin_unit,
            allow_same_bin_vote = allow_same_bin_vote,
        ))
        print()

    spark = get_spark('count_per_user_bin', mem=mem, cores=cores)

    if verbose:
        print('loading puv')
    puv = (
        spark.read.json(f'{data_dir}/puv.json')
        .filter(post_filter)
        .filter(user_filter)
        .filter(vote_filter)
        .withColumn('VoteCreationBin', F.date_trunc(bin_unit, F.col('VoteCreationDate')))
        .withColumn('PostCreationBin', F.date_trunc(bin_unit, F.col('PostCreationDate')))
    )

    user_vote_count = (
        puv
        .filter('not isNull(VoteCreationBin)')
        .filter('true' if allow_same_bin_vote else 'VoteCreationBin != PostCreationBin')
        .groupBy('UserId', 'VoteCreationBin', 'VoteTypeId')
        .agg(F.sum(F.col('VoteCount')).alias('VoteCount'))
    )
    user_post_count = (
        puv
        .filter('not isNull(PostCreationBin)')
        .groupBy('UserId', 'PostCreationBin', 'PostTypeId')
        .agg(F.countDistinct(F.col('PostId')).alias('PostCount'))
    )
    list_of_dict = lambda x: [
        {k: d[k] for k in x.columns}
        for d in x.collect()
    ]

    if verbose:
        print('wrting results')
    user_vote_count.write.format('json').save(f'{data_dir}/user_vote_count.json')
    user_post_count.write.format('json').save(f'{data_dir}/user_post_count.json')

    spark.stop()
