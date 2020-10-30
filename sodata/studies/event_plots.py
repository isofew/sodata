import sodata.scripts
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns


# TODO right now the reputation calculation is not complete:
# 1. only counting votes on answers due to previous selection
# 2. not counting bounties, edit rewards
# 3. can't include downvote and bounty deduction since the user who cast these votes are anonymized in data dump
# so maybe we'd better just scrape user profile pages...
def rep_changes(dfv):
    rs = [1]
    delta = {1: 15, 2: 10, 3: -2}
    for i, e in dfv.iterrows():
        r = rs[-1]
        r += delta[e.VoteTypeId] * e.VoteCount
        rs.append(r)
    return list(zip(rs[:-1], rs[1:]))


def level_up_votes(dfv, rep_levels=[
    # obtained from https://stackoverflow.com/help/privileges
    1,    5,    10,    15,    20,    50,    75,    100,    125,
    200,  250,  500,   1000,  1500,  2000,  2500,  3000,   5000,
    10000,      15000,        20000,        25000
]):
    df = dfv.sort_values('VoteCreationBin').reset_index()
    df['UserRepChange'] = rep_changes(df)
    return pd.concat([
        df[df.UserRepChange.map(lambda t: t[0]<l and t[1]>=l)]
        for l in rep_levels
    ]).reset_index()


def event_windows(dfp, dfv, offset):
    dfv['BinAfter'] = dfv.VoteCreationBin.map(lambda x: x + offset)
    dfv['BinBefore'] = dfv.VoteCreationBin.map(lambda x: x - offset)
    
    df_before = pd.merge(dfv, dfp, how='left', left_on='BinBefore', right_on='PostCreationBin')
    df_on     = pd.merge(dfv, dfp, how='left', left_on='VoteCreationBin', right_on='PostCreationBin')
    df_after  = pd.merge(dfv, dfp, how='left', left_on='BinAfter', right_on='PostCreationBin')
    
    return pd.concat([
        dfv[  ['UserId', 'VoteCount', 'VoteCreationBin', 'VoteTypeId']  ],
        df_before['PostCount'].rename('PostCount_before'),
        df_on    ['PostCount'].rename('PostCount_on'),
        df_after ['PostCount'].rename('PostCount_after'),
    ], axis=1).sort_values('VoteCreationBin').fillna(0)


def event_plots(
    data_dir, output_filename, figsize, verbose=True,
    no_mix=False, vote_type_ids=[1,2,3,'lvup'], n_samples=5, seed=0, bin_unit='day'
):
    if verbose:
        print('[event_plots] config', dict(no_mix=no_mix, vote_type_ids=vote_type_ids, n_samples=n_samples, seed=seed, bin_unit=bin_unit))
    
    spark = sodata.scripts.get_spark('event_plots', mem=4, cores=2)
    
    if verbose:
        print('loading count data')
    
    dfp = spark.read.format('json').load(f'{data_dir}/user_post_count.json').toPandas()
    dfv = spark.read.format('json').load(f'{data_dir}/user_vote_count.json').toPandas()
    
    dfp['PostCreationBin'] = pd.to_datetime(dfp['PostCreationBin'])
    dfv['VoteCreationBin'] = pd.to_datetime(dfv['VoteCreationBin'])
    
    np.random.seed(seed)
    user_ids = np.random.choice( sorted(set(dfp.UserId.unique()) & set(dfv.UserId.unique())), n_samples )
    
    offset = pd.DateOffset(days=dict(day=1, week=7)[bin_unit])
    
    if verbose:
        print('making plots')
    
    sns.set_theme()
    fig, ax = plt.subplots(nrows = n_samples, ncols = len(vote_type_ids), figsize=figsize)
    for i, user_id in enumerate(user_ids):
        dfp_ = dfp[(dfp.UserId == user_id)].reset_index()
        for j, vote_type_id in enumerate(vote_type_ids):
            if vote_type_id == 'lvup':
                dfv_ = level_up_votes(dfv[dfv.UserId == user_id])
            else:
                dfv_ = dfv[(dfv.UserId == user_id) & (dfv.VoteTypeId == vote_type_id)].reset_index()
                if no_mix and vote_type_id in [2,3]:
                    if vote_type_id == 2:
                        dfv_other = dfv[(dfv.UserId == user_id) & (dfv.VoteTypeId == 3)].reset_index()
                    elif vote_type_id == 3:
                        dfv_other = dfv[(dfv.UserId == user_id) & (dfv.VoteTypeId == 2)].reset_index()
                    common_bin = set(dfv_.VoteCreationBin.unique()) & set(dfv_other.VoteCreationBin.unique())
                    dfv_ = dfv_[dfv_.VoteCreationBin.map(lambda b: b not in common_bin)].reset_index()
            wnd = event_windows(dfp_, dfv_, offset)
            wnd[['PostCount_before', 'PostCount_on', 'PostCount_after']].boxplot(ax=ax[i][j])
            ax[i][j].set_xticklabels(['before', 'on', 'after'])
            if j == 0:
                ax[i][j].set_ylabel(f'PostCount for user={user_id}')
            if i == 0:
                vote_names = {1: 'Accepted', 2: 'UpVoted', 3: 'DownVoted', 'lvup': '(Privilege) Level Up'}
                ax[i][j].set_title(vote_names[vote_type_id])
    
    if verbose:
        print('save figure')
    
    plt.savefig(output_filename, dpi=300)
    spark.stop()
