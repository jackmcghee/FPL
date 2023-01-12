"""this picks up the jsons/csvs downloaded as part of download_json.py and does interesting stuff with them"""

import pandas as pd
pd.options.mode.chained_assignment = None 
from sys import path
from os.path import join
path.append('C:/Users/Knick/Documents/Python/Projects - Independent/FPL Main/FPL/Scripts/my_classes')
import FPL_4 as fpl
from itertools import product, combinations

base_dir='C:/Users/Knick/Documents/Python/Projects - Independent/FPL Main/22_23/draft'
download_dir=join(base_dir,'extracts')
analysis_dir=join(base_dir,'processed')

default_enc="utf-8-sig"

runner=fpl.draft_runner(base_dir)
runner.create_folder(analysis_dir)
runner.set_target_dir(analysis_dir)

analysis_folders=['player_history','bootstrap','transfers','manager_history','draft_picks']

def folder_creation(folder_path,folder_list):
    """creating one or more folders in a given folder"""
    for fldr in folder_list:
        runner.set_target_dir(analysis_dir)
        runner.create_folder(fldr,use_parent_dir=True)
        fldr_path=join(analysis_dir,fldr)
        runner.set_target_dir(fldr_path)
        runner.create_folder('grouped',use_parent_dir=True)
        fldr_dict={'main':fldr_path,'grouped':join(fldr_path,'grouped')} 
        yield fldr,fldr_dict
fldr_list = dict(folder_creation(analysis_dir,analysis_folders))

def groupby_all(folder,dataframe,columns_to_group,func_list=None,prefix=""):
    """used to give aggregate outputs of a given dataframe. One or more columns 
    can be specified and each will be grouped together separately"""
    if func_list is None:
        func_list=['sum','max','min','mean','median','mode','nunique','std']
    elif isinstance(func_list,str):
        func_list=[func_list]
    if isinstance(columns_to_group,str):
        columns_to_group=[columns_to_group]
    num_cols=len(columns_to_group)
    for i in range(1,num_cols+1):
        col_combinations=list(combinations(columns_to_group,i))
        for col in col_combinations:
            grped_combo=dataframe.groupby(list(col))
            column_names=f"{prefix}{'_'.join(col)}"
            folder_loc=join(fldr_list[folder]['grouped'])
            if 'sum' in func_list:
                grped_combo.sum().to_csv(join(folder_loc,f"{column_names}_sum.csv"),encoding=default_enc)
            if 'max' in func_list:
                grped_combo.max(numeric_only=True).to_csv(join(folder_loc,f"{column_names}_max.csv"),encoding=default_enc)
            if 'min' in func_list:
                grped_combo.min(numeric_only=True).to_csv(join(folder_loc,f"{column_names}_min.csv"),encoding=default_enc)
            if 'count' in func_list:
                grped_combo.count().to_csv(join(folder_loc,f"{column_names}_count.csv"),encoding=default_enc)
            if 'mean' in func_list:
                grped_combo.mean(numeric_only=True).to_csv(join(folder_loc,f"{column_names}_mean.csv"),encoding=default_enc)
            if 'median' in func_list:
                grped_combo.median(numeric_only=True).to_csv(join(folder_loc,f"{column_names}_median.csv"),encoding=default_enc)
            #if 'mode' in func_list:
                #grped_combo.agg(lambda x:x.value_counts().index[0]).to_csv(join(folder_loc,f"{column_names}_mode.csv"),encoding=default_enc)
            if 'nunique' in func_list:
                grped_combo.nunique().to_csv(join(folder_loc,f"{column_names}_unique.csv"),encoding=default_enc)
            if 'std' in func_list:
                grped_combo.std().to_csv(join(folder_loc,f"{column_names}_sum.csv"),encoding=default_enc)        

#this part maps the team and position name onto the element
summary_dir=join(download_dir,'summary') #elements
elements=join(summary_dir,'bootstrap_elements.csv')
elements_df=pd.read_csv(elements,index_col=0)
teams=join(summary_dir,'bootstrap_teams.csv') #teams
teams_df=pd.read_csv(teams,index_col='id')
teams_df_slim=teams_df.loc[:,['short_name','name']].rename(columns={'short_name':'team_short_name','name':'team_name'})
positions=join(summary_dir,'bootstrap_element_types.csv') #position
positions_df=pd.read_csv(positions,index_col='id')
positions_df_slim=positions_df.loc[:,['singular_name_short']].rename(columns={'singular_name_short':'position'})

new_elements_df=pd.merge(elements_df,teams_df_slim,how='left',left_on=['team'],
                               right_on=['id'],right_index=True) #getting the teams
new_elements_df=pd.merge(new_elements_df,positions_df_slim,how='left',left_on=['element_type'],
                               right_on=['id'],right_index=True) #gettings the positions

new_elements_df.to_csv(join(fldr_list['bootstrap']['main'],'bootstrap_elements_annotated.csv'),encoding=default_enc)
elements_slim=new_elements_df.loc[:,['id','team_short_name','position','web_name','total_points']].set_index('id')
elements_slim=elements_slim.rename(columns={'total_points':'current_points'})                                                                                                                    
    
groupby_all(folder='bootstrap',dataframe=new_elements_df,columns_to_group=['position','team_name'],func_list=['sum']) #TODO the max, min etc can come from the bootstrap thing itself, grouping by doesn't help here since you lose the player


#this part puts the manager picks onto the players (week by week)
manager_picks=join(download_dir,'manager/full/manager_picks.csv')
manager_picks_df=pd.read_csv(manager_picks,index_col=0) #this is all the players picked in each week
manager_picks_df['started']=manager_picks_df['position'].map(lambda x: True if x <= 11 else False) #if they are in the first 11, they started
#TODO, what about if only 10 played and 4 didn't have games, i think it might be good to join first, then do this bit
manager_picks_df.drop(['multiplier','is_captain','is_vice_captain'],axis=1,inplace=True) #no need for draft

player_history=join(download_dir,'player/full/player_history.csv')
player_history_df=pd.read_csv(player_history,index_col=0) #how every player has played
manager_picks_df_slim=manager_picks_df.loc[:,['element','event','manager','started']]
player_history_df_new=pd.merge(player_history_df,manager_picks_df_slim,how='left',left_on=['element','event'],
                               right_on=['element','event']) #this now shows who was picked in each week, this join will get rid of the players who didn't have games (and therefore deal with the above 'started' issue

#this now puts the player position and team onto the player in here
player_history_df_new_annotated=pd.merge(left=player_history_df_new,right=elements_slim,
                                         left_on=['element'],right_index=True) 
#player_history_df_new_annotated.to_csv(join(fldr_list['player_history']['main'],'player_history_annotated.csv'),encoding=default_enc) will do some more work on it during ideal team           

#now we create various spreadsheets for different group filters
played=player_history_df_new.loc[player_history_df_new['started']==True] #all the players who were started
subs=player_history_df_new.loc[player_history_df_new['started']==False] #all the players who were selected
plyr_fldr='player_history'
plyr_cols_to_group=['manager','event']
plyr_funcs=['sum']
groupby_all(folder=plyr_fldr,dataframe=subs,columns_to_group=plyr_cols_to_group,func_list=plyr_funcs,prefix='subs_by_')
groupby_all(folder=plyr_fldr,dataframe=played,columns_to_group=plyr_cols_to_group,func_list=plyr_funcs,prefix='played_by_')
groupby_all(folder=plyr_fldr,dataframe=player_history_df_new,columns_to_group=plyr_cols_to_group,func_list=plyr_funcs,prefix='All_by_')

#this is getting the manager league id and manaer short_name
league_dir=join(download_dir,'league') #league
manager_details=join(league_dir,'member_details/league_member_details_league_entries.csv')
man_df=pd.read_csv(manager_details,index_col=0)
man_df_slim=man_df.loc[:,['entry_id','short_name']].set_index('entry_id').rename(columns={'short_name':'manager'})

#now we're doing transfers
GROUPED_BASE_TRANSFER_COLUMNS=['attempted_transfers','successful_transfers']
transfers=join(league_dir,'transactions/transactions_transactions.csv')
t_df_raw=pd.read_csv(transfers,index_col=0,encoding=default_enc)
t_df=pd.merge(left=t_df_raw,right=man_df_slim,left_on='entry',right_index=True) #putting the manager initials on
t_df['player_in']=t_df['element_in'].map(elements_slim['web_name']) #putting the player in initials on
t_df['player_out']=t_df['element_out'].map(elements_slim['web_name']) #putting the player out initials on
t_df.to_csv(join(fldr_list['transfers']['main'],'all_transfers.csv'),encoding=default_enc)

def prep_transfers(dataframe,name,cols=GROUPED_BASE_TRANSFER_COLUMNS):
    dataframe.columns=cols
    dataframe['successful_proportion'] = dataframe['successful_transfers'] / dataframe['attempted_transfers']
    dataframe.to_csv(join(fldr_list['transfers']['grouped'],f'{name}.csv'),encoding=default_enc)

t_df_success=t_df.where(t_df['result']=='a') #only successful transfers
all_by_gw=t_df.groupby('event').count().loc[:,['result']] #grouping all by event
success_by_gw=t_df_success.groupby('event').count().loc[:,['result']] #grouping successful by event
full_by_gw=pd.merge(left=all_by_gw,right=success_by_gw,left_index=True,right_index=True) #joining the two
prep_transfers(full_by_gw,name='all_transfers_by_gw')

all_by_man=t_df.groupby('manager').count().loc[:,['result']] #grouping all by manager
success_by_man=t_df_success.groupby('manager').count().loc[:,['result']] #grouping successful by manager
full_by_man=pd.merge(left=all_by_man,right=success_by_man,left_index=True,right_index=True) #joining the two
prep_transfers(full_by_man,name='all_transfers_by_manager')

all_by_man_by_gw=t_df.groupby(['event','manager']).count()['result'] #all transfers
success_by_man_by_gw=t_df_success.groupby(['event','manager']).count()['result'] #group all by manager and event
full_by_man_by_gw = pd.merge(left=all_by_man_by_gw,right=success_by_man_by_gw,left_index=True,right_index=True)
prep_transfers(full_by_man_by_gw,name='all_transfers_by_gw_by_manager')

#splitting the transfers by result
data_waivers = t_df.loc[t_df['kind']=='w']
managers = t_df.manager.unique()
gws = t_df.event.unique()
gws.sort()
manager_gws = pd.MultiIndex.from_product([managers,gws], names=["manager", "GW"])
possible_results = {'a':'Accepted','di':'Player in not available','do':'Player out not available'}
waiver_summary_empty = pd.DataFrame(#data=np.zeros([len(manager_gws),len(possible_results.values())]),
             columns=possible_results.values(),
             index=manager_gws)
waiver_summary = waiver_summary_empty.copy()
for res,desc in possible_results.items():
    num_transfers = data_waivers.loc[data_waivers['result']==res].groupby(['manager','event']).count()
    num_transfers_col = num_transfers['added']
    waiver_summary[desc] = waiver_summary.index.map(num_transfers_col).fillna(0)
waiver_summary['Failed'] = waiver_summary['Player in not available'] + waiver_summary['Player out not available']
failed = waiver_summary['Player in not available'] + waiver_summary['Player out not available']
waiver_summary['Failed'] = failed
waiver_summary['Accepted_ratio'] = waiver_summary['Accepted'] / (waiver_summary['Accepted'] + failed)
waiver_summary['Accepted_ratio'] = waiver_summary['Accepted_ratio'].fillna(0).round(2)
waiver_summary['Rejected_ratio'] = waiver_summary['Player in not available'] / (waiver_summary['Accepted'] + waiver_summary['Player in not available'])
waiver_summary['Rejected_ratio'] = waiver_summary['Rejected_ratio'].fillna(0).round(2)
waiver_summary.to_csv(join(fldr_list['transfers']['grouped'],'waivers_by_gw_by_manager_by_result.csv'),encoding=default_enc)

manager_history=join(download_dir,'manager/full/manager_history.csv') #scores for each manager each GW
df=pd.read_csv(manager_history,index_col=0)

def fill_then_rank(dataframe,col_list):
    if isinstance(col_list,str):
        col_list = [col_list] 
    for name in col_list:
        name_fill=f'{name}_filled'
        dataframe[name_fill]=dataframe[name].fillna(0).astype(int)
        dataframe[f'{name}_rank']=dataframe[name_fill].rank(ascending=False).astype(int)
        dataframe.drop(name_fill,axis=1,inplace=True)
    return dataframe

score_summary=df.copy()
success_by_entry_by_gw=t_df_success.groupby(['event','entry']).count()['result'] #all sucessful transfers
all_by_entry_by_gw=t_df.groupby(['event','entry']).count()['result'] #all transfers
score_summary_trans_s=pd.merge(score_summary,success_by_entry_by_gw,how='left',left_on=['event','entry'],right_index=True) #adding number of successful transfers
score_summary_trans_s.rename(columns={'result':'successful_transfers','points':'gw_points'},inplace=True)
score_summary_trans=pd.merge(score_summary_trans_s,all_by_entry_by_gw,how='left',left_on=['event','entry'],right_index=True) #adding number of total transfers
score_summary_trans.rename(columns={'result':'attempted_transfers'},inplace=True)
score_summary_trans=fill_then_rank(score_summary_trans,['attempted_transfers','successful_transfers','gw_points','total_points'])
score_summary_trans['gw_rank_by_gw']=score_summary_trans.groupby('event').rank(ascending=False)['gw_points'].astype(int) #ranking scores for manager per gw
score_summary_trans.drop(['rank','rank_sort','event_transfers'],axis=1,inplace=True)
score_summary_trans.drop('points_on_bench',axis=1,inplace=True)
subs.rename(columns={'total_points':'points_on_bench'},inplace=True) 
subs_grouped_df=subs.groupby(['event','manager']).sum()['points_on_bench']
score_summary_trans_subs=pd.merge(score_summary_trans,subs_grouped_df,how='left',left_on=['event','entry'],right_index=True) #addings number of points on bench
score_summary_trans_subs['points_inc_bench']=score_summary_trans_subs['points_on_bench']+score_summary_trans_subs['gw_points']
score_summary_trans_subs['bench_points_div_total_points']=score_summary_trans_subs['points_on_bench']/score_summary_trans_subs['gw_points']
score_summary_trans_subs=fill_then_rank(score_summary_trans_subs,['points_on_bench','points_inc_bench','bench_points_div_total_points'])
for i in ['total_points','points_on_bench','points_inc_bench','successful_transfers','attempted_transfers','bench_points_div_total_points']:
    score_summary_trans_subs[f'{i}_rank_by_gw']=score_summary_trans_subs.groupby('event').rank(ascending=False)[i]
score_summary_full=pd.merge(score_summary_trans_subs,man_df_slim,left_on='entry',right_index=True).fillna(0)
score_summary_full=score_summary_full.loc[:,['event','gw_points','gw_rank_by_gw','gw_points_rank',
        'total_points','total_points_rank_by_gw','total_points_rank','points_on_bench','points_on_bench_rank_by_gw',
        'points_on_bench_rank','points_inc_bench','points_inc_bench_rank_by_gw','points_inc_bench_rank',
        'bench_points_div_total_points','bench_points_div_total_points_rank_by_gw','bench_points_div_total_points_rank',
       'attempted_transfers','attempted_transfers_rank_by_gw','attempted_transfers_rank',                                      
       'successful_transfers','successful_transfers_rank_by_gw','successful_transfers_rank','manager']]
score_summary_full.to_csv(join(fldr_list['manager_history']['main'],'manager_summary.csv'),encoding=default_enc)

summary_cols=['event','gw_points','gw_rank_by_gw','total_points_rank_by_gw',
                'points_on_bench','points_on_bench_rank_by_gw','points_inc_bench',
               'points_inc_bench_rank_by_gw','bench_points_div_total_points_rank_by_gw',
                'successful_transfers','successful_transfers_rank_by_gw',
              'attempted_transfers','attempted_transfers_rank_by_gw','manager']
score_summary_for_grouping=score_summary_full.loc[:,summary_cols]
groupby_all('manager_history',dataframe=score_summary_for_grouping,columns_to_group=['manager','event'])

#sorting the ideal team 
min_ranks={'GKP':1,'DEF':3,'MID':2,'FWD':1}
max_ranks={'GKP':1,'DEF':5,'MID':5,'FWD':3}
players=player_history_df_new_annotated.copy()
players=players.set_index('id')
all_man_all_team=pd.DataFrame()
all_managers=players.manager.dropna().unique()
all_gws=players.event.dropna().unique()

for manager,gw in product(all_managers,all_gws):
    min_team=pd.DataFrame()
    remainder_team=pd.DataFrame()
    team=players.loc[(players.manager==manager)&(players.event==gw)]
    pos_rank=team.groupby('position').rank('first',ascending=False)['total_points']
    team_pos_rank=pd.merge(team,pos_rank,left_on='id',right_index=True,suffixes=['','_rank'])
    team_pos_rank_sorted=team_pos_rank.sort_values('total_points',ascending=False) #full team sorted

    for pos in team_pos_rank_sorted.position.unique():
        pos_sorted=team_pos_rank_sorted.loc[team_pos_rank_sorted['position']==pos] #all players in the team for the position
        pos_sorted_top=pos_sorted.head(min_ranks[pos]) #the best x players, x is the min number of players needed in the team
        min_team=pd.concat([min_team,pos_sorted_top]) #collating an ideal team
        num_to_drop=len(pos_sorted) - max_ranks[pos] #the number of players that need to be dropped by default 
        pos_sorted_rem=pos_sorted[~pos_sorted.index.isin(pos_sorted_top.index)] #the non top players
        pos_sorted_true_rem=pos_sorted_rem.drop(pos_sorted_rem.tail(num_to_drop).index) #getting rid of the bottom players (GKs)
        remainder_team=pd.concat([remainder_team,pos_sorted_true_rem])
    min_team_length=len(min_team)
    rem_team=remainder_team.sort_values('total_points',ascending=False).head(11-min_team_length) #the best of the rest
    full_ideal_team=pd.concat([min_team,rem_team])
    full_ideal_team['ideal']=True
    full_non_ideal_team=team_pos_rank.loc[~team_pos_rank.index.isin(full_ideal_team.index)]
    full_non_ideal_team['ideal']=False
    full_team=pd.concat([full_ideal_team,full_non_ideal_team])
    all_man_all_team=pd.concat([all_man_all_team,full_team])
players['ideal']=players.index.map(all_man_all_team['ideal'])
players['manager']=players['manager'].map(man_df_slim['manager'])
players.to_csv(join(fldr_list['player_history']['main'],'player_history_annotated.csv'),encoding=default_enc)
max_round=players['event'].max()
most_recent_gw=players.loc[players['event']==max_round].drop_duplicates().set_index('element') #only the most recent round and ignore double gws (will be the same owner anyway)

#this is dealing with draft picks
draft_picks_dir=join(league_dir,'draft_picks')
dp_df=pd.read_csv(join(draft_picks_dir,'initial_draft_picks_choices.csv'),index_col=0)
dp_df['player']=dp_df['element'].map(elements_slim['web_name'])
dp_df['current_points']=dp_df['element'].map(elements_slim['current_points'])
dp_df['current_points_rank']=dp_df['current_points'].rank(ascending=False)
dp_df['original_owner']=dp_df['entry'].map(man_df_slim['manager'])
dp_df['current_owner']=dp_df['element'].map(most_recent_gw['manager'])
dp_df.to_csv(join(fldr_list['draft_picks']['main'],'draft_picks_annotated.csv'),encoding=default_enc)

dp_df_slim=dp_df.loc[:,['id','player_last_name','round','was_auto','current_points','current_points_rank']]
dp_df_slim.groupby('player_last_name').sum().to_csv(join(fldr_list['draft_picks']['grouped'],'draft_picks_by_manager.csv'),encoding=default_enc)
dp_df_slim.groupby('round').sum().to_csv(join(fldr_list['draft_picks']['grouped'],'draft_picks_by_manager.csv'),encoding=default_enc)
