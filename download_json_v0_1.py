"""this is a script to download all the jsons from the FPL Draft API
it does summary level, league level, manager level and other gw level information. 
It outputs mostly raw data, with little work done on the data.
"""


from sys import path
#from pathlib import Path
from os import chdir, getcwd, mkdir
from os.path import join, dirname, abspath
import time
import pandas as pd
import logging
from datetime import datetime
import json

#this is setting up the relative paths
setup_folder = dirname(abspath(__file__))
chdir(setup_folder)
chdir("..") #up one level
season_dir = getcwd()
chdir("..")
root_dir = getcwd()
path.append(join(setup_folder,'my_classes')) 

import FPL_4 as fpl

settings_folder = join(season_dir,'settings')
with open(join(settings_folder,'game.json'),encoding='utf8') as f:
    settings = json.load(f)

draft_folder=settings['draft']['prefix_folder']

draft_dir = join(season_dir,draft_folder)
extract_dir = join(draft_dir,'extracts')
runner=fpl.draft_runner(extract_dir) #this is the instance that will download everything
runner.create_folder(draft_dir)
runner.create_folder(extract_dir)

settings['draft']['filepath'] = extract_dir

#logging setuo
logging_dir = join(season_dir,'logging') #hardcoded for now #TODO needs to be dynamic
logging_file_name=f"setup_{datetime.now():%Y-%m-%d %H-%M}.log"
runner.create_folder(logging_dir)
logging_full_path = join(logging_dir,logging_file_name)
log_level = 20
FORMAT = "%(asctime)s - %(levelname)s - %(funcName)s: %(message)s"
logging.basicConfig(
    filename = logging_full_path,
    level = log_level,
    format = FORMAT,
    datefmt = "%Y-%m-%d %H:%M:%S")
logging.info(f'logging started')

logging.info('defining initial variables')
min_gw = 1
max_gw = None
MAX_FINAL_GW=38
manager_id_list = None #[39205]
league_id = settings['draft']['league_id'] #from the game file
chdir(extract_dir)
time_start=time.perf_counter()
logging.info(f'Initial variables defined\ntarget folder address: {extract_dir}\
    \n league id: {league_id}\
    \nGameweek range: {min_gw}-{max_gw}')

#this part downloads all the summary level information
runner.set_target_dir(join(extract_dir,'summary')) #the folder location for these downloads
runner.create_folder(runner.get_target_dir() ) 

logging.info(f'Created folder {runner.get_target_dir()} and set as target folder')
logging.info('game download starting')
runner.download('game','overall_game_status.json',lambda x: isinstance(x,dict)) #the overall status of the game
logging.info(f'Downloaded {runner.get_full_url()} into {runner.get_full_file_path()}')
if max_gw is None:
    max_gw = runner.get_json()['current_event'] #this is the GW that has just been or is in progress
runner.download('pl/event-status','current_event_status.json',lambda x: isinstance(x,dict)) #the status of the current event
logging.info(f'Downloaded {runner.get_full_url()} into {runner.get_full_file_path()}')
runner.download('top-elements','top_scorers.json',lambda x: '1' in x.keys())
logging.info(f'Downloaded {runner.get_full_url()} into {runner.get_full_file_path()}')
runner.download('bootstrap-static','bootstrap.json',lambda x: 'elements' in x.keys())
logging.info(f'Downloaded {runner.get_full_url()} into {runner.get_full_file_path()}')
runner.unpack_to_csv() #this is taking the bootstrap json and unpacking the elements into CSVs, where possible. #TODO there are significant issues with events,fixtures and settings
min_p_id = 1
max_p_id=max(runner.get_dfs()['elements']['id']) 
player_id_list = range(min_p_id,max_p_id)
logging.info(f'Player id range: {min_p_id} - {max_p_id}')
#the fixtures one just deals in the next 3 fixtures which is fine but unneccessary and limiting, we'll work it from the player info
logging.info(f'The following headings have been unpacked to CSVs: {", ".join(runner.get_dfs().keys())}')
logging.info('game download finished')

#this part downloads all the league level information
logging.info('league download starting')
league_dir = join(extract_dir,'league')
logging.info(f'creating folder {league_dir}')
runner.create_folder(league_dir)
runner.set_target_dir(league_dir)
trans_dir = join(league_dir,'transactions') #this part deals with transactions
logging.info(f'creating folder {trans_dir}')
runner.create_folder(trans_dir)
runner.set_target_dir(trans_dir)
url_suffix = f'draft/league/{league_id}/transactions' 
logging.info(f'downloading {runner.base_url}/{url_suffix}')
runner.download(url_suffix,'transactions.json',lambda x: 'transactions' in x.keys())
runner.unpack_to_csv()
lge_mem_det_dir = join(league_dir,'manager_details') #this part deals with member details
runner.create_folder(lge_mem_det_dir)
runner.set_target_dir(lge_mem_det_dir)
url_suffix = f'league/{league_id}/details'
logging.info(f'downloading {runner.base_url}/{url_suffix}') #it might be better to keep the logging in the class
runner.download(url_suffix,'league_manager_details.json',lambda x: 'league' in x.keys())
runner.unpack_to_csv()
ownership_dir = join(league_dir,'player_ownership') #this part deals with player ownership
runner.create_folder(ownership_dir)
runner.set_target_dir(ownership_dir)
if manager_id_list is None: #fetch the ids of the members of the league, unless they've already been given
    manager_id_list=[i['entry_id'] for i in runner.get_json()['league_entries']]
runner.download(f'league/{league_id}/element-status','player_ownership.json',lambda x: 'element_status' in x.keys())
runner.unpack_to_csv()
draft_pick_dir = join(league_dir,'draft_picks') #this part deals with the initial draft picks
runner.create_folder(draft_pick_dir)
runner.set_target_dir(draft_pick_dir)
runner.download(f'draft/{league_id}/choices','initial_draft_picks.json',lambda x: 'choices' in x.keys())
runner.unpack_to_csv()
trades_dir = join(league_dir,'trades') #this part deals with trades
runner.create_folder(trades_dir)
runner.set_target_dir(trades_dir)
runner.download(f'draft/league/{league_id}/trades','current_trades.json',lambda x: 'trades' in x.keys())
logging.info('league download finished')

#this part downloads all the player level information
logging.info('player download starting') 
player_dir = join(extract_dir,'player')
player_full_dir = join(player_dir,'full')
runner.create_folder(player_dir)
runner.create_folder(player_full_dir) #this will hold the large DF of fixtures and history
runner.set_target_dir(player_dir)
player_fixture_df = pd.DataFrame()
player_history_df = pd.DataFrame()

for id in player_id_list: #getting the details of each player (elements)
    logging.info(f'downloading data for player_id: {id}')
    runner.download(f'element-summary/{id}',f'player_{id}.json',lambda x: 'fixtures' in x.keys())
    dfs = runner.unpack_to_csv(save=False) #this is the history and fixturess
    for i in dfs.values():
        i['element']=id #need to add a column to show the player
    player_fixture_df = pd.concat([player_fixture_df,dfs['fixtures']]) #building the full DataFrame of fixtures
    player_history_df = pd.concat([player_history_df,dfs['history']])  #and history
logging.info("Save fixtures and history of all players")
player_fixture_df.to_csv(join(player_full_dir,'player_fixtures.csv'), encoding="utf-8-sig")
player_history_df.to_csv(join(player_full_dir,'player_history.csv'), encoding="utf-8-sig")
logging.info('player download finished')

#this part downloads all the mamager level information, there is some gw level info
logging.info('manager download starting')
gw_list = range(min_gw,max_gw+1) #don't use next event because of when we hit GW38
manager_dir = join(extract_dir,'manager')
manager_overall_dir = join(extract_dir,'manager','overall_history')
manager_event_dir = join(extract_dir,'manager','event_history')
manager_full_dir = join(manager_dir,'full')
runner.create_folder(manager_dir) #creating multiple folders
runner.create_folder(manager_overall_dir)
runner.create_folder(manager_event_dir)
runner.create_folder(manager_full_dir)
#runner.create_folder(join(manager_event_dir,'full'))
#runner.create_folder(join(manager_event_dir,'subs'))

manager_picks_df = pd.DataFrame()
manager_auto_subs_df = pd.DataFrame()
manager_entries_df = pd.DataFrame()
manager_history_df = pd.DataFrame()

for m in manager_id_list: #getting details of each manager
    logging.info(f'downloading data for manager_id: {m}')
    manager_file_prefix=f"manager_{m}"
    runner.set_target_dir(manager_overall_dir)
    runner.download(f'entry/{m}/history',f'{manager_file_prefix}.json',lambda x: 'history' in x.keys())
    entry_json = runner.get_json()['entry'] 
    del entry_json['league_set'] #get rid of the league set attribute
    runner.unpack_to_csv(['history','entry'],save=False) #TODO gonna put them all together
    dfs = runner.get_dfs()
    manager_entries_df = pd.concat([manager_entries_df,dfs['entry']])
    manager_history_df = pd.concat([manager_history_df,dfs['history']])
    team_name = dfs['entry']['name']
    logging.info(f'Team name: {team_name}')#f'Team identified as {manager_entries_df['name']})

    runner.set_target_dir(manager_event_dir)
    for gw in gw_list: #per gw
        logging.info(f'Downloading team info for gw {gw}')
        runner.download(f'entry/{m}/event/{gw}',f'{manager_file_prefix}_{gw}.json',lambda x: isinstance(x,dict) and 'picks' in x.keys())
        #for f in ['picks','subs']:
            #f'{f}/{manager_file_prefix}_{gw}'
        runner.unpack_to_csv(('picks','subs'),prefix=False,save=False) #saving picks and subs to csvs with custom names, 'entry history' is pointless 
        gw_df_list = runner.get_dfs() #this part is creating a df for all picks and one for all subs 
        for df in gw_df_list.values(): #need to assign values so the gws and managers are distinct when joined together
            df['manager'] = m
            df['event'] = gw
        manager_picks_df = pd.concat([manager_picks_df,gw_df_list['picks']])
        manager_auto_subs_df = pd.concat([manager_auto_subs_df,gw_df_list['subs']])

runner.set_target_dir(manager_full_dir)
manager_entries_df.to_csv(join(manager_full_dir,'manager_entries.csv'), encoding="utf-8-sig")

manager_history_df_new=manager_history_df.copy()
manager_history_df.to_csv(join(manager_full_dir,'manager_history.csv'), encoding="utf-8-sig")

picks_df=manager_picks_df.reset_index(drop=True)
picks_df_new=picks_df.copy()
picks_df['played'] = picks_df['position'].map(lambda x: True if x <= 11 else False) #if the position of the player is top 11, they played (otherwise they were a sub)
#picks_df.drop(['multiplier','is_captain','is_vice_captain'],axis = 1,inplace=True) #these aren't needed for draft
picks_df_new.to_csv(join(manager_full_dir,'manager_picks_updated.csv'), encoding="utf-8-sig")
picks_df.to_csv(join(manager_full_dir,'manager_picks.csv'), encoding="utf-8-sig")

subs_df=manager_auto_subs_df.reset_index() #doing things with the subs df
subs_df['index'] = subs_df['index'].map(lambda x:x + 1) #the index starts at 0, I'd like it to start at 1
subs_df=subs_df.rename(columns={'index':'seq_per_wk_per_manager'}) #the index actually shows how many were done in one week
subs_df.to_csv(join(manager_full_dir,'manager_auto_subs.csv'), encoding="utf-8-sig")

logging.info('manager download finished')

#this part downloads all the rest of the gw level information
logging.info('gw download starting')
full_gw_list = range(min_gw,MAX_FINAL_GW+1) #this part needs all gws to set up fixtures
gw_dir = join(extract_dir,'gw_info')
gw_full_dir = join(gw_dir,'full')
runner.create_folder(gw_dir)
runner.create_folder(gw_full_dir)
runner.set_target_dir(gw_dir)
gw_df = pd.DataFrame()
for gw in full_gw_list: #this will go from 1-38
    logging.info(f'downloading data for GW{gw}')
    runner.download(f'event/{gw}/live',f'gw_{gw}.json',lambda x: 'elements' in x.keys())
    runner.unpack_to_csv('fixtures',save=False) #the other bits are fiddly and can be got from player data
    df = runner.get_dfs()['fixtures']
    gw_df = pd.concat([df,gw_df])
    gw_df.to_csv(join(gw_full_dir,'all_gw_fixtures.csv'), encoding="utf-8-sig") #creating the full gw fixtures df TODO check it's actually needed
logging.info('gw download finished')

#this part does some basic reconciliation
#TODO add up all GWs, add up total scores and make sure they all match
#TODO group by each player, and compare overall score to total scores

time_end=time.perf_counter()
time_full=time_end - time_start
logging.info(f'Script time taken: {time_full}')

