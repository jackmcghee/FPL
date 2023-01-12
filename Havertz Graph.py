"""A Havertz graph is a bar plot that tracks a player's gw scores versus their ownership
it can be used to identify a manager's hilarious misstep as they transfer a player out, only for said player to 
then get a haul of points"""

import pandas as pd
from os import chdir, getcwd
from os.path import join, abspath, dirname
import matplotlib.pyplot as plt
import seaborn as sns
from sys import path
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
download_dir=join(draft_dir,'extracts')
analysis_dir=join(draft_dir,'processed')
runner = fpl.draft_runner(join(analysis_dir,'graphs'))
runner.create_folder('bar_plots',use_parent_dir=True)
runner.create_folder('bar_plots/gw_reviews',use_parent_dir=True)

player_history_dir=join(analysis_dir,'player_history')

all_GWs=pd.read_csv(join(player_history_dir,'player_history_annotated.csv'))
plyer = all_GWs.loc[all_GWs['element']==30] #TODO this obviously needs to be configurable
plyer_graph_data = plyer.loc[:,['total_points','event','manager','started']]
plyer_graph_data['manager'] = plyer['manager'].fillna('None')
plyer_graph_data['started'] = plyer['started']#.fillna('False')
plyer_graph_data_final = plyer_graph_data.reset_index(drop=True)
player_owners = plyer_graph_data_final['manager'].unique()

sns.set(rc = {'figure.figsize':(10,10)})
sns.set_palette("pastel")

games_benched = ['Sub' if x == 'False' else None for x in plyer_graph_data_final['started']]

fig, ax = plt.subplots()
ax = sns.barplot(data=plyer_graph_data_final,x='event',y='total_points',
                 hue='manager',dodge=False,saturation=0.7,hue_order=player_owners)
for x,p in zip(ax.patches,games_benched):
    ax.annotate(p, 
                   (x.get_x() + x.get_width() / 2., 1), 
                   ha = 'center', va = 'center', 
                   xytext = (0, 9), 
                   textcoords = 'offset points',
                   color='black')
plt.xlabel("GW", size=14)
plt.ylabel("GW Points", size=14)
player_name = list(plyer.web_name)[0]
plt.title(f'{player_name} Points and Ownership 22/23 up to GW16',size=20)
plt.savefig(f"{analysis_dir}/graphs/bar_plots/gw_reviews/{player_name}")
plt.show()
