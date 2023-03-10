"""this part uses the data generated by the wrangling_csvs scripts to generate graphs"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from os.path import join
from sys import path
path.append('C:/Users/Knick/Documents/Python/Projects - Independent/FPL Main/FPL/Scripts/my_classes')
target_folder='C:/Users/Knick/Documents/Python/Projects - Independent/FPL Main/22_23/draft/processed'
import FPL_4 as fpl
runner=fpl.draft_runner(target_folder)

manager_dir=join(target_folder,'manager_history')
graph_dir=join(target_folder,'graphs')
hm_dir=join(graph_dir,'heat_maps')
lp_dir=join(graph_dir,'line_plots')
hist_dir=join(graph_dir,'hist_plots')
summary=pd.read_csv(join(manager_dir,'manager_summary.csv'))

def manager_summary_heatmap(df,column='gw_points'):
    sns.set(rc={'figure.figsize':(11.7,8.27)})
    fig, ax = plt.subplots()
    a = summary.loc[:,[column,'manager','event']]
    a_prep = a.pivot('event','manager')
    cols = a_prep.columns
    new_cols = [i[1] for i in cols]
    a_prep.columns=new_cols
    col_p = column.title()
    ax.set(xlabel='GW', ylabel=col_p,title=f"{col_p} v GW")
    fig = sns.heatmap(a_prep.T,cmap="PiYG",annot=True)
    plt.savefig(f'{hm_dir}/manager_{column}_hm.png')

def add_zero_gw(df,manager_col='manager'):
    df_new=df.copy()
    d = pd.DataFrame(0, index=range(len(df_new[manager_col].unique())), columns=df_new.columns)
    d[manager_col]=df_new[manager_col].unique()
    df_new=pd.concat([d,df_new])
    return df_new

def manager_summary_lineplot(df,column='total_points'):
    sns.set(rc={'figure.figsize':(15,8.27)})
    fig, ax = plt.subplots()
    if '_rank_' in column:
        ax.invert_yaxis()
    sns.lineplot(x=df['event'],y=df[column],hue=df['manager'])
    plt.savefig(f'{lp_dir}/manager_{column}_lp.png')

def check_for_zeros(df):
    """this will check if any GW has no points associated with it"""
    pass

runner.create_folder(graph_dir)
runner.create_folder([hm_dir,lp_dir,hist_dir])
hm_col_list = ['gw_points','points_on_bench','attempted_transfers','successful_transfers']
for col in hm_col_list: #this produces heatmaps for each attribute in the list above
    manager_summary_heatmap(summary,col)

new_summary=add_zero_gw(summary)
#lp_col_list = ['total_points','total_points_rank_by_gw']
#for col in lp_col_list: #this produces linepots
#    manager_summary_lineplot(new_summary,col)

sns.set(rc={'figure.figsize':(15,8.27)})

#this is creating a plot to chart the running total points of each manager, for each GW 
fig, ax = plt.subplots()
column='total_points'
sns.lineplot(x=new_summary['event'],y=new_summary[column],hue=new_summary['manager'])
ax.set(xlabel='GW', ylabel='Total Points',title="Running score of total points")
plt.savefig(f'{lp_dir}/manager_{column}_lp.png')

#this is creating a plot to chart the rank of each manager's total points, for each GW 
fig, ax = plt.subplots()
column='total_points_rank_by_gw' 
sns.lineplot(x=summary['event'],y=summary[column],hue=summary['manager'])
ax.invert_yaxis()
ax.set(xlabel='GW', ylabel='Rank',title="Rank of total points by GW")
plt.savefig(f'{lp_dir}/manager_{column}_lp.png')

#this is creating a histogram of the gw points that all managers have recorded
sns.set(rc={'figure.figsize':(20,20)})
fig, ax = plt.subplots()
summary_no_gw_7 = summary.loc[summary['event']!=7] #Queen died, no games TODO make this generic for any GW with 0 scores
sns.displot(summary_no_gw_7['gw_points']).set(title="Distribution of all GW Scores")
plt.savefig(f'{hist_dir}/all_total_points_dp.png',bbox_inches='tight')
