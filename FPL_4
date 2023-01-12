import json
import requests
from os import mkdir
import pandas as pd
from os.path import join, splitext

"""this needs a better name, but for now is good enough
it holds the classes used in the rest of the code""""

class File(object):
    """used to manipulate files"""
    def __init__(self, 
    folder=None,
    subfolder=None,
    file_name=None):
        self.folder = folder
        self.subfolder = subfolder
        self.file_name = file_name

    def load_game_settings(self,name):
        with open(join(self.folder,self.file_name),encoding='utf8') as f:
            game_settings = json.load(f)
        self.game_settings = game_settings

class draft_runner:
    """the main class used at the moment""" 
    def __init__(self,
    target_folder='C:/Users/Knick/Documents/Python/Projects - Independent/FPL Main/testing',
    base_url='https://draft.premierleague.com/api/'
    ):
        self.target_folder=target_folder
        self.base_url=base_url
        self.full_url=base_url
        self.dfs={}
    
    def create_folder(self,new_folder,use_parent_dir=False):
        if isinstance(new_folder,str):
            new_folder_list=[new_folder]
        else:
            new_folder_list=new_folder
        for fldr in new_folder_list:
            if use_parent_dir is True:
                parent_dir=self.target_folder
                full_dir_path=f'{parent_dir}/{fldr}'
            else:
                full_dir_path=fldr
            try:
                mkdir(f'{full_dir_path}/')
            except WindowsError:
                print(f"Folder {fldr} already created") #TODO this is too all encompasing
            except Exception as err:
                print(err)
            #if fldr not in parent_fldr_list:
        return True

    def set_target_dir(self,target_folder):
        self.target_folder=target_folder

    def get_target_dir(self):
        return self.target_folder

    def download(self,url_suffix,file_name,url_check=None,save=True):
        """use this method to download jsons that are not specific to a league, team or player"""
        full_url=f'{self.base_url}/{url_suffix}' #this is the full url of the json that is being downloaded
        path=f'{self.target_folder}/{file_name}' #this is the path it needs to be downloaded into
        #TODO these can be None and if so use the self. version, also make a self version
        url_json=url_to_json(full_url)
        if url_check is not None: #if there is a url_check required
            if not(url_check(url_json)): #if it fails...
                return None   #TODO this should be a logging warning too
        if save == True: #might make this a dev option, but for now it's useful to avoid saving if not needed             
            #TODO if ending isn't JSON, make it json
            save_json(url_json,path) #if it doesn't fail, then save it
        self.json=url_json
        self.full_url=full_url
        self.url_suffix=url_suffix
        self.file_name=file_name #TODO some of these need putting in the init
        self.full_file_path=path
        return url_json

    def get_json(self):
        return self.json

    def set_json(self,json):
        self.json=json

    def get_base_url(self):
        return self.base_url

    def set_base_url(self,base_url):
        self.base_url=base_url

    def get_full_url(self):
        return self.full_url 
    #n need for a set_full_url since it's made from the base and the previx
    
    def get_file_name(self):
        return self.file_name

    def get_full_file_path(self):
        return self.full_file_path

    def get_dfs(self):
        return self.dfs

#class draft_runner(draft_runner_light):
    def unpack_to_csv(self,json_columns=None,col_names=None,prefix=True,save=True):
        """this takes all the headers from a json and makes them into CSVs"""
        my_json=self.json 
        all_keys=my_json.keys()
        filename_no_ext = splitext(self.file_name)[0]
        dfs={}
        #this bit is just dealing with possible combinations of arguments
        if json_columns is None: #if no keys are selected
            selected_cols=all_keys #use all the keys in the json
        elif isinstance(json_columns,str): #if only one is input, put it in a list so it can be looped through
            selected_cols=[json_columns]
        else:
            selected_cols=json_columns
        if col_names is None: #if no column names are given, then...
            if prefix: #use the names of the selected cols, with the file_name as a prefix
                col_names=[f'{filename_no_ext}_{i}' for i in selected_cols]
            else: #just use the selected cols 
                col_names=selected_cols
        elif isinstance(col_names,str):
            col_names=[col_names]
        elif len(col_names)!=len(selected_cols):
            print("Mismatch of columns and names") #TODO throw an exception if the lengths of the header and name lists are different
        #now it's the actual meat
        for i,j in zip(selected_cols,col_names): 
            data=my_json[i]
            try:
                if isinstance(data,dict):
                    data=[data]
                df=pd.DataFrame(data)
                address=join(self.target_folder,f'{j}.csv')
                if len(df) and save:
                    df.to_csv(address, encoding="utf-8-sig")
                dfs[i]=df
            except Exception as e:
                print(e)
        self.dfs = dfs
        return dfs

def url_to_json(full_url):
    r=requests.get(full_url)
    url_json=r.json()
    return url_json

def save_json(url_json,target_dir):
    with open(target_dir,'w',encoding='utf8') as output:
        json.dump(url_json,output,indent=4,ensure_ascii=False)
    return True

def save_json_to_csv(data,address,index=""): #json_save_as_csv
    df=pd.DataFrame(data)
    if index != "":
        df.set_index(index,inplace=True)
    df.to_csv(address, encoding="utf-8-sig")
    return df

