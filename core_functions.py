import difflib
import json
import operator
import os
import random
from pprint import pprint

import numpy as np
import pandas as pd
import requests
from dateutil.parser import parse
from pandas import compat
from pandas.api.types import is_numeric_dtype

from settings import Settings
setting = Settings()
# from cleaning_module import CleaningModule
# cleaning_mod = CleaningModule()


class CoreFunctions():

    def changeStringToDate(self, df_change):
        # print('IN function')
        for col in df_change.columns:
            try:
                if df_change[col].dtype == 'object':
                    try:
                        df_change[col] = pd.to_datetime(df_change[col])
                    except:
                        continue
            except:
                continue
        # print([col for col in df_change.columns if df_change[col].dtype == 'datetime64[ns]'])
        return df_change

    def get_sheets(self, file_name):
        xl = pd.ExcelFile(file_name)
        return xl.sheet_names

    def read_excel_sheet(self, file_name, sheet=None):
        self.compare_dataframes = {}
        if sheet:
            self.dataFrame = self.changeStringToDate(pd.read_excel(file_name, sheet))
            return True
        else:
            self.dataFrame = self.changeStringToDate(pd.read_excel(file_name))
            return True
        pass

    def get_numeric_statistics(self):
        return self.dataFrame.describe(include=[np.number]).to_dict()

    def get_dataframe(self, cmp_key=""):
        if cmp_key == "":
            return self.dataFrame.copy()
        elif cmp_key not in list(self.compare_dataframes.keys()):
            index = str(int(cmp_key[-1])-1)
            old_key = cmp_key[:-1]+index
            self.compare_dataframes[cmp_key] = self.compare_dataframes[old_key].copy()

        return self.compare_dataframes[cmp_key].copy()

    def set_dataframe(self, joined_col):
        try:
            self.compare_dataframes
        except Exception as e:
            self.compare_dataframes = {}
        for df_dict in self.dataFrameList:
            if df_dict['Joined on'] == joined_col:
                self.dataFrame = self.changeStringToDate(df_dict['DataFrame'].copy())
                # print([col for col in self.dataFrame.columns if self.dataFrame[col].dtype == 'datetime64[ns]'])
                return True
            else:
                pass
        return False

    def set_cleaned_data(self):
        # self.dataFrame = self.dataFrame
        # print('Dataframe is set properly')
        # print(self.dataFrame.columns)
        return True

    def set_dataframes_list(self, rdf):
        self.dataFrameList = rdf
        return True
    
    def get_columns(self, cmp_key=""):
        return list(self.dataFrame.columns if cmp_key == "" else self.compare_dataframes[cmp_key].columns)

    def modify_columns(self, col):
        self.dataFrame.columns = col
        return True

    def is_df_set(self):
        return (not self.dataFrame.empty)

    def replace_all_missing_values(self, miss, value=0, cmp_key=""):
        if cmp_key == "":
            if miss == 'mean':
                self.dataFrame.fillna(self.dataFrame.mean(), inplace=True)
            elif miss=='zero':
                self.dataFrame.fillna(0, inplace=True)
            elif miss=='value':
                self.dataFrame.fillna(value, inplace=True)
            elif miss=='previous_value':
                self.dataFrame.fillna(method='ffill', inplace=True)
        else:
            if miss == 'mean':
                self.compare_dataframes[cmp_key].fillna(self.compare_dataframes[cmp_key].mean(), inplace=True)
            elif miss=='zero':
                self.compare_dataframes[cmp_key].fillna(0, inplace=True)
            elif miss=='value':
                self.compare_dataframes[cmp_key].fillna(value, inplace=True)
            elif miss=='previous_value':
                self.compare_dataframes[cmp_key].fillna(method='ffill', inplace=True)

        return True

    def remove_missing_values(self, df_copy, col_list):
        df_copy.dropna(subset=col_list, inplace=True)
        return df_copy

    def date_to_string(self, df_copy):

        for col in df_copy.select_dtypes(include=['datetime64']).columns:
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
        return df_copy


    def missing_value_count(self):
        return self.dataFrame.isnull().sum().to_dict()

    def drop_duplicates(self, col):
        self.dataFrame[col].duplicated().sum()

    def change_col_to_categeory(self, col):
        self.dataFrame[col] = self.dataFrame[col].astype('category')

    def replace_missing_value_col(self,col, miss, value=0):

        if miss == 'mean':
            self.dataFrame[col].fillna(self.dataFrame[col].mean(), inplace=True)
        elif miss=='zero':
            self.dataFrame[col].fillna(0, inplace=True)
        elif miss=='value':
            self.dataFrame[col].fillna(value, inplace=True)
        elif miss=='previous_value':
            self.dataFrame[col].fillna(method='ffill', inplace=True)
        elif miss == 'skip':
            self.dataFrame.dropna(subset=[col],inplace=True)
        return True       

    def get_stat(self):
        return self.dataFrame.describe().to_dict()

    def change_date(self, start, end, col, df_copy):
        self.dataFrame_copy = self.changeStringToDate(self.dataFrame.copy())
        df_copy[col] = pd.to_datetime(df_copy[col])
        df_copy = df_copy[(df_copy[col] >= start) & (df_copy[col] <= end)]
        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
        return df_copy

    def to_dict_fillna(self, data):
        return dict((k, v.fillna(0).to_dict()) for k, v in compat.iteritems(data))

    def get_sorting_by_values(self, in_dict):
        sorted(in_dict.items(),key=operator.itemgetter(1),reverse=True)
        pass


    def get_coreletion (self, col_name, sort = True):
        if col_name in self.coreletion_col_list:    
            if sort:
                return self.get_sorting_by_values(self.dataFrame.corr().ix[col_name, :-1].fillna(0).to_dict())
            else:
                return self.dataFrame.corr().ix[col_name, :-1].fillna(0).to_dict()
        else:
            return False

    def filter_data(self, fil, total):
        return [x for x in filter(lambda x:any(b in fil for b in x), total)]

    def filter_data_exact_pairs(self,x,y):
        return_list = []
        
        for drilled_ele in y:
            try:
                x_str = '|'.join(x)
            except:
                x_str = '|'.join(str(i) for i in x)

            # When the chart is drilled more than once, the drilled_ele will be list of tuples
            if type(drilled_ele) == type(()): 
                x_str = '|'+x_str+'|'
                drilled_list = '|'+'|'.join([str(ent) for ent in drilled_ele])+'|'
                if x_str in drilled_list:
                    return_list.append(drilled_ele)
                else:
                    continue
            # When the chart is drilled only once, the drilled_ele will be list of strings
            else:
                if x_str == drilled_ele:
                    return_list.append(drilled_ele)
        return return_list

    def get_key_values(self, x_axis, y_axis, drilled_att, df_copy):
        # This function returns the column values of y-axis element w.r.t. x-axis element
        # Example : ADID vs Age 
        # Returned will be the dictionary : { 'adid1':value, 'adid2':value, ... } 

        result = {}
        x_axis_element = x_axis[-1]
        df = df_copy  # self.dataFrame if cmp_key == "" else self.compare_dataframes[cmp_key]
        try:
            df[y_axis] = pd.to_numeric(df[y_axis])
        except:
            pass
        grp = df.groupby(x_axis[:-1]+[y_axis])
        try:
            if len(x_axis) > 1:
                abc = self.filter_data_exact_pairs(drilled_att, grp.groups.keys())
                for group_by_df in [x for x in list(grp) if x[0] in abc]:
                    # result[group_by_df[1].iloc[0][x_axis_element]] = round(group_by_df[0][-1],2)
                    for x_ele in group_by_df[1][x_axis_element]:
                        result[x_ele] = round(float(group_by_df[0][-1]),2)
            else:
                for group_by_df in list(grp):
                    # result[group_by_df[1].iloc[0][x_axis_element]] = round(group_by_df[0],2)
                    for x_ele in group_by_df[1][x_axis_element]:
                        result[x_ele] = round(float(group_by_df[0]),2)
        except:
            print("EXCEPPPPPTTTTTTTT")

        print('<<<',result)
        return result

    def get_pivot_data(self, x_axis, drilled_att, df_copy):

        result = {}
        df = df_copy # self.dataFrame if cmp_key == "" else self.compare_dataframes[cmp_key]
        
        if drilled_att != []:
            grp = df.groupby(x_axis[:-1])

            if len(x_axis) > 1:
                abc = self.filter_data_exact_pairs(drilled_att, grp.groups.keys())
                for group_by_df in [a for a in list(grp) if a[0] in abc]:
                    result[group_by_df[0]] = group_by_df[1]

            else:
                for group_by_df in list(grp):
                    result[group_by_df[0]] = group_by_df[1]

            return list(result.values())[0].to_json(orient='records')
        else:
            return df.to_json(orient='records')
    
    def get_drilled_data(self, df_copy, x_axis, y_axis=None):

        df = df_copy  # self.dataFrame if cmp_key == "" else self.compare_dataframes[cmp_key]
        
        last_groupby_col = x_axis[-1]
        try:
            if df[last_groupby_col].dtype == 'object' or df[last_groupby_col].dtype == 'datetime64[ns]':
                df[last_groupby_col] = pd.to_datetime(df[last_groupby_col])
                df.sort_values(by=[last_groupby_col], inplace=True)
                df[last_groupby_col] = df[last_groupby_col].dt.strftime('%Y-%m-%d')
        except:
            pass
        grp = df.groupby(x_axis, sort=False)
        total_dict = {}
        total_dict_count = {}
        x_data = []
        describe_obj = ''
        describe_obj_keys =[]
        if len(x_axis) > 1:
            abc = self.filter_data_exact_pairs(y_axis, grp.groups.keys())
            for group_by_df in [a for a in list(grp) if a[0] in abc]:
                item = group_by_df[0][-1]
                if item not in x_data:
                    x_data.append(item)
                    pass
                else:
                    pass
                describe_obj  = self.to_dict_fillna(group_by_df[1].describe().round(2))
                agg_types = ['mean', 'std', 'min', 'max']
                describe_obj_keys = list(describe_obj.keys())
                da_count  = group_by_df[1].count().to_dict()
                total_dict_count[group_by_df[0][-1]] = da_count[x_axis[-1]]

                for aggregation_type in agg_types:
                    if aggregation_type in total_dict:
                        if group_by_df[0][-1] in total_dict[aggregation_type]:
                            for aggregation in describe_obj_keys:
                                if aggregation_type in describe_obj[aggregation]:
                                    total_dict[aggregation_type][group_by_df[0][-1]].extend([describe_obj[aggregation][aggregation_type]])
                        else:
                            total_dict[aggregation_type][group_by_df[0][-1]]=[]
                            for aggregation in describe_obj_keys:
                                if aggregation_type in describe_obj[aggregation]:
                                    total_dict[aggregation_type][group_by_df[0][-1]].extend([describe_obj[aggregation][aggregation_type]])                    
                    else:
                        total_dict[aggregation_type] = {}
                        if group_by_df[0][-1] in total_dict[aggregation_type]:
                            for aggregation in describe_obj_keys:
                                if aggregation_type in describe_obj[aggregation]:
                                    total_dict[aggregation_type][group_by_df[0][-1]].extend([describe_obj[aggregation][aggregation_type]])                    
                        else:
                            total_dict[aggregation_type][group_by_df[0][-1]]=[]
                            for aggregation in describe_obj_keys:
                                if aggregation_type in describe_obj[aggregation]:
                                    total_dict[aggregation_type][group_by_df[0][-1]].extend([describe_obj[aggregation][aggregation_type]])

        else:
            for group_by_df in list(grp):
                x_data.append(group_by_df[0])
                agg_types = ['mean', 'std', 'min', 'max']
                describe_obj = self.to_dict_fillna(group_by_df[1].describe().round(2))
                describe_obj_keys = list(describe_obj.keys())
                da_count  = group_by_df[1].count().to_dict()
                total_dict_count[group_by_df[0]] = da_count[x_axis[-1]]
                for aggregation_type in agg_types:
                    if aggregation_type not in total_dict:
                        total_dict[aggregation_type] ={}
                    else:
                        pass
                    if group_by_df[0] not in total_dict[aggregation_type]:
                        total_dict[aggregation_type][group_by_df[0]]=[]
                    else:
                        pass
                    
                    for aggregation in describe_obj_keys:
                        if aggregation_type in describe_obj[aggregation]:
                            total_dict[aggregation_type][group_by_df[0]].extend([describe_obj[aggregation][aggregation_type]])
        return x_data, describe_obj_keys,total_dict, total_dict_count

    def get_sum_data(self, x_axis, y_axis, df_copy, drilled_att=None):

        df = df_copy  # self.dataFrame if cmp_key == "" else self.compare_dataframes[cmp_key]
        grp = df.groupby(x_axis)
        total_sum_dict = {}
        if len(x_axis) > 1:
            abc = self.filter_data_exact_pairs(drilled_att, grp.groups.keys())
            for group_by_df in [a for a in list(grp) if a[0] in abc]:
                sum_dict  = group_by_df[1].sum(numeric_only=True).to_dict()
                total_sum_dict[group_by_df[0][-1]] = sum_dict[y_axis]
        else:
            for group_by_df in list(grp):
                sum_dict  = group_by_df[1].sum(numeric_only=True).to_dict()
                total_sum_dict[group_by_df[0]] = sum_dict[y_axis]

        return total_sum_dict

    def value_axes_validator(self, x_axis, y_axis, drilled_att, df_copy):

        df = df_copy  # self.dataFrame if cmp_key == "" else self.compare_dataframes[cmp_key]
        x_axis_element = x_axis[-1]

        if len(x_axis) > 1:
            grp = df.groupby(x_axis[:-1])
            abc = self.filter_data_exact_pairs(drilled_att, grp.groups.keys())
            for groupby_obj in [x for x in list(grp) if x[0] in abc]:
                if is_numeric_dtype(groupby_obj[1][y_axis]):
                    print(True)
                if not is_numeric_dtype(groupby_obj[1][y_axis]):
                    try:
                        groupby_obj[1][y_axis] = pd.to_numeric(groupby_obj[1][y_axis])
                    except:
                        return False
                if (abs(len(groupby_obj[1].groupby(x_axis)) - len(groupby_obj[1].index)) in [0, 1, 2]):
                    return True
                return False
        else:
            try:
                if is_numeric_dtype(df[y_axis]) and (abs(len(df.groupby(x_axis_element)) - len(df.index)) in [0, 1, 2]):
                    return True
                return False
            except:
                return False

    def sum_axes_validator(self, y_axis, df_copy):
        try:
            # if is_numeric_dtype(self.dataFrame[y_axis] if cmp_key == "" else self.compare_dataframes[cmp_key][y_axis]):
            if is_numeric_dtype(df_copy[y_axis]):
                return True
            else:
                try:
                    pd.to_numeric(df_copy[y_axis])
                    return 5
                except:
                    return False
        except:
            return False

    def get_all_date_columns(self, cmp_key=""):
        if cmp_key == "":
            return [col for col in self.dataFrame.columns if self.dataFrame[col].dtype == 'datetime64[ns]']
        return [col for col in self.compare_dataframes[cmp_key].columns if self.compare_dataframes[cmp_key][col].dtype == 'datetime64[ns]']

    def get_all_min_max_date_values(self,date_columns, cmp_key=""):

        df = self.dataFrame if cmp_key == "" else self.compare_dataframes[cmp_key]
        date_min_max_columns = []
        for i in range(len(date_columns)):
            min_max_date = []
            min_max_date.append(df[date_columns[i]].min().date())
            min_max_date.append(df[date_columns[i]].max().date())
            date_dict = {}
            date_dict[date_columns[i]] = min_max_date
            date_min_max_columns.append(date_dict)

        return date_min_max_columns


    def get_numeric_statistics(self):
        return self.dataFrame.describe(include=[np.number]).to_dict()

    def get_statistics_all(self):
        return self.dataFrame.describe(include='all').to_dict()
    
    def get_unique_value(self, col):
        return list(self.dataFrame[col].unique())
    
    def get_frequency_with_col(self, col):
        return self.dataFrame[col].value_counts().to_json(date_format='iso')
        pass

    def get_missing_values_by_columns(self):
        freq_count = self.dataFrame.isnull().sum().to_dict()
        freq_count.update({'all': self.dataFrame.isnull().sum().sum()})
        return freq_count
        pass    

    def get_duplicate_records(self):
        return str(sum(self.dataFrame.duplicated()))
    
    def drop_duplicate(self):
        self.dataFrame = self.dataFrame.drop_duplicates(subset=None, keep='first')
        
        pass

    def export_excel(self, random_name = None, sheet=None, file_name=""):
        if file_name:
                
            if not os.path.exists('Backend/uploads/cleaned_files'):
                print('Not found')
                os.makedirs('Backend/uploads/cleaned_files')
            else:
                print('found')
            self.dataFrame.to_excel('Backend/uploads/cleaned_files/'+file_name+'.xlsx')
            return True
        else:
            if not os.path.exists('export'):
                os.makedirs('export')
            if not random_name:
                random_name = str(random.randint(5000, 10000)) + ".xlsx"
            if not sheet:
                sheet = 'sheet1'
            self.dataFrame.to_excel('Backend/export/' +random_name ,sheet_name=sheet, engine='xlsxwriter')
            return random_name

    def get_posible_coreletion_col(self):
        self.coreletion_col_list = self.dataFrame.select_dtypes(include=[np.number]).columns.tolist()
        return self.coreletion_col_list

    def create_new_column(self, new_col, old_col1, old_col2, operator, num = False):
        
        if num:
            self.dataFrame[new_col] = eval('self.dataFrame[old_col1]'+ operator[0]+ '(self.dataFrame[old_col2]'+ operator[1] + 'self.dataFrame[old_col1].'+ operator[2]+'())')
        else:
            self.dataFrame[new_col] = eval('self.dataFrame[old_col1]'+ operator[0]+ '(self.dataFrame[old_col2]'+ operator[1] + 'num)')
        return True
    
    def replace_values(self, replace_dict={},col=None):
        if col:
            self.dataFrame[col] = self.dataFrame[col].map(replace_dict)
        else:
            self.dataFrame.replace(replace_dict, inplace = True)
        
        pass
    
    def get_data_type(self):
        return self.dataFrame.dtypes.apply(lambda x: x.name).to_dict()
        pass

    def change_data_type(self, change_dt):
        try:
            self.dataFrame = self.dataFrame.astype(change_dt)
            return True
        except Exception as e:
            return False
        pass

    def get_all_data(self, export_type, start_index, end_index):
        if export_type == 'col':
            return self.dataFrame.iloc[start_index:end_index].to_json(date_format='iso')
        elif export_type == 'records':
            return self.dataFrame.iloc[start_index:end_index].to_json(orient='records', date_format='iso')
        elif export_type == 'index':
            return self.dataFrame.iloc[start_index:end_index].to_json(orient='index', date_format='iso')
        else:
            return self.dataFrame.iloc[start_index:end_index].to_json(orient='split', date_format='iso')
        pass

    def replace_nan_with_Null(self):
        self.dataFrame = self.dataFrame.replace(np.nan, 'Null')
        
        
    def replace_Null_with_nan(self):
        self.dataFrame = self.dataFrame.replace('Null', np.nan)


    def get_regex_for_col(self, col):
        re_op = [json.loads(requests.post(set_obj.regex_config, json=str(a)).text) for a in list(self.dataFrame[col])]
        return_dict = {}
        for ele in re_op:
            if ele['label'] in return_dict:
                return_dict[ele['label']]+=1
            else:
                return_dict[ele['label']]=1
        return return_dict
        pass

    def get_regex_for_all_csv(self):
        return_dict = {}
        for col in list(self.dataFrame.columns):

            re_op = [json.loads(requests.post(set_obj.regex_config, json=str(a)).text) for a in list(self.dataFrame[col])]
            if col in return_dict:
                for ele in re_op:
                    if ele['label'] in return_dict[col]:
                        return_dictp[col][ele['label']]+=1
                    else:
                        return_dict[col][ele['label']]=1
            else:
                return_dict[col] = {}
                for ele in re_op:
                    if ele['label'] in return_dict[col]:
                        return_dict[col][ele['label']]+=1
                    else:
                        return_dict[col][ele['label']]=1
        return return_dict
        pass

    def text_similarity(self, col, thresold = 60):
        # print(self.get_frequency_with_col(col))
        freq  = list(self.dataFrame[col].value_counts(sort=True).to_dict().keys())
        simi = {}
        for i in range(len(freq)):
            for j in range(len(freq[i:])):
                if freq[i]==freq[j]:
                    continue
                else:
                    if (difflib.SequenceMatcher(None,freq[i],freq[j])).ratio()*100> thresold:
                        simi[freq[i]]=freq[j]
                    else:
                        continue
        return simi

        pass

    def diff_checker(self, file2, file1):

        df1 = pd.read_excel(file1, sheet = 'Main Sheet')
        df2 = pd.read_excel(file2, sheet = 'Main Sheet')
        common_col = [col for col in list(df1.columns) if col in list(df2.columns)]
        ab = df1.where(df1.values!=df2.loc[ : , df1.columns ].values)
        
        ab.dropna(how='all', inplace = True)


        if ab.size > 0:
            ab.fillna('Unchanged', inplace= True)
            if not os.path.exists('export'):
                os.makedirs('export')
            changes = str(random.randint(50505, 2505050)) + ".xlsx"
            ab.to_excel('Backend/export/'+changes, index=False)

            changed_df= df2[common_col].iloc[ab.index.tolist()].to_dict()
            changed_file = str(random.randint(50505, 2505050)) + ".xlsx"
            ab.to_excel('Backend/export/'+changed_file, index=False)
            
            return changes, changed_file
        else:
            return None, None

        pass

    def replace_value_col(self, col, value, replacement):
        if value in list(self.dataFrame[col].unique()):
            self.dataFrame.loc[self.dataFrame[col] == value, col] = replacement
            return True
        else:
            return False

    def delete_col(self, col):

        if col in list(self.dataFrame.columns):
            self.dataFrame.drop(col, axis=1, inplace=True)
            return True
        else:
            return False
    
    def get_uploaded_files(self):
        return [ele for ele in os.listdir('BackEnd/uploads/') if ele.endswith('.xlsx')]
        pass

    def get_side_tile_name(self, tile):
        
        sides = []
        if tile:
            for side in setting.sideTileMapping:
                if tile in setting.sideTileMapping[side]:
                    sides.append(side)

        return sides

    def get_tile_index(self, tile):
        for side in setting.sideTileMapping:
            if tile in setting.sideTileMapping[side]:
                return setting.sideTileMapping[side].index(tile)

        return -1

    def set_dict_for_present(self, dict_key):
        self.compare_dataframes[dict_key] = self.changeStringToDate(self.dataFrame_copy.copy())

    def set_dict_for_upload(self, dict_key, file_dataframe):
        self.compare_dataframes[dict_key] = self.changeStringToDate(file_dataframe.copy())
        return True

    def set_dataframe_from_arg(self, dataf):
        self.dataFrame = dataf.copy()