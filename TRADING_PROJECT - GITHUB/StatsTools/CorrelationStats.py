from scipy.optimize import minimize
import numpy as np
import pandas as pd
import datetime as dt
import copy
import matplotlib.pyplot as plt
import time
import math
import plotly.express as px
import plotly.graph_objects as go
from cvxpy import *
from scipy.optimize import nnls
import scipy.optimize
from datetime import datetime
from scipy.optimize import nnls
import seaborn as sns
import urllib
from sqlalchemy import create_engine
import pyodbc
from statsmodels.tsa.stattools import adfuller
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import sklearn
import sqlite3

class CorrelationStats():
    def __init__(self,db_directory):
        conn = sqlite3.connect(db_directory)
        cursor = conn.cursor()
        self.conn = conn
        self.cursor = cursor
        df = pd.read_sql("SELECT * FROM AssetIndex",self.conn)
        #filter for missing data 
        ids_missing = list(pd.read_sql("SELECT * FROM RemovedAssets",self.conn)["ID"].values)
        df =df[~df['ID'].isin(ids_missing)]
        self.df_index = df


    def cointegration_regression(self,df_price_X,df_price_Y):
        X = np.array(df_price_X).reshape(-1,1)
        Y = np.array(df_price_Y).reshape(-1,1)
        model = LinearRegression()
        model_fit = model.fit(X,Y)
        beta_value = model_fit.coef_[0][0]
        prediction = model_fit.predict(X)
        residual = (Y-prediction)
        residual = list(residual.ravel())
        return beta_value,residual

    def adf_test(self,data_series):
        adf_fuller_results = adfuller(data_series)
        p_value = adf_fuller_results[1]

        if p_value <= 0.05:
            print("Series are Stationary. P_value is: {}".format(p_value))
            print("--------------------------------------------------------")
            return True
        else:
            print("Series are not Stationary. P_value is: {}".format(p_value))
            print("--------------------------------------------------------")
            return False


    #Evaluates if the pairs are cointegrated
    def cointegration_evaluation(self,sector_type,sector,minimum_years,periodicity):
        df_index = self.df_index.copy()
        sector_ids = df_index[df_index[sector_type] == sector]["ID"].to_list()

        def check_cointegration(df_price_X,df_price_Y):
            beta_v,residual_v = self.cointegration_regression(df_price_X,df_price_Y)
            adf_test_value = self.adf_test(residual_v)
            return adf_test_value,beta_v

        def double_check_cointegration(_df_price_merge):
            adf_test_1,beta_1 = check_cointegration(_df_price_merge["Log_price_1"],_df_price_merge["Log_price_2"])
            adf_test_2,beta_2 = check_cointegration(_df_price_merge["Log_price_2"],_df_price_merge["Log_price_1"])

            if adf_test_1 == True and adf_test_2 == True:
                return True
            else:
                return False
                
        list_of_lists = []
        n = len(sector_ids)
        for i in range(n):
            for j in range(i + 1, n):
                id_1 = sector_ids[i]
                id_2 = sector_ids[j]
                instrument_type = df_index[df_index["ID"] == id_2]["Instrument Type"].values[0]
                df_price_1 = pd.read_sql("SELECT ID,Date,[Adj Close] FROM PriceIndex WHERE ID = {}".format(id_1),self.conn,index_col = "Date").sort_index()
                df_price_2 = pd.read_sql("SELECT ID,Date,[Adj Close] FROM PriceIndex WHERE ID = {}".format(id_2),self.conn,index_col = "Date").sort_index()

                print(f"Trying for ID 1: {id_1} and ID 2: {id_2}")
                symbol_1 =self.df_index[self.df_index["ID"]== id_1]["Symbol"].values[0]
                symbol_2 =self.df_index[self.df_index["ID"]== id_2]["Symbol"].values[0]
                pair_name = symbol_1+"/"+symbol_2
                df_price_1 = df_price_1[["Adj Close"]]
                df_price_2 = df_price_2[["Adj Close"]]
                df_price_merge = df_price_1.merge(df_price_2,how = "inner", right_index =True, left_index = True)
            
                df_price_merge.rename(columns = {"Adj Close_x": "Adj Close_1","Adj Close_y": "Adj Close_2"},inplace = True)
                df_price_merge["Log_price_1"] = np.log2(df_price_merge["Adj Close_1"])
                df_price_merge["Log_price_2"] = np.log2(df_price_merge["Adj Close_2"])
            
                pair_cointegrated = double_check_cointegration(df_price_merge)
                pair_cointegrated = str(pair_cointegrated)                      
                current_date = datetime.now().strftime("%d/%m/%Y")
                list_values= [id_1,id_2,pair_name,current_date,pair_cointegrated,instrument_type]
                list_of_lists.append(list_values)
                    

        df_cointegration_check = pd.DataFrame(list_of_lists,columns = ["ID 1", "ID 2", "Pair","Date Appended","isCointegrated","Pair Type"])
        df_cointegration_check["GICS Sector"] = sector
        df_cointegration_exists = pd.read_sql_query(f"SELECT * FROM  CointegrationCheck WHERE [GICS Sector] == '{sector}'",self.conn)
        df_cointegration_check.to_sql("CointegrationCheck",self.conn,if_exists="append",index =False)


        if df_cointegration_exists.empty:
            df_cointegration_check.to_sql("CointegrationCheck",self.conn,if_exists="append",index =False)
        else:
            # Think on a condition to update the values. 
            # Query to delete by pair. 
            # Query to re- append the deleted values. 
            pass
    
    #Runs all the cointegration checks by industry 
    def run_cointegration_checks(self):
        unique_sectors = list(self.df_index["GICS Sector"].unique())
        if "Market" in unique_sectors:
            unique_sectors.remove("Market")

        for i in unique_sectors:
            self.cointegration_evaluation("GICS Sector",i,5,"D")
            


    #Only applicable to those that have passed the cointegration check
    def cointegration_metrics(self):
        
        df_cointegration_check = pd.read_sql("SELECT * FROM CointegrationCheck",self.conn).sort_values("Pair")
        df_cointegration_check = df_cointegration_check[df_cointegration_check["isCointegrated"] == "True"]
        unique_pairs = df_cointegration_check["Pair"].unique()

        for i in unique_pairs:
            id_1 = df_cointegration_check[df_cointegration_check["Pair"] ==i]["ID 1"].values[0]
            id_2 = df_cointegration_check[df_cointegration_check["Pair"] ==i]["ID 2"].values[0]

            
            df_price_1 = pd.read_sql("SELECT ID,Date,[Adj Close] FROM PriceIndex WHERE ID = {}".format(id_1),self.conn,index_col = "Date").sort_index()
            df_price_2 = pd.read_sql("SELECT ID,Date,[Adj Close] FROM PriceIndex WHERE ID = {}".format(id_2),self.conn,index_col = "Date").sort_index()
            df_price_1 = df_price_1[["Adj Close"]]
            df_price_2 = df_price_2[["Adj Close"]]
            df_price_merge = df_price_1.merge(df_price_2,how = "inner", right_index =True, left_index = True)
            df_price_merge.rename(columns = {"Adj Close_x": "Adj Close_1","Adj Close_y": "Adj Close_2"},inplace = True)
            df_price_merge["Log_price_1"] = np.log2(df_price_merge["Adj Close_1"])
            df_price_merge["Log_price_2"] = np.log2(df_price_merge["Adj Close_2"])
            
            symbol_1 =self.df_index[self.df_index["ID"]== id_1]["Symbol"].values[0]
            symbol_2 =self.df_index[self.df_index["ID"]== id_2]["Symbol"].values[0]
            pair_name = symbol_1+"/"+symbol_2
            beta,residual_v = self.cointegration_regression(df_price_merge["Log_price_2"],df_price_merge["Log_price_1"])


            sector = self.df_index[self.df_index["ID"]==id_1]["GICS Sector"].values[0]

            df_residual = pd.DataFrame(residual_v,columns = ["Value"])
            df_residual["Metric"] = "Cointegration Residual"
            df_residual["Pair"] = pair_name
            df_residual["ID 1"] = id_1
            df_residual["ID 2"] = id_2
            df_residual["Date"] = df_price_merge.index.values
            df_residual["GICS Sector"] = sector
         
            # ADD sector aswell...
        # If cointegrated, we append the data into the database
            # Append the values to the SQL database
            df_residual_exists = pd.read_sql(f"SELECT * FROM CointegrationMetrics WHERE Pair == '{pair_name}'",self.conn).sort_values("Date")
            if df_residual_exists.empty:
                df_residual.to_sql("CointegrationMetrics",self.conn,if_exists="append",index = False)
            else:
                df_residual =df_residual[df_residual["Date"]>df_residual_exists["Date"].iloc[-1]]
                df_residual.to_sql("CointegrationMetrics",self.conn,if_exists="append",index = False)
        else:
            pass
    
    #Under valued and over valued. 
    # Check cointegration pairs by their Index, and evaluate how much overvalued/undervalued they are, based on the distance to the pair. 
    
    def stock_relative_valuation(self,sector_industry):
        df_index = self.df_index.copy()
        def label_name(DF):
            df = DF.copy()
            df.columns = df.columns.astype(str)
            # Calculate percentiles for each row
            p20 = df.quantile(0.2, axis=1)
            p40 = df.quantile(0.4, axis=1)
            p60 = df.quantile(0.6, axis=1)
            p80 = df.quantile(0.8, axis=1)
            p25 = df.quantile(0.25, axis=1)
            p75 = df.quantile(0.75, axis=1)
            p50 = df.quantile(0.75, axis=1)
            # Threshold are based on length. If above 4 it takes 5 labels, if equal to 3, 3 labelss and if only 2, 2 labels
            if len(df.columns) >=4:
                for column in df.columns:
                    df[column] = df.apply(lambda row: 'Very Undervalued' if row[column] < p20[row.name] else
                                        ('Undervalued' if p20[row.name] <= row[column] < p40[row.name] else
                                        ('Normal' if p40[row.name] <= row[column] < p60[row.name] else
                                            ('Overvalued' if p60[row.name] <= row[column] < p80[row.name] else
                                            'Very Overvalued'))), axis=1)
            elif len(df.columns) ==3:
                for column in df.columns:
                    df[column] = df.apply(lambda row: 'Undervalued' if row[column] < p25[row.name] else
                                        ('Normal' if p25[row.name] <= row[column] < p75[row.name] else 'Overvalued'), axis=1)
                    
            elif len(df.columns) ==2:
                for column in df.columns:
                    df[column] = df.apply(lambda row: 'Undervalued' if row[column] < p50[row.name] else 'Overvalued', axis=1)
            elif len(df.columns) ==1:
                for column in df.columns:
                    df[column] = df.apply(lambda row: 'Normal' , axis=1)

            return df
                
        # Seggregate either by Sector or by industry 
        if sector_industry == "GICS Sector":
            gics_ids = df_index[df_index["Instrument Type"]== "Index"]
            gics_ids= list(gics_ids.loc[gics_ids['GICS Sub-Industry'].isnull() | (gics_ids['GICS Sub-Industry'] == '')]["ID"].values)
            df_cointegrated = pd.read_sql_query(f"SELECT * FROM CointegrationMetrics",self.conn)
            for i in gics_ids:
                sector_name = df_index[df_index["ID"]==i]["GICS Sector"].values[0]
                df_cointegration_sector = df_cointegrated[df_cointegrated["ID 2"]==i]
                df_cointegration_sector = df_cointegration_sector.drop_duplicates(subset = ["Date","Pair"])
                df_pivoted = df_cointegration_sector.pivot_table(index='Date', columns='ID 1', values='Value', aggfunc='first')
                df_cointegration_label = label_name(df_pivoted)
                df_cointegration_label = df_cointegration_label.reset_index()
                df_melted = pd.melt(df_cointegration_label, id_vars='Date', value_vars=df_cointegration_label.columns, var_name='ID 1', value_name='Value')
                df_melted["ID 2"] = i
                df_melted["GICS Sector"] = sector_name
                df_melted["Metric"] = "Relative Sector Valuation"
                
                unique_ids = list(df_melted["ID 1"].unique())
                for x in unique_ids:
                    df_exists = pd.read_sql_query(f"SELECT * FROM CointegrationValuation WHERE [Metric] == 'Relative Sector Valuation' AND [ID 1] == {x}",self.conn)
                
                    melted_df_2 = df_melted[df_melted["ID 1"] == x].copy()
                    if df_exists.empty:
                        melted_df_2.to_sql("CointegrationValuation",self.conn,if_exists="append",index=False)
                    else:
                        last_date_in_db = df_exists['Date'].max()
                        melted_df_2 = melted_df_2[melted_df_2['Date'] > last_date_in_db]

                        melted_df_2.to_sql("CointegrationValuation",self.conn,if_exists="append",index=False)

    #Takes the mean and the std and checks whether the stock is overvalued or not. 
    def stock_absolute_valuation(self,sector_industry):
        df_index = self.df_index.copy()
        if sector_industry == "GICS Sector":
            gics_ids = df_index[df_index["Instrument Type"]== "Index"]
            gics_ids= list(gics_ids.loc[gics_ids['GICS Sub-Industry'].isnull() | (gics_ids['GICS Sub-Industry'] == '')]["ID"].values)
            df_cointegrated = pd.read_sql_query(f"SELECT * FROM CointegrationMetrics",self.conn)

            for i in gics_ids:
                sector_name = df_index[df_index["ID"]==i]["GICS Sector"].values[0]
                df_cointegration_sector = df_cointegrated[df_cointegrated["ID 2"]==i]
                df_cointegration_sector = df_cointegration_sector.drop_duplicates(subset = ["Date","Pair"])
                df_pivoted = df_cointegration_sector.pivot_table(index='Date', columns='ID 1', values='Value', aggfunc='first')

                def label(value, mean, std):
                    if value < (mean - 1 * std):
                        return 'Very Undervalued'
                    elif (mean - 1 * std) <= value < (mean - 0.5 * std):
                        return 'Undervalued'
                    elif (mean - 0.5 * std) <= value <= (mean + 0.5 * std):
                        return 'Normal'
                    elif (mean + 0.5 * std) < value <= (mean + 1 * std):
                        return 'Overvalued'
                    else:
                        return 'Very Overvalued'

                # Apply the label function and replace the 'Value' column with labels
                for col in df_pivoted.columns:
                    mean_value = df_pivoted[col].mean()
                    std_value = df_pivoted[col].std()
                    label_col = f'{col}_Label'  # Create a new column for labels
                    df_pivoted[col] = df_pivoted[col].apply(lambda value: label(value, mean_value, std_value))

                df_cointegration_label = df_pivoted.copy()
                df_cointegration_label = df_cointegration_label.reset_index()
                df_melted = pd.melt(df_cointegration_label, id_vars='Date', value_vars=df_cointegration_label.columns, var_name='ID 1', value_name='Value')
                df_melted["ID 2"] = i
                df_melted["GICS Sector"] = sector_name
                df_melted["Metric"] = "Absolute Sector Valuation"
                unique_ids = list(df_melted["ID 1"].unique())
                for x in unique_ids:
                    df_exists = pd.read_sql_query(f"SELECT * FROM CointegrationValuation WHERE [Metric] == 'Absolute Sector Valuation' AND [ID 1] == {x}",self.conn)
                    melted_df_2 = df_melted[df_melted["ID 1"] == x].copy()
                    if df_exists.empty:
                        melted_df_2.to_sql("CointegrationValuation",self.conn,if_exists="append",index=False)
                    else:
                        last_date_in_db = df_exists['Date'].max()
                        melted_df_2 = melted_df_2[melted_df_2['Date'] > last_date_in_db]
                        melted_df_2.to_sql("CointegrationValuation",self.conn,if_exists="append",index=False)


def correlation_stats_update():
    corr_stats = CorrelationStats(r"YOUR_DATABASE_DIRECTORY")
    corr_stats.run_cointegration_checks()
    corr_stats.cointegration_metrics()
    corr_stats.stock_relative_valuation("GICS Sector") # updates the relative Valuation labels 
    corr_stats.stock_absolute_valuation("GICS Sector") # updates thee absolute valuation labels

#correlation_stats_update()




