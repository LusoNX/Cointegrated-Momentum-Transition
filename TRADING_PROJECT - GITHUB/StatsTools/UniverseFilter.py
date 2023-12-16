import pandas as pd 
import numpy as np
import sqlite3
from datetime import datetime, timedelta



class UniverseFilter():
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


    def universe_filter(self,gics_sector_industry,cointegration_condition,momentum_condition,transition_DAYS):
        df_cointegration = pd.read_sql("SELECT * FROM CointegrationValuation",self.conn).sort_values("Date")
        df_momentum = pd.read_sql(f"SELECT * FROM MomentumMetrics WHERE [Momentum Type]== '{gics_sector_industry}'",self.conn).sort_values("Date")
        last_date = df_momentum["Date"].iloc[-1]
        momentum_labels = ["30D Momentum Label","90D Momentum Label","1Y Momentum Label"]
        df_momentum= df_momentum[df_momentum["Metric"].isin(momentum_labels)]
        last_date = df_cointegration["Date"].iloc[-1]
        df_cointegration = df_cointegration[df_cointegration["Date"]==last_date]


        def filter_factor(df, _condition,id_column):
            conditions = []
            
            list_of_ids = []
            for i in _condition.keys():
                df_ind = df[df["Metric"]== i]
                df_ind = df_ind[df_ind["Value"].isin(_condition[i])]
                list_of_ids.append(list(df_ind[id_column].values))
            
            if id_column == "ID":
                common_values = set(list_of_ids[0]).intersection(list_of_ids[1],list_of_ids[2])
            else:
                common_values = set(list_of_ids[0]).intersection(list_of_ids[1])
            common_values_list = list(common_values)
            return common_values_list


   
        df_list = []
        for i in momentum_labels:
            
            df_m = df_momentum.copy()
            df_m = df_m[df_m["Metric"]==i]
            #Capture the last rank aswell
            df_label =df_m.sort_values("Date").copy()
            df_label = df_m[df_label["Date"]==df_label["Date"].iloc[-1]].copy()
            df_label = df_label[["ID","Value"]]
            df_label = df_label.rename(columns = {"Value":i})

            # Transform the labels into ranks from 1 to 5. 
            label_order = ['Very Low', 'Low', 'Normal', 'High', 'Very High']
            label_mapping = {label: i+1 for i, label in enumerate(label_order)}
            
            df_m = df_m.replace({'Value': label_mapping})
            melted_df = df_m.pivot(index='Date', columns='ID', values='Value')
            melted_df = melted_df.sort_index()
            melted_df = melted_df.diff()
            melted_df = melted_df.iloc[-(transition_DAYS):]
            melted_df_sum = melted_df.sum(axis=0)
            shift_name = i.replace("Label", "Transition")
            melted_df_sum = pd.DataFrame(melted_df_sum,columns= [shift_name])
            
            melted_df_sum = melted_df_sum.reset_index()
            melted_df_sum = melted_df_sum.merge(df_label[["ID",i]],on="ID",how="inner")
            melted_df_sum = melted_df_sum.set_index("ID")
            melted_df_sum = melted_df_sum.sort_index()
            df_list.append(melted_df_sum)
        df_momentum_ids = pd.concat(df_list,axis =1)

        df_momentum_ids["Sum"] = df_momentum_ids.sum(axis=1)

        # Now check keep IDS that are at least 0 in all 3 metrics. 
        #df_momentum_ids = df_momentum_ids[(df_momentum_ids > 0).all(axis=1)]
        df_momentum_ids = df_momentum_ids[df_momentum_ids["Sum"]>0]
        df_momentum_ids = df_momentum_ids.reset_index()


        df_momentum_ids = df_momentum_ids.merge(self.df_index[["ID","Name","Symbol","GICS Sector"]],on = "ID", how ="inner")
     
        #for i in momentum_condition.keys():
        #    df_m = df_momentum[df_momentum["Metric"]==i].copy()
        #    df_m = df_m.rename(columns = {"Value":i})
        #    df_momentum_ids= df_momentum_ids.merge(df_m[["ID",i]],on ="ID", how = "inner")
        #df_momentum_ids.to_csv("momentum_values.csv")

        result_ids = filter_factor(df_cointegration, cointegration_condition,"ID 1")
        df_cointegration_ids = pd.DataFrame(result_ids,columns=["ID"])
        df_cointegration_ids = df_cointegration_ids.merge(self.df_index[["ID","Name","Symbol","GICS Sector"]],on = "ID", how ="inner")
        

        for i in cointegration_condition.keys():
            df_m = df_cointegration[df_cointegration["Metric"]==i].copy()
            df_m = df_m.rename(columns = {"ID 1":"ID"})
            df_m = df_m.rename(columns = {"Value":i})
            df_cointegration_ids= df_cointegration_ids.merge(df_m[["ID",i]],on ="ID", how = "inner")
        
        df_cointegration_ids =df_cointegration_ids.sort_values("ID")
        df_momentum_ids = df_momentum_ids.sort_values("ID")

        df_momentum_ids.to_csv("momentum_values.csv")
        df_cointegration_ids.to_csv("cointegration_values.csv")
        df_final_filter = df_cointegration_ids.merge(df_momentum_ids[["ID","30D Momentum Transition","90D Momentum Transition","1Y Momentum Transition","30D Momentum Label","90D Momentum Label","1Y Momentum Label"]],on="ID",how ="inner")
        current_date_time = datetime.now()
        # Format the date as a string in the "d-m-y" format
        formatted_date = current_date_time.strftime("%d-%m-%Y")
        df_final_filter.to_csv(r"C:\Users\Luan\Desktop\TRADING_PROJECT\Filtered_Stocks\FILTER_{}.csv".format(formatted_date))
    


    def _universe_filter_b(self,gics_sector_industry,cointegration_condition,momentum_condition,transition_DAYS,init_date,end_date):
        df_cointegration = pd.read_sql("SELECT * FROM CointegrationValuation",self.conn).sort_values("Date")
        df_cointegration["Date"] = pd.to_datetime(df_cointegration["Date"],format ="%Y-%m-%d")
        df_cointegration = df_cointegration[(df_cointegration["Date"] >= init_date) & (df_cointegration["Date"] <= end_date)]


        df_momentum = pd.read_sql(f"SELECT * FROM MomentumMetrics WHERE [Momentum Type]== '{gics_sector_industry}'",self.conn).sort_values("Date")
        df_momentum["Date"] = pd.to_datetime(df_momentum["Date"],format ="%Y-%m-%d")
        df_momentum = df_momentum[(df_momentum["Date"] >= init_date) & (df_momentum["Date"] <= end_date)]
        last_date = df_momentum["Date"].iloc[-1]
        momentum_labels = ["30D Momentum Label","90D Momentum Label","1Y Momentum Label"]
        df_momentum= df_momentum[df_momentum["Metric"].isin(momentum_labels)]
        last_date = df_cointegration["Date"].iloc[-1]
        df_cointegration = df_cointegration[df_cointegration["Date"]==last_date]


        def filter_factor(df, _condition,id_column):
            conditions = []
            
            list_of_ids = []
            for i in _condition.keys():
                df_ind = df[df["Metric"]== i]
                df_ind = df_ind[df_ind["Value"].isin(_condition[i])]
                list_of_ids.append(list(df_ind[id_column].values))
            
            if id_column == "ID":
                common_values = set(list_of_ids[0]).intersection(list_of_ids[1],list_of_ids[2])
            else:
                common_values = set(list_of_ids[0]).intersection(list_of_ids[1])
            common_values_list = list(common_values)
            return common_values_list

        df_list = []
        for i in momentum_labels:
            df_m = df_momentum.copy()
            df_m = df_m[df_m["Metric"]==i]
            #Capture the last rank aswell
            df_label =df_m.sort_values("Date").copy()
            df_label = df_m[df_label["Date"]==df_label["Date"].iloc[-1]].copy()
            df_label = df_label[["ID","Value"]]
            df_label = df_label.rename(columns = {"Value":i})

            # Transform the labels into ranks from 1 to 5. 
            label_order = ['Very Low', 'Low', 'Normal', 'High', 'Very High']
            label_mapping = {label: i+1 for i, label in enumerate(label_order)}
            
            df_m = df_m.replace({'Value': label_mapping})
            melted_df = df_m.pivot(index='Date', columns='ID', values='Value')
            melted_df = melted_df.sort_index()
            melted_df = melted_df.diff()
            melted_df = melted_df.iloc[-(transition_DAYS):]
            melted_df_sum = melted_df.sum(axis=0)
            shift_name = i.replace("Label", "Transition")
            melted_df_sum = pd.DataFrame(melted_df_sum,columns= [shift_name])
            
            melted_df_sum = melted_df_sum.reset_index()
            melted_df_sum = melted_df_sum.merge(df_label[["ID",i]],on="ID",how="inner")
            melted_df_sum = melted_df_sum.set_index("ID")
            melted_df_sum = melted_df_sum.sort_index()
            df_list.append(melted_df_sum)
        df_momentum_ids = pd.concat(df_list,axis =1)

        df_momentum_ids["Sum"] = df_momentum_ids.sum(axis=1)

        # Now check keep IDS that are at least 0 in all 3 metrics. 
        #df_momentum_ids = df_momentum_ids[(df_momentum_ids > 0).all(axis=1)]
        df_momentum_ids = df_momentum_ids[df_momentum_ids["Sum"]>0]
        df_momentum_ids = df_momentum_ids.reset_index()

        # Create a section where u look for the last 3 days. So if the indicator is 0 for the next 3 days it closes the positiion. if its below 1 it automatically closes after 1 day
        

        df_momentum_ids = df_momentum_ids.merge(self.df_index[["ID","Name","Symbol","GICS Sector"]],on = "ID", how ="inner")
    
        result_ids = filter_factor(df_cointegration, cointegration_condition,"ID 1")
        df_cointegration_ids = pd.DataFrame(result_ids,columns=["ID"])
        df_cointegration_ids = df_cointegration_ids.merge(self.df_index[["ID","Name","Symbol","GICS Sector"]],on = "ID", how ="inner")
        

        for i in cointegration_condition.keys():
            df_m = df_cointegration[df_cointegration["Metric"]==i].copy()
            df_m = df_m.rename(columns = {"ID 1":"ID"})
            df_m = df_m.rename(columns = {"Value":i})
            df_cointegration_ids= df_cointegration_ids.merge(df_m[["ID",i]],on ="ID", how = "inner")
        
        df_cointegration_ids =df_cointegration_ids.sort_values("ID")
        df_momentum_ids = df_momentum_ids.sort_values("ID")

        df_momentum_ids.to_csv("momentum_values.csv")
        df_cointegration_ids.to_csv("cointegration_values.csv")
        df_final_filter = df_cointegration_ids.merge(df_momentum_ids[["ID","30D Momentum Transition","90D Momentum Transition","1Y Momentum Transition","30D Momentum Label","90D Momentum Label","1Y Momentum Label"]],on="ID",how ="inner")
        
        df_final_filter["Date"] = end_date.strftime("%Y-%m-%d")
        
        return df_final_filter
    

        #current_date_time = datetime.now()
        # Format the date as a string in the "d-m-y" format
        #formatted_date = current_date_time.strftime("%d-%m-%Y")
        

    def universe_filter_backtest(self):
        # Filter the entire dataframe by segments of dates and then simply, calculate the values. 
        end_date = datetime.now()
        #end_date = end_date.strftime("%d-%m-%Y") 
        init_date = end_date - timedelta(days = 30)
        #init_date = init_date.strftime("%d-%m-%Y")
        df_list = []
        x = 0
        while init_date <= end_date:
            _init_date = init_date  - timedelta(days=30)
            _end_date = init_date
            init_date = init_date +timedelta(days=1)
        

            print(_end_date)
            print(init_date)
            cointegration_condition = {"Relative Sector Valuation": ["Undervalued","Very Undervalued"],
                    "Absolute Sector Valuation": ["Undervalued","Very Undervalued"]}
            momentum_condition ={"30D Momentum Label": ["Normal","High","Low","Very Low"],
                    "90D Momentum Label": ["Low","Very Low","Normal"],
                    "1Y Momentum Label": ["Low","Very Low","Normal"]}
            df = self._universe_filter_b("Stock GICS Sector",cointegration_condition,momentum_condition,30,_init_date,_end_date)
            df_list.append(df)
            if x == 10:
                break
        
        df_final = pd.concat(df_list,axis =0)
        df_final.to_csv(r"C:\Users\Luan\Desktop\TRADING_PROJECT\Filtered_Stocks\FILTER_{}.csv".format(""))
        
        

def universe_update():
    universe_filter = UniverseFilter(r"YOUR_DATABASE_DIRECTORY")
    cointegration_condition = {"Relative Sector Valuation": ["Undervalued","Very Undervalued"],
                    "Absolute Sector Valuation": ["Undervalued","Very Undervalued"]}
    momentum_condition ={"30D Momentum Label": ["Normal","High","Low","Very Low"],
                    "90D Momentum Label": ["Low","Very Low","Normal"],
                    "1Y Momentum Label": ["Low","Very Low","Normal"]}
    
    #universe_filter.universe_filter_backtest()
    #Transition filter signifies if the filter is including absolute momentum, or it takes into account the transition. 
    #Transtion = True, means we are interested in movementes where the label changes from One positioning to the other. 
    #The idea is that we are capturing the "new momentum stocks before they ocurring"
    #transition_DAYS represents the number of days that you will check where changes where made. For example, for the last 10 days you see the difference in Position (from 1 to 5), sum them and check which ones are positive and by how much
    universe_filter.universe_filter("Stock GICS Sector",cointegration_condition,momentum_condition,30)
#universe_update()