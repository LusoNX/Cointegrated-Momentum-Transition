import pandas as pd 
import numpy as np
import sqlite3


class MomentumStats():
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

    ## Defines the momentum by sector or sub-sector and clarifies if the stock is winner or a loser        


    def stock_momentum(self,sector_industry_type,sector_industry):
        df_index = self.df_index.copy()
        ids = df_index[df_index["Instrument Type"] == "Stock"]
        ids = list(ids[ids[sector_industry_type]==sector_industry]["ID"].values)
        list_df = []
        for i in ids:
            df = pd.read_sql(f"SELECT ID,Date,[Adj Close] FROM PriceIndex WHERE ID =={i}",self.conn,index_col="Date").sort_index()
            df = df.rename(columns={"Adj Close":i})
            df = df.drop(columns = "ID")
            list_df.append(df)
        df_final = pd.concat(list_df,axis = 1)
        
        #30D, 90D, 1Y
        df_30D = df_final.pct_change(30).iloc[30::]
        df_90D = df_final.pct_change(90).iloc[90::]
        df_1Y = df_final.pct_change(250).iloc[250::]
        
        #Rank them 
        df_30D_rank = df_30D.rank(axis =1)
        df_90D_rank = df_90D.rank(axis =1)
        df_1Y_rank = df_1Y.rank(axis =1)
        
        # Labels
        def label_name(DF):
            df = DF.copy()
            df.columns = df.columns.astype(str)

            # Calculate percentiles for each row
            p20 = df.quantile(0.2, axis=1)
            p40 = df.quantile(0.4, axis=1)
            p60 = df.quantile(0.6, axis=1)
            p80 = df.quantile(0.8, axis=1)

            # Replace values based on the calculated thresholds
            for column in df.columns:
                df[column] = df.apply(lambda row: 'Very Low' if row[column] < p20[row.name] else
                                    ('Low' if p20[row.name] <= row[column] < p40[row.name] else
                                    ('Normal' if p40[row.name] <= row[column] < p60[row.name] else
                                        ('High' if p60[row.name] <= row[column] < p80[row.name] else
                                        'Very High'))), axis=1)
            return df
                
        df_30D_label = label_name(df_30D)
        df_90D_label = label_name(df_90D)
        df_1Y_label = label_name(df_1Y)

        def melt_and_update(DF,metric_name):
            df = DF.copy()
            melted_df = pd.melt(df.reset_index(), id_vars=['Date'], var_name='ID', value_name='Value')
            melted_df["Metric"] = metric_name
            melted_df["Momentum Type"] = f"Stock {sector_industry_type}"
            melted_df[["ID"]] = melted_df[["ID"]].astype(int)

            melted_df = melted_df.merge(self.df_index[["ID","Name","GICS Sector","GICS Sub-Industry"]], on = "ID", how = "inner")
            unique_ids = melted_df["ID"].unique()
            for i in unique_ids:
                df_exists = pd.read_sql_query(f"SELECT * FROM MomentumMetrics WHERE [Metric] == '{metric_name}' AND [ID] == {i}",self.conn)
                melted_df_2 = melted_df[melted_df["ID"] == i].copy()
                if df_exists.empty:
                    melted_df_2.to_sql("MomentumMetrics",self.conn,if_exists="append",index=False)
                else:
                    last_date_in_db = df_exists['Date'].max()
                    melted_df_2 = melted_df_2[melted_df_2['Date'] > last_date_in_db]
                    melted_df_2.to_sql("MomentumMetrics",self.conn,if_exists="append",index=False)     
        
        # Heres a potential rational. 
        # labels are used for general purpose
        # Rnaks are used for conviction weight. The better the rank, the more allocation a given strategy may receive 
        # To be decided..
        
        # Export to database
        melt_and_update(df_30D_label,"30D Momentum Label")
        melt_and_update(df_90D_label,"90D Momentum Label")
        melt_and_update(df_1Y_label,"1Y Momentum Label")
        melt_and_update(df_30D_rank,"30D Momentum Rank")
        melt_and_update(df_90D_rank,"90D Momentum Rank")
        melt_and_update(df_1Y_rank,"1Y Momentum Rank")
        


    def update_stock_momentum(self,sector_industry_type):
        unique_sectors = self.df_index[self.df_index["Instrument Type"]=="Index"]
        if sector_industry_type == "GICS Sector":
            unique_sectors = unique_sectors["GICS Sector"].unique()
        elif sector_industry_type == "GICS Sub-Industry":
            unique_sectors = unique_sectors["GICS Sub-Industry"].unique()

        for i in unique_sectors:
            self.stock_momentum(sector_industry_type,i)


        
    def sector_momentum(self):
        df_index = self.df_index.copy()
        df_index = df_index[df_index["Instrument Type"]=="Index"]
        df_index = df_index[df_index['GICS Sub-Industry'].isnull() | (df_index['GICS Sub-Industry'] == '')]
        ids =   list(df_index["ID"].unique())
        list_df = []
        for i in ids:
            df = pd.read_sql(f"SELECT ID,Date,[Adj Close] FROM PriceIndex WHERE ID =={i}",self.conn,index_col="Date").sort_index()
            df = df.rename(columns={"Adj Close":i})
            df = df.drop(columns = "ID")
            list_df.append(df)
        df_final = pd.concat(list_df,axis = 1)


        #30D, 90D, 1Y
        df_30D = df_final.pct_change(30).iloc[30::]
        df_90D = df_final.pct_change(90).iloc[90::]
        df_1Y = df_final.pct_change(250).iloc[250::]
        
        #Rank them 
        df_30D_rank = df_30D.rank(axis =1)
        df_90D_rank = df_90D.rank(axis =1)
        df_1Y_rank = df_1Y.rank(axis =1)

        self.df_30D_rank_sector = df_30D.rank(axis =1)
        self.df_90D_rank_sector = df_90D.rank(axis =1)
        self.df_1Y_rank_sector = df_1Y.rank(axis =1)
        
        # Labels
        def label_name(DF):
            df = DF.copy()
            df.columns = df.columns.astype(str)

            # Calculate percentiles for each row
            p20 = df.quantile(0.2, axis=1)
            p40 = df.quantile(0.4, axis=1)
            p60 = df.quantile(0.6, axis=1)
            p80 = df.quantile(0.8, axis=1)

            # Replace values based on the calculated thresholds
            for column in df.columns:
                df[column] = df.apply(lambda row: 'Very Low' if row[column] < p20[row.name] else
                                    ('Low' if p20[row.name] <= row[column] < p40[row.name] else
                                    ('Normal' if p40[row.name] <= row[column] < p60[row.name] else
                                        ('High' if p60[row.name] <= row[column] < p80[row.name] else
                                        'Very High'))), axis=1)
            return df
        
        df_30D_label = label_name(df_30D)
        df_90D_label = label_name(df_90D)
        df_1Y_label = label_name(df_1Y)
        
        self.df_30D_label_sector = df_30D_label
        self.df_90D_label_sector = df_90D_label
        self.df_1Y_label_sector = df_1Y_label


        
    def sub_industry_momentum(self):
        pass

    
    def update_index_momentum(self):
        #Run the Momentum for the sector
        self.sector_momentum()
        df_index = self.df_index.copy()
        df_index = df_index[df_index["Instrument Type"]== "Index"]
        gics_ids= list(df_index.loc[df_index['GICS Sub-Industry'].isnull() | (df_index['GICS Sub-Industry'] == '')]["ID"].values)

        def melt_and_update(DF,metric_name):
            df = DF.copy()
            melted_df = pd.melt(df.reset_index(), id_vars=['Date'], var_name='ID', value_name='Value')
            melted_df["Metric"] = metric_name
            melted_df[["ID"]] = melted_df[["ID"]].astype(int)
            melted_df = melted_df.merge(self.df_index[["ID","Name","GICS Sector","GICS Sub-Industry"]], on = "ID", how = "inner")
            
            unique_ids = list(melted_df["ID"].unique())
            for i in unique_ids:
                if i in gics_ids:
                    name = "GICS Sector"
                else:
                    name = "GICs Sub-Industry"
                df_exists = pd.read_sql_query(f"SELECT * FROM MomentumMetrics WHERE [Metric] == '{metric_name}' AND [ID] == {i}",self.conn)


                melted_df_2 = melted_df[melted_df["ID"] == i].copy()
                melted_df_2["Momentum Type"] = f"Index {name}"                    
                if df_exists.empty:
                    melted_df_2.to_sql("MomentumMetrics",self.conn,if_exists="append",index=False)
                else:
                    last_date_in_db = df_exists['Date'].max()
                    melted_df_2 = melted_df_2[melted_df_2['Date'] > last_date_in_db]
                    melted_df_2.to_sql("MomentumMetrics",self.conn,if_exists="append",index=False)               
        

        # Sector update
        df_30D_label_sector = self.df_30D_label_sector.copy() 
        df_90D_label_sector = self.df_90D_label_sector.copy()
        df_1Y_label_sector = self.df_1Y_label_sector.copy()

        df_30D_rank_sector = self.df_30D_rank_sector.copy()
        df_90D_rank_sector = self.df_90D_rank_sector.copy()
        df_1Y_rank_sector = self.df_1Y_rank_sector.copy()

        
        melt_and_update(df_30D_label_sector,"30D Momentum Label")
        melt_and_update(df_90D_label_sector,"90D Momentum Label")
        melt_and_update(df_1Y_label_sector,"1Y Momentum Label")
        melt_and_update(df_30D_rank_sector,"30D Momentum Rank")
        melt_and_update(df_90D_rank_sector,"90D Momentum Rank")
        melt_and_update(df_1Y_rank_sector,"1Y Momentum Rank")

        # Sub-Industry update (if applicable)

        # Stock update



def momentum_update():
    mom_stats = MomentumStats(r"YOUR_DATABASE_DIRECTORY")
    #mom_stats.stock_momentum("GICS Sector","Industrials")
    mom_stats.update_stock_momentum("GICS Sector")
    mom_stats.update_index_momentum()



#momentum_update()

