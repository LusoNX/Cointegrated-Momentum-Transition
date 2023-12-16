import sqlite3
import pandas as pd 
import matplotlib.pyplot as plt
db_directory =r"YOUR-DATABASE-DIRECTORY"
conn = sqlite3.connect(db_directory)
cursor = conn.cursor()

def check_individual_metric(id,metric_label,show_figure):
    df_index = pd.read_sql(f"SELECT * FROM AssetIndex",conn)
    gics_sector = df_index[df_index["ID"]==id]["GICS Sector"].values[0]
    id_2 = df_index[(df_index["GICS Sector"] == gics_sector) & (df_index["Instrument Type"] == "Index")]["ID"].values[0]
    momentum_labels = ["30D Momentum Rank","90D Momentum Rank","1Y Momentum Rank"]
    cointegration_labels = ["Cointegration Residual"]

    if metric_label in momentum_labels:
        df = pd.read_sql(f"SELECT * FROM MomentumMetrics WHERE [Metric]=='{metric_label}' AND [ID] == {id}",conn).sort_values("Date")
    elif metric_label in cointegration_labels:
        
        df = pd.read_sql(f"SELECT * FROM CointegrationMetrics WHERE [Metric]=='Cointegration Residual' AND [ID 1] == {id} AND [ID 2] == {id_2}",conn)
    
    df["mean"] = df["Value"].mean()
    df["threshold"] = df["Value"].mean() - df["Value"].std()
    df=df.set_index("Date")
    if show_figure == True:
        df[["Value","mean","threshold"]].plot()
        plt.show()
    else:
        pass
    return df


def check_individual_labels_transition(_id_list,gics_sector_industry,transition_DAYS):
    df_cointegration = pd.read_sql("SELECT * FROM CointegrationValuation",conn).sort_values("Date")
    df_momentum = pd.read_sql(f"SELECT * FROM MomentumMetrics WHERE [Momentum Type]== '{gics_sector_industry}'",conn).sort_values("Date")
    last_date = df_momentum["Date"].iloc[-1]
    momentum_labels = ["30D Momentum Label","90D Momentum Label","1Y Momentum Label"]
    df_momentum= df_momentum[df_momentum["Metric"].isin(momentum_labels)]
    last_date = df_cointegration["Date"].iloc[-1]
    df_cointegration = df_cointegration[df_cointegration["Date"]==last_date]

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
    df_momentum_ids = df_momentum_ids.reset_index()

    print(df_momentum_ids["ID"])
    df_momentum_ids = df_momentum_ids[df_momentum_ids["ID"].isin(_id_list)]
    #df_momentum_ids = df_momentum_ids.set_index("ID")
    print(df_momentum_ids)
    asdasd
    #df_momentum_ids["Sum"] = df_momentum_ids.sum(axis=1)

    

id_list = [268,326]
check_individual_labels_transition(id_list,"Stock GICS Sector",30)

check_individual_metric(326,"1Y Momentum Rank",False)