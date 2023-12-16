import sqlite3 
import pandas as pd 
import yfinance as yf

class getData():
    def __init__(self,db_directory):
        conn = sqlite3.connect(db_directory)
        cursor = conn.cursor()
        self.conn = conn
        self.cursor = cursor
            

    def getAssetIndexData(self,stock_index):

        if stock_index == "stock":
            # Get sp500 index tickers
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            tables = pd.read_html(url)
            df_sp500 = tables[0]
            df_sp500 = df_sp500.rename(columns = {"Security":"Name"})
            df_sp500["Region"] = "US"
            df_sp500["Currency"] = "USD"
            df_sp500["ISIN"] = ""
            df_sp500["Code"] = df_sp500["Symbol"]
            df_sp500["Instrument Type"] = "Stock"
            df_sp500.index.name = "ID"
            df_sp500 = df_sp500[["Name","Symbol","ISIN","Instrument Type","GICS Sector", "GICS Sub-Industry","Code","Region","Currency"]]
            df_sp500.to_sql("AssetIndex",self.conn,if_exists= "append",index = True)
        elif stock_index == "index":
            sector_tickers_and_names = {
                "XLC": "Communication Services",
                "XLY": "Consumer Discretionary",
                "XLP": "Consumer Staples",
                "XLE": "Energy",
                "XLF": "Financials",
                "XLV": "Health Care",
                "XLI": "Industrials",
                "XLB": "Materials",
                "XLRE": "Real Estate",
                "XLK": "Information Technology",
                "XLU": "Utilities",
            }
            
            df_sector = pd.DataFrame({
                'Symbol': list(sector_tickers_and_names.keys()),
                'Code': ['SP500 ' + sector_tickers_and_names[ticker] for ticker in sector_tickers_and_names],
                'Name': list(sector_tickers_and_names.values()),
                "GICS Sector":list(sector_tickers_and_names.values())
            }, index=range(10000, 10000 + len(sector_tickers_and_names)))

            df_sector["Region"] = "US"
            df_sector["Currency"] = "USD"
            df_sector["ISIN"] = ""
            df_sector["Instrument Type"] = "Index"
            df_sector["GICS Sub-Industry"] = ""
            df_sector.index.name = "ID"
            df_sector = df_sector[["Name","Symbol","ISIN","Instrument Type","GICS Sector", "GICS Sub-Industry","Code","Region","Currency"]]
            df_sector.to_sql("AssetIndex",self.conn,if_exists= "append",index = True)


            # Download historical data for each sector ETF
            


#sector_data = yf.download(list(sector_tickers_and_names.keys()), start=, end="2023-01-01")  # Adjust the date range as needed


    def getPriceData(self,start_date,end_date):
        df_index = pd.read_sql("SELECT * FROM AssetIndex",self.conn)
        symbols = df_index.Symbol.to_list()
        ids = df_index.ID.to_list()

        def get_historical_data(symbol, start_date, end_date):
            try:
                stock_data = yf.download(symbol, start=start_date, end=end_date)
                return stock_data
            except Exception as e:
                print(f"Failed to fetch data for {symbol}: {str(e)}")
                return symbol
            
        
        # Loop through each symbol and fetch historical data

        failed_ids = []
        for symbol,id in zip(symbols,ids):
            print(f"Fetching data for {symbol}...")          
            try:
                data = get_historical_data(symbol, start_date, end_date)
                

                data = data.round(3)
                data = data.reset_index()
                data["ID"] = id
                data["Date"] =pd.to_datetime(data["Date"],format("%Y-%m-%d"))

                
                df_exists = pd.read_sql_query(f"SELECT * FROM PriceIndex WHERE ID =={id}",self.conn).sort_values("Date")
            except:
                data = pd.DataFrame()
            
            # Must have data
            # Must have at least 5 years 
            if isinstance(data, pd.DataFrame) and not data.empty and (data['Date'].max() - data['Date'].min() >= pd.Timedelta(days=365 * 5)):
                if df_exists.empty:
                    data.to_sql("PriceIndex",self.conn,if_exists="append",index=False)
                else:
                    last_date =df_exists["Date"].iloc[-1]
                    data = data[data["Date"]>last_date]
                    data.to_sql("PriceIndex",self.conn,if_exists="append",index=False)
            else:
                failed_ids.append(id)
        

        df_failed = pd.DataFrame([failed_ids])
        df_failed = df_failed.T
        df_failed = df_failed.rename(columns = {df_failed.columns[0]:"ID"})
        df_failed["Reason"] = "Min 5 years or Missing Data"
        df_failed["Date"] = end_date
        df_failed["Date"] = pd.to_datetime(df_failed["Date"],format = "%Y-%m-%d")
        df_failed = df_failed.merge(df_index[["ID","Name"]], on = "ID",how = "inner")
        df_failed.to_csv("failed.csv")
        df_failed.to_sql("RemovedAssets",self.conn,if_exists = "replace",index = False)
            

def update_price_data():
    get_data = getData(r"C:\Users\Luan\Desktop\TRADING_PROJECT\DATABASE\TradingDatabase.db")
    #get_data.getAssetIndexData("stock")
    #get_data.getAssetIndexData("index")
    start_date = '2018-01-01'
    from datetime import datetime
    current_date_time = datetime.now()
    # Format the date as a string in the "d-m-y" format
    end_date = current_date_time.strftime("%Y-%m-%d")
    #end_date = '2023-11-11'
    get_data.getPriceData(start_date,end_date)

#update_price_data()