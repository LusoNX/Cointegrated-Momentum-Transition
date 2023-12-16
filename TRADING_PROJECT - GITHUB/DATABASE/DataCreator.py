import pandas as pd
import sys
from sqlite3 import connect
import urllib
from sqlalchemy import create_engine
import pyodbc
import urllib
import sqlite3


def sqlite_database():
    import sqlite3

    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('TradingDatabase.db')
    cursor = conn.cursor()
    #cursor.execute('DELETE FROM AssetIndex;')
    #cursor.execute('VACUUM;')

    cursor.execute("""
        CREATE TABLE CointegrationValuation (
            "ID 1" INT,
            "ID 2" INT,
            "Date" DATETIME,     
            Metric TEXT,
            "Value" FLOAT,
            "GICS Sector" TEXT
                   
        );
    """)

    cursor.execute("""
        CREATE TABLE CointegrationValuation (
            "ID 1" INT,
            "ID 2" INT,
            "Date" DATETIME,     
            Metric TEXT,
            "Value" FLOAT,
            "GICS Sector" TEXT
                   
        );
    """)

    cursor.execute("""
        CREATE TABLE CointegrationMetrics (
            "ID 1" INT,
            "ID 2" INT,
            Pair TEXT,
            "Date" DATETIME,     
            Metric TEXT,
            "Value" FLOAT,
            "GICS Sector" TEXT
        );
    """)
    

    cursor.execute("""
        CREATE TABLE MomentumMetrics (
            "ID" INT,
            Name TEXT,
            "GICS Sector" TEXT,
            "GICS Sub-Industry" TEXT,
            "Date" DATETIME,     
            Metric TEXT,
            "Value" FLOAT,
            "Momentum Type"
        );
    """)
    


    cursor.execute("""
        CREATE TABLE CointegrationCheck (
            "ID 1" INT,
            "ID 2" INT,       
            Pair TEXT,
            'Date Appended' DATETIME,
            "GICS Sector" TEXT,
            isCointegrated TEXT,
            "Pair Type" TEXT
        );
    """)
    

    
    # Create AssetIndex table
    cursor.execute("""
        CREATE TABLE AssetIndex (
            ID INTEGER PRIMARY KEY,
            Name TEXT,
            Symbol TEXT,
            ISIN TEXT,
            "Instrument Type" TEXT,
            'GICS Sector' TEXT,
            'GICS Sub-Industry' TEXT,
            Code TEXT,
            Region TEXT,
            Currency TEXT
        );
    """)

    # Create PriceIndex table
    cursor.execute("""
        CREATE TABLE PriceIndex (
            ID INTEGER,
            "Date" DATETIME,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            "Adj Close" FLOAT,
            Volume FLOAT,
            FOREIGN KEY(ID) REFERENCES AssetIndex(ID)
        );
    """)

    

    cursor.execute("""
        CREATE TABLE RemovedAssets (
            ID INTEGER,
            'Name' TEXT,
            "Date" DATETIME,     
            Reason TEXT,
            FOREIGN KEY(ID) REFERENCES AssetIndex(ID)
        );
    """)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


sqlite_database()

