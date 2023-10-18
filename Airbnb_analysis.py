# Parse the repository URL to extract owner and repo name
import evadb
import pandas as pd
import pudb
import numpy as np


if __name__ == "__main__":
    try:
        # establish evadb api cursor
        print("⏳ Connect to EvaDB...")
        cursor = evadb.connect().cursor()
        print("✅ Connected to EvaDB...")
        print("CHECK1")
        
        #Creating the table for airbnbdata
        np.seterr(divide='ignore', invalid='ignore')
        cursor.query("DROP TABLE IF EXISTS airbnbdata;").df()
        cursor.query(f"""
        CREATE TABLE airbnbdata (
            price INTEGER,
            review_scores_rating INTEGER,
            bedrooms INTEGER,
            last_review TEXT(1000));
            """).df()
        #Loading data from CSV file to airbnb table
        cursor.query("LOAD CSV 'listings.csv' INTO airbnbdata;").execute()
        
        tableNew_query = cursor.query(f"""
                SELECT *
                FROM airbnbdata;
        """).df()
        print(tableNew_query)
        

        query1 = cursor.query(f"""
            SELECT *
            FROM airbnbdata;
        """).df()
        query1 = query1.dropna()
        

        
        query1["airbnbdata.price"] = query1["airbnbdata.price"].str.replace('$','') #Removing $ from price column, otherwise it will be treated as string
        query1['airbnbdata.price'] = pd.to_numeric(query1['airbnbdata.price'], errors='coerce')
        query1["airbnbdata.price"] = query1["airbnbdata.price"].astype(float) #Converting price column from string to float
        
        #Removing table name from headers in dataframe so that when it is converted to csv the column names don't have tablename as it will cause error while loading into table again
        query1.columns = query1.columns.str.split('.').str[-1] 
        
        print(query1.dtypes)

        query1.to_csv('modified.csv')
        
        
        cursor.query("DROP TABLE IF EXISTS airbnbdata_modified;").df()
        
        #Creating the table for modified airbnbdata
        cursor.query(f"""
        CREATE TABLE airbnbdata_modified (
            price INTEGER,
            review_scores_rating INTEGER,
            bedrooms INTEGER,
            last_review TEXT(1000));
            """).df()
        
        #Loading data from CSV file to modified airbnb table
        cursor.query("LOAD CSV 'modified.csv' INTO airbnbdata_modified;").execute()
        
        modified_data_query = cursor.query(f"""
                SELECT *
                FROM airbnbdata_modified;
        """).df()
        print(modified_data_query.dtypes)
        print(modified_data_query)
        
        cursor.query(f"""
            DROP FUNCTION IF EXISTS HomeSaleForecast;
            """).df()
        
        # bedRoom_data = cursor.query(f"""SELECT review_scores_rating, last_review, price
        #     FROM airbnbdata_modified
        #     where bedrooms=3
        #     """).df()
        # print(bedRoom_data)
        
        #Creating the forecasting function using neuralforecast with frequency as Monthly
        cursor.query("""
            CREATE OR REPLACE FUNCTION HomeSaleForecast FROM
                (
                SELECT review_scores_rating, last_review, price
                FROM airbnbdata_modified
                where bedrooms=3
                
                )
            TYPE Forecasting
            LIBRARY 'neuralforecast'
            PREDICT 'price'
            TIME 'last_review'
            FREQUENCY 'M'
            HORIZON 5
            AUTO 'F'
            """).df()

        
        
        
        output= cursor.query("""SELECT
                    HomeSaleForecast(4);""").df()
        print(output)


    except Exception as e:
        print(f"❗️ EvaDB Session ended with an error: {e}")