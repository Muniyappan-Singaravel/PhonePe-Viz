# required libraries and modules
import streamlit as st
import mysql.connector
import pandas as pd
import requests
import json
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from rapidfuzz import process, fuzz
from functools import partial
from streamlit_plotly_events import plotly_events


# building connection with database
connection = mysql.connector.connect(host = "localhost", user = "root", password = "muni@mysql", database = "phonepedb")
mycursor = connection.cursor()


# function for replacing state name to match geojson
def state_name_replace(df = None):
    
    original_names = [
    'Arunachal Pradesh', 'Assam', 'Chandigarh', 'Karnataka', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
    'Punjab', 'Rajasthan', 'Sikkim', 'Tripura', 'Uttarakhand', 'Telangana', 'Bihar', 'Kerala',
    'Madhya Pradesh', 'Andaman & Nicobar', 'Gujarat', 'Lakshadweep', 'Odisha',
    'Dadra and Nagar Haveli and Daman and Diu', 'Ladakh', 'Jammu & Kashmir', 'Chhattisgarh',
    'Delhi', 'Goa', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Tamil Nadu', 'Uttar Pradesh',
    'West Bengal', 'Andhra Pradesh', 'Puducherry', 'Maharashtra']

    custom_names = [
    'arunachal-pradesh', 'assam', 'chandigarh', 'karnataka', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
    'punjab', 'rajasthan', 'sikkim', 'tripura', 'uttarakhand', 'telangana', 'bihar', 'kerala',
    'madhya-pradesh', 'andaman-&-nicobar-islands', 'gujarat', 'lakshadweep', 'odisha',
    'dadra-&-nagar-haveli-&-daman-&-diu', 'ladakh', 'jammu-&-kashmir', 'chhattisgarh',
    'delhi', 'goa', 'haryana', 'himachal-pradesh', 'jharkhand', 'tamil-nadu', 'uttar-pradesh',
    'west-bengal', 'andhra-pradesh', 'puducherry', 'maharashtra']

    if df is None:
        state_name_map = dict(zip(original_names, custom_names))
        return state_name_map

    else:
        state_name_map = dict(zip(custom_names, original_names))
        df["State"] = df["State"].replace(state_name_map)
        return df 


# retrieves country level data's based on map type, year and quarter.
def whole_country(map_type, year, quarter=None):
    global india_data
    
    if map_type == "Transactions":     # retrieves transactions data
        if quarter is None:
            query = '''SELECT state AS State, year AS Year, 
                    SUM(transaction_count) AS Transaction_Count, 
                    ROUND(SUM(transaction_amount) / 1000000.0, 2) AS Transaction_Amount 
                    FROM aggregated_transactions 
                    WHERE year = %s GROUP BY state, year'''
            params = (year,)
        
        else:
            query = '''SELECT state AS State, year AS Year, quarter AS Quarter, 
                    SUM(transaction_count) AS Transaction_Count, 
                    ROUND(SUM(transaction_amount) / 1000000.0, 2) AS Transaction_Amount 
                    FROM aggregated_transactions 
                    WHERE year = %s AND quarter = %s 
                    GROUP BY state, year, quarter'''
            params = (year, quarter)
    
    elif map_type == "Users":      # retrieves users data
        if quarter is None:
            query = '''SELECT DISTINCT State, Year, 
                    SUM(Max_Registered_Users) OVER (PARTITION BY State, year) AS Registered_Users 
                    FROM (SELECT DISTINCT state AS State, 
                    year AS Year, 
    				MAX(district_total_user) OVER (PARTITION BY state, year, district) AS Max_Registered_Users
                    FROM map_users 
                    WHERE year = %s) AS District_Data'''
            params = (year,)
            
        else:
            query = '''SELECT state AS State, year AS Year, quarter AS Quarter, 
                    SUM(district_total_user) AS Registered_Users 
                    FROM map_users 
                    WHERE year = %s AND quarter = %s 
                    GROUP BY state, year, quarter'''
            params = (year, quarter)

    mycursor.execute(query, params)
    data = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]
    df = pd.DataFrame(data, columns=columns)
    
    india_data = state_name_replace(df)     # replacing state name's using predefined function 
    return india_data
    

# geojson links for all state
def zip_geo_link():
    
    original_names = [
        'Arunachal Pradesh', 'Assam', 'Chandigarh', 'Karnataka', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
        'Punjab', 'Rajasthan', 'Sikkim', 'Tripura', 'Uttarakhand', 'Telangana', 'Bihar', 'Kerala',
        'Madhya Pradesh', 'Andaman & Nicobar', 'Gujarat', 'Lakshadweep', 'Odisha',
        'Dadra and Nagar Haveli and Daman and Diu', 'Ladakh', 'Jammu & Kashmir', 'Chhattisgarh',
        'Delhi', 'Goa', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Tamil Nadu', 'Uttar Pradesh',
        'West Bengal', 'Andhra Pradesh', 'Puducherry', 'Maharashtra']
    
    geo_links = [r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/arunachal-pradesh.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/assam.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/chandigarh.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/karnataka.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/manipur.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/meghalaya.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/mizoram.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/nagaland.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/punjab.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/rajasthan.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/sikkim.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/tripura.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/uttarakhand.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/telangana.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/bihar.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/kerala.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/madhya-pradesh.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/andaman-and-nicobar-islands.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/gujarat.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/lakshadweep.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/odisha.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/dnh-and-dd.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/ladakh.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/jammu-and-kashmir.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/chandigarh.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/delhi.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/goa.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/haryana.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/himachal-pradesh.geojson",
                 r"C:\Users\Lenovo\Data Science\JHARKHAND_DISTRICTS.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/tamil-nadu.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/uttar-pradesh.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/west-bengal.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/andhra-pradesh.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/puducherry.geojson",
                 r"https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@bcbcba3/geojson/states/maharashtra.geojson"]
    
    state_geo = dict(zip(original_names, geo_links))    # zipped as dictionary, so that can call the link
    return state_geo


# getting district name of geojson data for required state 
def get_geo_dist(state_name):

    original_district = {"District" : []}
    state = zip_geo_link()[state_name]     # calling required link based on state name
    
    if state_name != "Jharkhand":    # for Jharkhand we have different file, so extraction method is different
        geo_data = requests.get(state).json()

        for feature in geo_data["features"]:
            original_district["District"].append(feature["properties"]["district"])

    else:
        with open(state, "r") as file:    # for Jharkhand
            geo_data = json.load(file)

            for feature in geo_data["features"]:
                original_district["District"].append(feature["properties"]["dtname"])

    return pd.DataFrame(original_district) 


# function to messy district name's of our database 
def clean_district_name(name):
    name = name.lower()
    name = name.replace(" district", "")
    name = name.replace("the ", "")
    name = name.strip()
    name = name.title()  
    return name


# matching database district name's to geojson district name's. 
def match_district(name, geojson_districts):
    match, score, index = process.extractOne(name, geojson_districts, scorer=fuzz.WRatio)   # using string matching library.
    return match if score >= 80 else None 


# retrieves state level data based on map type and state name, time frame
def state_wise(map_type, state_name, year, quarter=None):
    
    if map_type == "Transactions":
        if quarter is None:
            query = '''
                SELECT state AS State, year AS Year, district AS District, 
                       SUM(transaction_count) AS Transaction_Count, 
                       ROUND(SUM(transaction_amount) / 1000000.0, 2) AS Transaction_Amount 
                       FROM map_transactions 
                       WHERE state = %s AND year = %s 
                       GROUP BY state, year, district'''
            params = (state_name_replace()[state_name], year)    # to match state name as per database
        else:
            query = '''
                SELECT state AS State, year AS Year, district AS District, quarter AS Quarter, 
                       transaction_count AS Transaction_Count, 
                       ROUND(transaction_amount / 1000000.0, 2) AS Transaction_Amount 
                       FROM map_transactions 
                       WHERE state = %s AND year = %s AND quarter = %s'''
            params = (state_name_replace()[state_name], year, quarter)
            
    elif map_type == "Users":
        if quarter is None:
            query = '''SELECT DISTINCT state AS State, 
                    year AS Year, district AS District, 
                    MAX(district_total_user) OVER (PARTITION BY state, year, district) AS Registered_Users 
                    FROM map_users 
                    WHERE state = %s AND year = %s'''
            params = (state_name_replace()[state_name], year)
            
        else:
            query = '''SELECT state AS State, 
                    year AS Year, quarter AS Quarter, 
                    district AS District, 
                    district_total_user AS Registered_Users 
                    FROM map_users 
                    WHERE state = %s AND year = %s AND quarter = %s'''
            params = (state_name_replace()[state_name], year, quarter)
            
    else:
        return None  

    mycursor.execute(query, params)
    data = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]

    df = pd.DataFrame(data, columns=columns)
    df = state_name_replace(df)

    geo_dist = get_geo_dist(state_name)["District"].tolist()  # getting geojson districts of particular state
    
    # preloading geojson district names to match_district function using partial function tool
    matcher = partial(match_district, geojson_districts = geo_dist)  

    # matching both district name's of geojson and database.
    df["District"] = df["District"].apply(clean_district_name).apply(matcher)  
    
    return df


# retrieves category data of country based on time frame
def category_data_country(year, quarter = None):

    if quarter is None:
        query = '''SELECT year AS Year, transaction_type AS Transaction_Type, 
                SUM(transaction_count) AS Transaction_Count, 
                ROUND( SUM(transaction_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM aggregated_transactions 
                WHERE year = %s GROUP BY transaction_type, year'''
        params = (year,)

    else:
        query = '''SELECT year AS Year, quarter AS Quarter, transaction_type AS Transaction_Type, 
                SUM(transaction_count) AS Transaction_Count, 
                ROUND( SUM(transaction_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM aggregated_transactions WHERE year = %s AND quarter = %s 
                GROUP BY transaction_type, year, quarter'''
        params = (year, quarter)
    
    mycursor.execute(query, params)
    return pd.DataFrame(mycursor.fetchall(), columns = [col[0] for col in mycursor.description])


# retrieves category data of required state based on time frame
def category_data_state(state, year, quarter = None):

    if quarter is None:
        query = '''SELECT state AS State, year AS Year, 
                transaction_type AS Transaction_Type, 
                SUM(transaction_count) AS Transaction_Count, 
                ROUND( SUM(transaction_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM aggregated_transactions WHERE state = %s AND year = %s 
                GROUP BY transaction_type, year, state'''
        params = (state_name_replace()[state], year)

    else:
        query = '''SELECT state AS State, year AS Year, quarter AS Quarter, 
                transaction_type AS Transaction_Type, 
                SUM(transaction_count) AS Transaction_Count, 
                ROUND( SUM(transaction_amount)/ 1000000.0, 2) AS Transaction_Amount 
                FROM aggregated_transactions WHERE state = %s AND year = %s AND quarter = %s 
                GROUP BY transaction_type, quarter, year, state'''
        params = (state_name_replace()[state], year, quarter)
    
    mycursor.execute(query, params)
    return state_name_replace(pd.DataFrame(mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))


# retrieves top transaction data's based on time frame
def top_trnx_country(year, quarter = None):
    
    if quarter is None:
        query1 = '''SELECT state AS State, year AS Year, district AS District, 
                SUM(district_count) as Transaction_Count, 
                ROUND( SUM(district_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_districts WHERE year = %s 
                GROUP BY district, year, state 
                ORDER BY Transaction_Amount DESC LIMIT  10'''

        query2 = '''SELECT state AS State, year AS Year, pincode AS Pincode, 
                SUM(pincode_count) AS Transaction_Count, 
                ROUND( SUM(pincode_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_pincodes WHERE year = %s 
                GROUP BY pincode, year, state 
                ORDER BY Transaction_Amount DESC LIMIT 10'''
        params = (year,)

    else:
        query1 = '''SELECT state AS State, year AS Year, quarter AS Quarter, district AS District, 
                SUM(district_count) as Transaction_Count, 
                ROUND( SUM(district_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_districts 
                WHERE year = %s AND quarter = %s 
                GROUP BY district, year, quarter, state 
                ORDER BY Transaction_Amount DESC LIMIT  10'''

        query2 = '''SELECT state AS State, year AS Year, quarter AS Quarter, pincode AS Pincode, 
                SUM(pincode_count) AS Transaction_Count, 
                ROUND( SUM(pincode_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_pincodes
                WHERE year = %s AND quarter = %s 
                GROUP BY pincode, year, state 
                ORDER BY Transaction_Amount DESC LIMIT 10'''
        params = (year, quarter)

    mycursor.execute(query1, params)
    top_trnx_dist = state_name_replace(pd.DataFrame( mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))
    top_trnx_dist["District"] = top_trnx_dist["District"].apply(clean_district_name)

    mycursor.execute(query2, params)
    top_trnx_pin = state_name_replace(pd.DataFrame( mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))

    return top_trnx_dist, top_trnx_pin  # retrieves both district and pincode level


# retrieves top transaction data's based on state name and time frame
def top_trnx_state(state, year, quarter = None):
    
    if quarter is None:
        query1 = '''SELECT state AS State, year AS Year, district AS District, 
                SUM(district_count) as Transaction_Count, 
                ROUND( SUM(district_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_districts 
                WHERE state = %s AND year = %s 
                GROUP BY district, year, state 
                ORDER BY Transaction_Amount DESC LIMIT 10'''

        query2 = '''SELECT state AS State, year AS Year, pincode AS Pincode, 
                SUM(pincode_count) AS Transaction_Count, 
                ROUND( SUM(pincode_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_pincodes
                WHERE state = %s AND year = %s 
                GROUP BY pincode, year 
                ORDER BY Transaction_Amount DESC LIMIT 10'''
        params = (state_name_replace()[state], year)

    else:
        query1 = '''SELECT state AS State, year AS Year, quarter AS Quarter, district AS District, 
                SUM(district_count) as Transaction_Count, 
                ROUND( SUM(district_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_districts 
                WHERE state = %s AND year = %s AND quarter = %s 
                GROUP BY district, year, quarter 
                ORDER BY Transaction_Amount DESC LIMIT 10'''

        query2 = '''SELECT state AS State, year AS Year, quarter AS Quarter, pincode AS Pincode, 
                SUM(pincode_count) AS Transaction_Count, 
                ROUND( SUM(pincode_amount) / 1000000.0, 2) AS Transaction_Amount 
                FROM top_transaction_pincodes
                WHERE state = %s AND year = %s AND quarter = %s 
                GROUP BY pincode, year 
                ORDER BY Transaction_Amount DESC LIMIT 10'''
        params = (state_name_replace()[state], year, quarter)

    
    mycursor.execute(query1, params)
    top_trnx_dist = state_name_replace(pd.DataFrame(mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))
    top_trnx_dist["District"] = top_trnx_dist["District"].apply(clean_district_name)   # no need for choropleth so just cleaning the names 

    mycursor.execute(query2, params)
    top_trnx_pin = state_name_replace(pd.DataFrame(mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))

    return top_trnx_dist, top_trnx_pin    # both district and pincode data's


# retrieves top users data's based on time frame
def top_users_country(year, quarter = None):
    
    if quarter is None:
        query1 = '''SELECT state AS State, year AS Year, 
                top_user_districts AS District,
                top_rgstrd_usr_in_dist AS Registered_Users 
                FROM top_user_districts 
                WHERE year = %s 
                ORDER BY Registered_Users DESC LIMIT 10'''

        query2 = '''SELECT state AS State, year AS Year, 
                top_user_pincode AS Pincode, 
                top_rgstrd_usr_in_pincode AS Registered_Users 
                FROM top_user_pincodes
                WHERE year = %s
                ORDER BY Registered_Users DESC LIMIT 10'''
        params = (year,)

    else:
        query1 = '''SELECT state AS State, year AS Year, 
                quarter AS Quarter, top_user_districts AS District, 
                top_rgstrd_usr_in_dist AS Registered_Users 
                FROM top_user_districts 
                WHERE year = %s AND quarter = %s 
                ORDER BY Registered_Users DESC LIMIT 10'''

        query2 = '''SELECT state AS State, year AS Year, 
                quarter AS Quarter, top_user_pincode AS Pincode, 
                top_rgstrd_usr_in_pincode AS Registered_Users 
                FROM top_user_pincodes
                WHERE year = %s AND quarter = %s
                ORDER BY Registered_Users DESC LIMIT 10'''
        params = (year, quarter)

    
    mycursor.execute(query1, params)
    top_user_dist = state_name_replace(pd.DataFrame( mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))
    top_user_dist["District"] = top_user_dist["District"].apply(clean_district_name)

    mycursor.execute(query2, params)
    top_user_pin = state_name_replace(pd.DataFrame(mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))

    return top_user_dist, top_user_pin    # both district and pincode data's


# retrieves top users data's based on state and time frame
def top_users_state(state, year, quarter = None):

    if quarter is None:
        query1 = '''SELECT state AS State, year AS Year, 
                top_user_districts AS District, 
                top_rgstrd_usr_in_dist AS Registered_Users 
                FROM top_user_districts 
                WHERE state = %s AND year = %s 
                ORDER BY Registered_Users DESC LIMIT 10'''

        query2 = '''SELECT state AS State, year AS Year, 
                top_user_pincode AS Pincode, 
                top_rgstrd_usr_in_pincode AS Registered_Users 
                FROM top_user_pincodes
                WHERE state = %s AND year = %s 
                ORDER BY Registered_Users DESC LIMIT 10'''
        params = (state_name_replace()[state], year)

    else:
        query1 = '''SELECT state AS State, year AS Year, quarter AS Quarter, 
                top_user_districts AS District, 
                top_rgstrd_usr_in_dist AS Registered_Users 
                FROM top_user_districts 
                WHERE state = %s AND year = %s AND quarter = %s 
                ORDER BY Registered_Users DESC LIMIT 10'''

        query2 = '''SELECT state AS State, year AS Year, quarter AS Quarter, 
                top_user_pincode AS Pincode, 
                top_rgstrd_usr_in_pincode AS Registered_Users 
                FROM top_user_pincodes
                WHERE state = %s AND year = %s AND quarter = %s 
                ORDER BY Registered_Users DESC LIMIT 10'''
        params = (state_name_replace()[state], year, quarter)

    mycursor.execute(query1, params)
    top_user_dist = state_name_replace(pd.DataFrame(mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))
    top_user_dist["District"] = top_user_dist["District"].apply(clean_district_name)

    mycursor.execute(query2, params)
    top_user_pin = state_name_replace(pd.DataFrame(mycursor.fetchall(), columns = [col[0] for col in mycursor.description]))

    return top_user_dist, top_user_pin    # both district and pincode data's


# choropleth graph to explore dynamically country level details base on map type and time frame
def country_choropleth(map_type, year, quarter=None):
    india_data = whole_country(map_type, year, quarter)

    india_geo = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

    # different parameters for Transactions map type
    if map_type == "Transactions":     
        hover_cols = ["State", "Transaction_Count", "Transaction_Amount"]
        hovertemplate = (
            "<b>%{customdata[0]}</b><br>"
            "Transactions: %{customdata[1]:,}<br>"
            "Amount: ₹%{customdata[2]:,} Cr<extra></extra>")
        color_col = "Transaction_Amount"
        
    # different parameters for Users map type
    else:
        hover_cols = ["State", "Registered_Users"]
        hovertemplate = (
            "<b>%{customdata[0]}</b><br>"
            "Registered Users: %{customdata[1]:,}<extra></extra>")
        color_col = "Registered_Users"

    fig = go.Figure(go.Choropleth(
        geojson = india_geo,
        locations = india_data["State"],
        z = india_data[color_col],      # color parameter
        featureidkey = "properties.ST_NM",
        customdata = india_data[hover_cols],
        colorscale = "Viridis",
        marker_line_width = 0.5,
        marker_line_color = 'white',
        hovertemplate = hovertemplate,
        hoverlabel = dict(
            bgcolor = "#4b3d8f",
            font = dict(color="white", size=14, family="Arial")),
        selected = dict(marker = dict(opacity = 1)),   
        unselected = dict(marker = dict(opacity = 0.7)),   # parameter to visually differentiate unselected states
        showscale = False))

    fig.update_geos(fitbounds="locations", visible=False)  # make's only country visible

    fig.update_layout(
        height = 600,
        margin = {"r": 0, "t": 0, "l": 0, "b": 0},
        clickmode = 'event+select',    # enabling click mode to capture the state name
        geo = dict(bgcolor = "white"),
        plot_bgcolor = "white",
        paper_bgcolor = "white",
        dragmode = "zoom",
        showlegend = False)

    return fig


# choropleth graph to explore dynamically state level details base on state, map type, and time frame
def state_choropleth(map_type, state_name, year, quarter=None):
    
    state_data = state_wise(map_type, state_name, year, quarter)
    geo_data = zip_geo_link()[state_name]

    # different geojson file and featureidkey between Jharkhand and other states
    if state_name == "Jharkhand":
        with open(geo_data, "r") as file:
            geo_file = json.load(file)
        featureidkey = "properties.dtname"
    else:
        geo_file = geo_data
        featureidkey = "properties.district"

    # different attributes for different map type
    if map_type == "Transactions":
        color_col = "Transaction_Count"  
        hover_cols = ["District", "Transaction_Count", "Transaction_Amount"]
        range_color = (state_data["Transaction_Count"].min(), state_data["Transaction_Count"].max())
        hovertemplate = (
            "<b>%{customdata[0]}</b><br>"
            "Transactions: %{customdata[1]:,}<br>"
            "Amount: ₹%{customdata[2]:,} Cr<extra></extra>")
        
    else:
        color_col = "Registered_Users"
        hover_cols = ["District", "Registered_Users"]
        range_color = (state_data["Registered_Users"].min(), state_data["Registered_Users"].max())
        hovertemplate = (
            "<b>%{customdata[0]}</b><br>"
            "Registered Users: %{customdata[1]:,}<extra></extra>")

    fig = go.Figure(go.Choropleth(
        geojson = geo_file,
        locations = state_data["District"],
        z = state_data[color_col],
        featureidkey = featureidkey,
        customdata = state_data[hover_cols],
        colorscale = "Viridis",
        zmin = range_color[0],
        zmax = range_color[1],
        marker_line_width = 0.5,
        marker_line_color = 'white',
        hovertemplate = hovertemplate,
        hoverlabel = dict(
            bgcolor = "#4b3d8f",
            font = dict(color = "white", size = 14, family = "Arial")),
        showscale = False))

    fig.update_geos(            
        fitbounds = "locations",   # makes visible of that particular state 
        visible = False,
        bgcolor = 'rgba(0,0,0,0)')

    fig.update_layout(
        height=600,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        dragmode="zoom",
        showlegend=False)

    return fig


# Pie chart for category level transaction analysis based on state and time frame
def category_chart(year, state = None, quarter = None):

    # dynamically calling predefined functions for source data based on constraints 
    if state is None:
        if quarter is None:
            data = category_data_country(year)
        else:
            data = category_data_country(year, quarter)
            
    elif quarter is None:
        data = category_data_state(state, year)
        
    else:
        data = category_data_state(state, year, quarter)

    fig = go.Figure()

    # chart that displays Transaction_Count details
    fig.add_trace(go.Pie(   
        labels = data["Transaction_Type"],
        values = data["Transaction_Count"],
        name = "No of Count",
        domain = {"x" : [0, 0.44]},
        title = "<b>By Transaction Count<b>",
        marker = {"colors" : px.colors.qualitative.Prism},
        hovertemplate = "<b>%{label}</b><br>" +
                        "<b>Count:<b> %{value}<br>" +
                        "<b>Percentage:<b> %{percent}<extra></extra>",
        hoverlabel = dict(bgcolor = "#4b3d8f", font = dict(color = "white", size = 12, family = "Arial"))))

    # chart that displays Transaction_Amount details
    fig.add_trace(go.Pie(
        labels = data["Transaction_Type"],
        values = data["Transaction_Amount"],
        name = "Amount in Cr",
        domain = {"x" : [0.56, 1]},
        title = "<b>By Transaction Amount<b>",
        marker = {"colors" : px.colors.qualitative.Prism},
        hovertemplate = "<b>%{label}</b><br>" +
                        "<b>Amount:<b> ₹%{value:,.2f} Cr<br>" + 
                        "<b>Percentage:<b> %{percent}<extra></extra>",
        hoverlabel=dict(bgcolor = "#4b3d8f", font = dict(color = "white", size = 12, family = "Arial"))))

    fig.update_layout(
        title = "<b>Transaction Distribution By Category<b>",
        template = "plotly_white")
    
    return fig


# graph for dynamically analyzing top transaction details
def top_trnx_chart(year, state = None, quarter = None):

    if state is None:
        if quarter is None:
            district_data = top_trnx_country(year)[0]
            pincode_data = top_trnx_country(year)[1]
        else:
            district_data = top_trnx_country(year, quarter)[0]
            pincode_data = top_trnx_country(year, quarter)[1]

    elif quarter is None:
        district_data = top_trnx_state(state, year)[0]
        pincode_data = top_trnx_state(state, year)[1]

    else:
        district_data = top_trnx_state(state, year, quarter)[0]
        pincode_data = top_trnx_state(state, year, quarter)[1]

    subtitle = ("Transaction Count by District", "Transaction Amount by District", 
                "Transaction Count by Pincode", "Transaction Amount by Pincode")
    
    fig = make_subplots(rows = 2, cols = 2,
                        subplot_titles = subtitle,
                        vertical_spacing = 0.25,
                        horizontal_spacing = 0.2)

    # graph for district datas
    fig.add_trace(go.Bar(x = district_data["District"].index,
                         y = district_data["Transaction_Count"],
                         name = "No of Counts",
                         marker_color = "#6A4C93",
                         hovertemplate = ("<b>State:</b> %{customdata[0]}<br>"
                                          "<b>District:</b> %{customdata[1]}<br>"
                                          "<b>Count:</b> %{y:,}<extra></extra>"),
                         customdata = district_data[["State", "District"]]),
                         row = 1, col =1)

    fig.add_trace(go.Bar(x = district_data["District"].index,
                         y = district_data["Transaction_Amount"],
                         name = "Amount (₹ Cr)",
                         marker_color = "#F25F5C",
                         hovertemplate = ("<b>State:</b> %{customdata[0]}<br>"
                                          "<b>District:</b> %{customdata[1]}<br>"
                                          "<b>Amount:</b> ₹%{y:,.2f} Cr<extra></extra>"),
                         customdata = district_data[["State", "District"]]),
                         row = 1, col = 2)

    # graph for pincode datas
    fig.add_trace(go.Bar(x = pincode_data["Pincode"].index,
                         y = pincode_data["Transaction_Count"],
                         name = "No of Counts",
                         marker_color = "#2EC4B6",
                         hovertemplate = ("<b>State:</b> %{customdata[0]}<br>"
                                          "<b>Pincode:</b> %{customdata[1]}<br>"
                                          "<b>Count:</b> %{y:,}<extra></extra>"),
                         customdata = pincode_data[["State", "Pincode"]]),
                         row = 2, col = 1)

    fig.add_trace(go.Bar(x = pincode_data["Pincode"].index,
                         y = pincode_data["Transaction_Amount"],
                         name = "Amount (₹ Cr)",
                         marker_color = "#FFBF69",
                         hovertemplate = ("<b>State:</b> %{customdata[0]}<br>"
                                          "<b>Pincode:</b> %{customdata[1]}<br>"
                                          "<b>Amount:</b> ₹%{y:,.2f} Cr<extra></extra>"),
                         customdata = pincode_data[["State", "Pincode"]]),
                         row = 2, col = 2)

    # layout updates
    fig.update_layout(title_text = "Top 10 Transaction Analysis",
                      title_font = {"family" : "Arial", "size" : 24, "weight" : "bold"},
                      template = "plotly_white",
                      showlegend = False,
                      height = 800,
                      hovermode = "x unified",
                      yaxis1 = {"title" : "Count"},
                      yaxis2 = {"title" : "Amount (₹ Crores)", "tickprefix" : "₹"},
                      xaxis1 = {"title" : "District"},
                      xaxis2 = {"title" : "District"},
                      yaxis3 = {"title" : "Count"},
                      yaxis4 = {"title" : "Amount (₹ Crores)", "tickprefix" : "₹"},
                      xaxis3 = {"title" : "Pincode"},
                      xaxis4 = {"title" : "Pincode"})

    fig.update_annotations(font = {"family" : "Gravitas One", "size" : 18, "weight" : "bold"})
    return fig


# graph for dynamically analyzing top users details
def top_users_chart(year, state = None, quarter = None):

    if state is None:
        if quarter is None:
            district_data = top_users_country(year)[0]
            pincode_data = top_users_country(year)[1]
        else:
            district_data = top_users_country(year, quarter)[0]
            pincode_data = top_users_country(year, quarter)[1]
    elif quarter is None:
        district_data = top_users_state(state, year)[0]
        pincode_data = top_users_state(state, year)[1]

    else:
        district_data = top_users_state(state, year, quarter)[0]
        pincode_data = top_users_state(state, year, quarter)[1]

    fig = make_subplots(rows = 1, cols = 2,
                        subplot_titles = ("Users by District", "Users by Pincode"),
                        horizontal_spacing = 0.2)

    # graph for district datas
    fig.add_trace(go.Bar(x = district_data["District"].index,
                         y = district_data["Registered_Users"],
                         name = "No of Users in District",
                         marker_color = "#2E86AB",
                         customdata = district_data[["State", "District"]],
                         hovertemplate = ("<b>State:</b> %{customdata[0]}<br>"
                                           "<b>District:</b> %{customdata[1]}<br>"
                                           "<b>Registered Users:</b> %{y:,}<extra></extra>")),
                         row = 1, col = 1)

    # graph for pincode datas
    fig.add_trace(go.Bar(x = pincode_data["Pincode"].index,
                         y = pincode_data["Registered_Users"],
                         name = "No of Users in Pincode",
                         marker_color = "#F18F01",
                         customdata = pincode_data[["State", "Pincode"]],
                         hovertemplate = ("<b>State:</b> %{customdata[0]}<br>"
                                           "<b>Pincode:</b> %{customdata[1]}<br>"
                                           "<b>Registered Users:</b> %{y:,}<extra></extra>")),
                         row = 1, col = 2)

    # layout updates
    fig.update_layout(title = "Top 10 Users Analysis",
                      title_font = {"family" : "Arial", "size" : 24, "weight" : "bold"},
                      template = "plotly_white",
                      height = 600,
                      showlegend = False,
                      hovermode = "x unified",
                      yaxis1 = {"title" : "Count"},
                      yaxis2 = {"title" : "Count"},
                      xaxis1 = {"title" : "District"},
                      xaxis2 = {"title" : "Pincode"})

    fig.update_annotations(font = {"family" : "Gravitas One", "size" : 18, "weight" : "bold"})
    return fig


# function for hierarchical analyze about users mobile brand
def mobile_brand_users():

    mycursor.execute('''select state as State, 
                    year as Year, quarter as Quarter, 
                    total_user AS Total_Users, brand AS Brand, 
                    brand_user_count AS Brand_User_Count
                    from aggregated_users''')
    data = mycursor.fetchall()
    columns = [col[0] for col in mycursor.description]

    df = state_name_replace(pd.DataFrame(data, columns = columns))

    # small annoymous funtion to find out the brand % 
    df["Percentage"] = df.groupby(["Year", "Quarter", "Brand"])["Brand_User_Count"] \
                                  .transform(lambda x : round(x / x.sum() * 100, 2)).astype(str) + "%"
    
    df["Quarter"] = df["Quarter"].apply(lambda x : f"Q{x}")

    # using sunburst to visualize datas hierarchically
    fig = px.sunburst(df,
                 path = ["Year", "Quarter", "Brand", "State"],
                 values = "Brand_User_Count",
                 custom_data = [ "Year", "Quarter","Total_Users", "Percentage"],
                 branchvalues = "total",
                 maxdepth = 2)

    fig.update_traces(marker = 
                     dict(colorscale = "Sunset",
                         cmin = df["Brand_User_Count"].min(),
                         cmax = df["Brand_User_Count"].max()))

    fig.update_traces(hovertemplate = 
                      "<b>%{label}</b><br>"
                      "<b>Brand Users:<b> %{value}<br>"
                      "<b>Percentage:<b> %{customdata[3]}<br>"
                      "<b>Year:<b> %{customdata[0]}<br>"
                      "<b>Quarter:<b> %{customdata[1]}<br>"
                      "<b>State Total Users:<b> %{customdata[2]:,}")

    fig.update_traces(
        insidetextorientation='radial',
        textfont_size=12,                   
        textinfo='label+percent parent',    
        texttemplate='%{label}<br>%{percentParent:.1%}')

    fig.update_layout(margin = dict(t = 10, l = 10, r = 10, b = 10))
    return fig


# getting years from database for time frame analysis
def years():
    mycursor.execute("select distinct(year) from aggregated_transactions")
    years = [str(yr[0]) for yr in mycursor.fetchall()]
    return years


# quarters for time frame analysis
def quarters():
     return  {"Q1 : Jan-Mar" : 1,
                "Q2 : Apr-Jun" : 2,
                "Q3 : Jul-Sep" : 3,
                "Q4 : Oct-Dec" : 4}


st.cache_data
# building interactive charts to analyse both country and state by click
def interactive_map_view(map_type, selected_year, quarter):
    
    if 'current_state' not in st.session_state:   
        st.session_state.current_state = None

    if st.session_state.current_state:    # switches to state map 
        st.subheader(f":gray[District View :] :green[{st.session_state.current_state}]")
        fig = state_choropleth(map_type, st.session_state.current_state, selected_year, quarter)
        st.plotly_chart(fig, use_container_width=True)

        if st.button(":red-background[Back to Country Map]"):   # moves back to country map
            st.session_state.current_state = None
            st.rerun()

    else:
        fig = country_choropleth(map_type, selected_year, quarter)   # country map
        selected_points = plotly_events(
            fig,
            click_event=True,
            hover_event=False,
            override_height=1000,
            override_width="100%",
            key="country_click_event")   # captures state name on the click

        st.markdown(selected_points)

        if selected_points:   # stores state name and reruns the function
            st.session_state.current_state = india_data.iloc[selected_points[0]["pointIndex"]]["State"]
            st.rerun()


# streamlit home page
def home_page():
    st.markdown("### :violet[PhonePe Pulse Data Explorer]")
    st.markdown("#### *Interactive Maps:*")
    st.write("Country Choropleth: Click any state to drill down into district-level data.")
    st.write("State-Level Views: Detailed maps for transactions/users by district.")
    st.markdown("#### *Time-Frame Filters:*")
    st.write("Analyze data by year (2018–2024) and quarter.")
    st.markdown("#### *Transaction Insights:*")
    st.write("Total transaction counts and values (state/district level).")
    st.markdown("#### *User Analytics:*")
    st.write("Registered user growth trends over time.")
    st.markdown("#### *Transaction Categories:*")
    st.write("Pie charts for volume/value by category (e.g., P2P, merchant payments).")
    st.markdown("#### *Top 10 Rankings:*")
    st.write("Top districts/pincodes by transaction volume & value per state.")
    st.markdown("#### *Mobile Brand Data (2018–2022 Q1):*")
    st.write("Sunburst charts for hierarchical view of user device brands.")
    st.markdown("#### *Data Pipeline:*")
    st.write("JSON → SQL: Converted raw PhonePe JSON files to structured MySQL tables using Python.")
    st.write("SQL Queries: Extracted and transformed data into analysis-ready DataFrames.")
    st.markdown("#### *Dynamic SQL Backend:*")
    st.write("Fetches filtered data from MySQL based on user inputs.")
    st.markdown("#### *Comparative Tools:*")
    st.write("Compare states/districts across metrics and time periods.")
    st.markdown("#### *Real-Time Updates:*")
    st.write("Visuals adjust instantly to time-range selections.")
    st.markdown("#### *Custom Visualizations:*")
    st.write("Plotly choropleths, bar/pie charts, and sunburst diagrams.")
    st.markdown("#### *Tech Stack:*")
    st.write("Python, Streamlit, Plotly, MySQL, and Pandas for data wrangling.")
    st.markdown("#### *Data Source:*")
    st.write("PhonePe Pulse GitHub (public dataset).")


# streamlit sidebar for time frame selection
def sidebar_filter():
    st.sidebar.markdown("### :blue-background[Select Datatype]")
    type = st.sidebar.selectbox("### Choose type", ["Transactions", "Users"])

    st.sidebar.markdown("### :blue-background[Select Timeframe]")

    if 'selected_year' not in st.session_state:
        st.session_state.selected_year = "2018"

    selected_year = st.sidebar.selectbox("Select Year", years(), index=years().index(st.session_state.selected_year))

    if selected_year != st.session_state.selected_year:
        st.session_state.selected_year = selected_year
        st.rerun()

    if 'quarter' not in st.session_state:
        st.session_state.quarter = "The Whole Year"

    quarter_options = ["The Whole Year"] + list(quarters().keys())
    quarter_index = quarter_options.index(st.session_state.quarter)

    selected_quarter = st.sidebar.radio(":blue-background[Select Quarter]", quarter_options, index=quarter_index)
    st.session_state.quarter = selected_quarter
    quarter = None if selected_quarter == "The Whole Year" else quarters()[selected_quarter]

    return type, st.session_state.selected_year, quarter


# function to display all transaction related details 
def transaction_tabs(year, quarter):
    st.subheader(":orange-background[Choose Tabs]")
    tab = st.tabs([":blue[Choropleth]", ":blue[Categories]", ":blue[Top Transaction Analysis]"])

    with tab[0]:
        st.markdown(":green[_Select State_]")
        interactive_map_view("Transactions", year, quarter)

    with tab[1]:
        fig = category_chart(year=year, state=st.session_state.current_state, quarter=quarter)
        st.plotly_chart(fig, use_container_width=True)

    with tab[2]:
        fig = top_trnx_chart(year=year, state=st.session_state.current_state, quarter=quarter)
        st.plotly_chart(fig, use_container_width=True)


# function to display all user related details
def user_tabs(year, quarter):
    st.subheader(":orange-background[Choose Tabs]")
    tab = st.tabs(["Choropleth", "Top Users Analysis", "Mobile Brand Analysis"])

    with tab[0]:
        st.markdown(":green[**Select State**]")
        interactive_map_view("Users", year, quarter)

    with tab[1]:
        fig = top_users_chart(year=year, state=st.session_state.current_state, quarter=quarter)
        st.plotly_chart(fig, use_container_width=True)

    with tab[2]:
        st.subheader(":orange[_Mobile Brands By Year & Quarter_]")
        st.markdown(":green[_Click On The Label_]")
        fig = mobile_brand_users()
        st.plotly_chart(fig, use_container_width=False)


# combining both transaction & users function
def explore_data_page():
    type, year, quarter = sidebar_filter()
    
    if type == "Transactions":
        transaction_tabs(year, quarter)
    else:
        user_tabs(year, quarter)


# streamlit setup.
st.title(":violet[PhonePe Pulse Visualization]")

column = st.tabs([":green-background[Home Page]", ":green-background[Explore Data]"])

with column[0]:
    home_page()

with column[1]:
    explore_data_page()
    

#__End__...
            
                
