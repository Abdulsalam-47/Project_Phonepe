import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import mysql.connector
import plotly.express as px
import requests
import json
from PIL import Image


#Dataframe creation

#sql connection
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe"
)

cursor = mydb.cursor()

#aggre_insurance_df
cursor.execute("SELECT * FROM aggregated_insurance")
agg_insur_table = cursor.fetchall()
mydb.commit()
columns = ["States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"]
aggregate_insurance=pd.DataFrame(agg_insur_table, columns=columns)


#aggre_transactions_df
cursor.execute("SELECT * FROM aggregated_transactions")
agg_trans_table = cursor.fetchall()
mydb.commit()
aggregate_transactions=pd.DataFrame(agg_trans_table, columns=columns)


#aggre_users_df
cursor.execute("SELECT * FROM aggregated_users")
agg_users_table = cursor.fetchall()
mydb.commit()
columns1 = ["States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"]
aggregate_users=pd.DataFrame(agg_users_table, columns=columns1)


#map_insurance_df
cursor.execute("SELECT * FROM map_insurance")
map_insur_table = cursor.fetchall()
mydb.commit()
map_columns = ["States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"]
map_insurance=pd.DataFrame(map_insur_table, columns=map_columns)


#map_insurance_df
cursor.execute("SELECT * FROM map_transactions")
map_trans_table = cursor.fetchall()
mydb.commit()
map_transactions=pd.DataFrame(map_insur_table, columns=map_columns)


#map_users_df
cursor.execute("SELECT * FROM map_users")
map_users_table = cursor.fetchall()
mydb.commit()
map_users_columns = ["States", "Years", "Quarter", "Districts", "RegisteredUsers", "AppOpens"]
map_users=pd.DataFrame(map_users_table, columns=map_users_columns)


#top_insurance_df
cursor.execute("SELECT * FROM top_insurance")
top_insur_table = cursor.fetchall()
mydb.commit()
top_columns = ["States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"]
top_insurance=pd.DataFrame(top_insur_table, columns=top_columns)


#top_transactions_df
cursor.execute("SELECT * FROM top_transactions")
top_trans_table = cursor.fetchall()
mydb.commit()
top_transactions=pd.DataFrame(top_trans_table, columns=top_columns)


#top_users_df
cursor.execute("SELECT * FROM top_users")
top_trans_table = cursor.fetchall()
mydb.commit()
top_users_columns = ["States", "Years", "Quarter", "Pincodes", "RegisteredUsers"]
top_users=pd.DataFrame(top_trans_table, columns=top_users_columns)


def Transaction_amount_count_Y(df, year):
    tacy=df[df["Years"] == year]
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)
    
    col1,col2=st.columns(2)
    with col1:

        fig_amount=px.bar(tacyg,x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    with col2:

        fig_count=px.bar(tacyg,x="States", y="Transaction_count", title=f"{year} TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered, height=650, width=600)
        st.plotly_chart(fig_count)


    col1,col2=st.columns(2)
    with col1:

        url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        response=requests.get(url)

        json_data=json.loads(response.content)

        states_names=[]
        for feature in json_data["features"]:
            states_names.append(feature["properties"]["ST_NM"])

        states_names.sort()

        fig_india_1 = px.choropleth(tacyg, 
                                geojson=json_data, 
                                locations="States", 
                                featureidkey="properties.ST_NM",
                                color="Transaction_amount", 
                                color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name="States",
                                title=f"{year} TRANSACTION AMOUNT",
                                fitbounds="locations", height=600, width=600
                                )
        
        fig_india_1.update_geos(visible=False)
        st.plotly_chart(fig_india_1)

    with col2:

        fig_india_2 = px.choropleth(tacyg, 
                                geojson=json_data, 
                                locations="States", 
                                featureidkey="properties.ST_NM",
                                color="Transaction_count", 
                                color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name="States",
                                title=f"{year} TRANSACTION COUNT",
                                fitbounds="locations", height=600, width=600
                                )
        
        fig_india_2.update_geos(visible=False)
        st.plotly_chart(fig_india_2)

    return tacy


def Transaction_amount_count_Q(df, quarter):
    tacy=df[df["Quarter"] == quarter]
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)
    
    col1,col2=st.columns(2)
    with col1:

        fig_amount=px.bar(tacyg,x="States", y="Transaction_amount", title=f"{tacy["Years"].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    with col2:

        fig_count=px.bar(tacyg,x="States", y="Transaction_count", title=f"{tacy["Years"].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_count)

    col1,col2=st.columns(2)
    with col1:

        url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

        response=requests.get(url)

        json_data=json.loads(response.content)

        states_names=[]
        for feature in json_data["features"]:
            states_names.append(feature["properties"]["ST_NM"])

        states_names.sort()

        fig_india_3 = px.choropleth(tacyg, 
                                geojson=json_data, 
                                locations="States", 
                                featureidkey="properties.ST_NM",
                                color="Transaction_amount", 
                                color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name="States",
                                title=f"{tacy["Years"].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
                                fitbounds="locations", height=600, width=600
                                )
        
        fig_india_3.update_geos(visible=False)
        st.plotly_chart(fig_india_3)

    with col2:

        fig_india_4 = px.choropleth(tacyg, 
                                geojson=json_data, 
                                locations="States", 
                                featureidkey="properties.ST_NM",
                                color="Transaction_count", 
                                color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name="States",
                                title=f"{tacy["Years"].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",
                                fitbounds="locations", height=600, width=600
                                )
        
        fig_india_4.update_geos(visible=False)
        st.plotly_chart(fig_india_4)
    
    return tacy


def Aggre_trans_type(df, state):

    tacy1=df[df["States"] == state]
    tacy1.reset_index(drop=True, inplace=True)

    tacyg1=tacy1.groupby("Transaction_type")[["Transaction_count","Transaction_amount"]].sum()
    tacyg1.reset_index(inplace=True)
    
    col1,col2=st.columns(2)
    with col1:
        fig_pie_1=px.pie(data_frame=tacyg1, names = "Transaction_type", values= "Transaction_amount",
                    width=600, title= f"{state.upper()} TRANSACTION AMOUNT")
        st.plotly_chart(fig_pie_1)

    with col2:
        fig_pie_2=px.pie(data_frame=tacyg1, names = "Transaction_type", values= "Transaction_count",
                    width=600, title= f"{state.upper()} TRANSACTION COUNT")
        st.plotly_chart(fig_pie_2)


# Aggregated user Analysis year based
def Aggre_user_plot_1(df,year):
    aggre_user_year=df[df["Years"]==year]
    aggre_user_year.reset_index(drop=True,inplace= True)

    aggre_user_year_grp=pd.DataFrame(aggre_user_year.groupby("Brands")["Transaction_count"].sum())
    aggre_user_year_grp.reset_index(inplace= True)

    fig_bar_1=px.bar(aggre_user_year_grp,x="Brands", y="Transaction_count", title=f"{year} BRANDS AND TRANSACTION COUNT",
                    width=1000, color_discrete_sequence=px.colors.sequential.Agsunset, hover_name="Brands")
    st.plotly_chart(fig_bar_1)

    return aggre_user_year


# Aggregated User Analysis quarter based
def Aggre_user_plot_2(df,quarter):
    aggre_user_year_quarter=df[df["Quarter"]== quarter]
    aggre_user_year_quarter.reset_index(drop=True,inplace= True)

    aggre_user_year_quarter_grp=pd.DataFrame(aggre_user_year_quarter.groupby("Brands")["Transaction_count"].sum())
    aggre_user_year_quarter_grp.reset_index(inplace= True)

    fig_bar_2=px.bar(aggre_user_year_quarter_grp,x="Brands", y="Transaction_count", title=f"{quarter} QUARTER, BRANDS AND TRANSACTION COUNT",
                        width=1000, color_discrete_sequence=px.colors.sequential.Aggrnyl, hover_name="Brands")
    st.plotly_chart(fig_bar_2)

    return aggre_user_year_quarter


#Aggregated User Analysis state based
def Aggre_user_plot_3(df,state):
    Aggre_user_y_q_s=df[df["States"]== state]
    Aggre_user_y_q_s.reset_index(drop=True,inplace= True)

    flg_line_1=px.line(Aggre_user_y_q_s, x="Brands",y="Transaction_count", hover_data="Percentage",
                        title=f"{state.upper()} BRANDS, TRANSACTION COUNT, PERCENTAGE",width=1000, markers=True)
    st.plotly_chart(flg_line_1)

    return Aggre_user_y_q_s


#Map District based
def map_dist(df, state):

    tacy1=df[df["States"] == state]
    tacy1.reset_index(drop=True, inplace=True)

    tacyg1=tacy1.groupby("Districts")[["Transaction_count","Transaction_amount"]].sum()
    tacyg1.reset_index(inplace=True)

    col1,col2=st.columns(2)
    with col1:
        map_fig_bar_1=px.bar(tacyg1, x="Transaction_amount",y="Districts", orientation="h", height=600, width=600,
                            title=f"{state.upper()} DISTRICTS AND TRANSACTION AMOUNT", color_discrete_sequence=px.colors.sequential.Magenta_r)
        st.plotly_chart(map_fig_bar_1)
    
    with col2:
        map_fig_bar_2=px.bar(tacyg1, x="Transaction_count",y="Districts", orientation="h", height=600, width=600,
                            title=f"{state.upper()} DISTRICTS AND TRANSACTION AMOUNT", color_discrete_sequence=px.colors.sequential.Mint_r)
        st.plotly_chart(map_fig_bar_2)

    return tacy1


#Map user 
def map_user_plot_1(df,year):
    map_user_year=df[df["Years"]== year]
    map_user_year.reset_index(drop=True,inplace= True)

    map_user_year_grp=pd.DataFrame(map_user_year.groupby("States")[["RegisteredUsers","AppOpens"]].sum())
    map_user_year_grp.reset_index(inplace= True)

    fig_line_1=px.line(map_user_year_grp, x="States",y=["RegisteredUsers", "AppOpens"],
                            title=f"{year} REGISTERED USERS AND APPOPENS",width=1000, height=800, markers=True)
    st.plotly_chart(fig_line_1)

    return map_user_year

#Map user plt 2
def map_user_plot_2(df,quarter):
    map_user_y_q=df[df["Quarter"]== quarter]
    map_user_y_q.reset_index(drop=True,inplace= True)

    map_user_y_q_grp=map_user_y_q.groupby("States")[["RegisteredUsers","AppOpens"]].sum()
    map_user_y_q_grp.reset_index(inplace= True)

    fig_line_1=px.line(map_user_y_q_grp, x="States",y=["RegisteredUsers", "AppOpens"],
                            title=f"{df["Years"].min()}YEAR {quarter}QUARTER REGISTERED USERS AND APPOPENS",width=1000, height=800, markers=True,
                            color_discrete_sequence=px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_line_1)

    return map_user_y_q

#map user plot 3
def map_user_plot_3(df,state):
    map_user_year_quarter=df[df["States"]== state]
    map_user_year_quarter.reset_index(drop=True,inplace= True)

    col1,col2=st.columns(2)
    with col1:

        fig_map_user_bar_1=px.bar(map_user_year_quarter,x="RegisteredUsers", y="Districts",
                                title="REGISTERED USER",height=800, width=600,color_discrete_sequence=px.colors.sequential.Cividis_r)
        st.plotly_chart(fig_map_user_bar_1)
    with col2:

        fig_map_user_bar_2=px.bar(map_user_year_quarter,x="AppOpens", y="Districts",
                                title="APPOPENS",height=800,width=600,color_discrete_sequence=px.colors.sequential.Cividis)
        st.plotly_chart(fig_map_user_bar_2)

#Top Insurace plot 1
def top_plt_1(df,state):
    top_insur_y_q=df[df["States"]== state]
    top_insur_y_q.reset_index(drop=True,inplace= True)
    col1,col2=st.columns(2)
    with col1:

        fig_top_insur_bar_1=px.bar(top_insur_y_q,x="Quarter", y="Transaction_amount", hover_data="Pincodes",
                                    title=f"{state.upper()} TRANSACTION AMOUNT",height=800,width=600,color_discrete_sequence=px.colors.sequential.haline)
        st.plotly_chart(fig_top_insur_bar_1)
    with col2:

        fig_top_insur_bar_2=px.bar(top_insur_y_q,x="Quarter", y="Transaction_count", hover_data="Pincodes",
                                    title=f"{state.upper()} TRANSACTION COUNT",height=800,width=600,color_discrete_sequence=px.colors.sequential.Agsunset)
        st.plotly_chart(fig_top_insur_bar_2)

#Top user plot 1
def top_user_plt_1(df,year):
    top_user_year=df[df["Years"]== year]
    top_user_year.reset_index(drop=True,inplace= True)

    top_user_year_grp=pd.DataFrame(top_user_year.groupby(["States","Quarter"])["RegisteredUsers"].sum())
    top_user_year_grp.reset_index(inplace= True)

    fig_top_user_1=px.bar(top_user_year_grp,x="States",y="RegisteredUsers", color="Quarter",width=1000,height=800, 
                          color_discrete_sequence=px.colors.sequential.Agsunset_r,hover_name="States",title=f"{year} REGISTERED USERS")
    st.plotly_chart(fig_top_user_1)

    return top_user_year

#Top user plot 2
def top_user_plt_2(df,state):
    top_user_year_state=df[df["States"]== state]
    top_user_year_state.reset_index(drop=True,inplace= True)


    fig_top_user_2=px.bar(top_user_year_state, x="Quarter", y="RegisteredUsers", title="REGISTERED USERS, PINCODEs, QUARTER",
                        width= 1000, height= 800,color="RegisteredUsers", hover_data="Pincodes", color_continuous_scale=px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_user_2)

# Establish MySQL connection
def top_chart_transaction_type(table_name):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe"
    )


    cursor = mydb.cursor()

    #plot 1
    query1 = f'''
        SELECT Transaction_type, SUM(Transaction_amount) as Transaction_amount
        FROM {table_name}
        GROUP BY Transaction_type
        ORDER BY Transaction_amount
    '''

    cursor.execute(query1)
    table_1 = cursor.fetchall()
    mydb.commit()
    df_1 = pd.DataFrame(table_1,columns=("Transaction_type", "Transaction_amount"))

    col1,col2=st.columns(2)
    with col1:
        fig_amount = px.pie(df_1, names="Transaction_type", values="Transaction_amount", title="TRANSACTION AMOUNT",
                            height=650,width=550)
        st.plotly_chart(fig_amount)

    #plot2
    query2 = f'''
                SELECT Transaction_type, SUM(Transaction_count) as Transaction_count
                FROM {table_name}
                GROUP BY Transaction_type
                ORDER BY Transaction_count
                '''

    cursor.execute(query2)
    table_2 = cursor.fetchall()
    mydb.commit()
    df_2 = pd.DataFrame(table_2,columns=("Transaction_type", "Transaction_count"))

    with col2:
        fig_count = px.pie(df_2, names="Transaction_type", values="Transaction_count", title="TRANSACTION COUNT",
                        height=650,width=550)
        st.plotly_chart(fig_count)


def top_chart_transaction_amount(table_name):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe"
    )


    cursor = mydb.cursor()

    #plot 1
    query1 = f'''
        SELECT States, SUM(Transaction_amount) AS transaction_amount
        FROM {table_name}
        GROUP BY States
        ORDER BY Transaction_amount DESC
        LIMIT 10;
    '''

    cursor.execute(query1)
    table_1 = cursor.fetchall()
    mydb.commit()
    df_1 = pd.DataFrame(table_1,columns=("States", "Transaction_amount"))

    col1,col2=st.columns(2)
    with col1:

        fig_amount=px.bar(df_1,x="States", y="Transaction_amount", title= "TOP 10 OF TRANSACTION AMOUNT", hover_name="States",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    #plot2
    query2 = f'''
        SELECT States, SUM(Transaction_amount) AS transaction_amount
        FROM {table_name}
        GROUP BY States
        ORDER BY Transaction_amount
        LIMIT 10;
    '''

    cursor.execute(query2)
    table_2 = cursor.fetchall()
    mydb.commit()
    df_2 = pd.DataFrame(table_2,columns=("States", "Transaction_amount"))
    
    with col2:

        fig_amount_2=px.bar(df_2,x="States", y="Transaction_amount", title= "LAST 10 OF TRANSACTION AMOUNT", hover_name="States",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)


    #plot3
    query3 = f'''
        SELECT States, avg(Transaction_amount) AS transaction_amount
        FROM {table_name}
        GROUP BY States
        ORDER BY Transaction_amount;
    '''


    cursor.execute(query3)
    table_3 = cursor.fetchall()
    mydb.commit()
    df_3 = pd.DataFrame(table_3,columns=("States", "Transaction_amount"))


    fig_amount_3=px.bar(df_3,x="Transaction_amount", y="States", title= "AVERAGE OF TRANSACTION AMOUNT", hover_name="States", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)

# Establish MySQL connection
def top_chart_transaction_count(table_name):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe"
    )


    cursor = mydb.cursor()

    #plot 1
    query1 = f'''
        SELECT States, SUM(Transaction_count) AS Transaction_count
        FROM {table_name}
        GROUP BY States
        ORDER BY Transaction_count DESC
        LIMIT 10;
    '''

    cursor.execute(query1)
    table_1 = cursor.fetchall()
    mydb.commit()
    df_1 = pd.DataFrame(table_1,columns=("States", "Transaction_count"))

    col1,col2=st.columns(2)
    with col1:

        fig_amount=px.bar(df_1,x="States", y="Transaction_count", title= "TOP 10 OF TRANSACTION COUNT", hover_name="States",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    #plot2
    query2 = f'''
        SELECT States, SUM(Transaction_count) AS Transaction_count
        FROM {table_name}
        GROUP BY States
        ORDER BY Transaction_count
        LIMIT 10;
    '''

    cursor.execute(query2)
    table_2 = cursor.fetchall()
    mydb.commit()
    df_2 = pd.DataFrame(table_2,columns=("States", "Transaction_count"))

    with col2:

        fig_amount_2=px.bar(df_2,x="States", y="Transaction_count", title= "LAST 10 OF TRANSACTION COUNT", hover_name="States",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)


    #plot3
    query3 = f'''
        SELECT States, avg(Transaction_count) AS Transaction_count
        FROM {table_name}
        GROUP BY States
        ORDER BY Transaction_count;
    '''


    cursor.execute(query3)
    table_3 = cursor.fetchall()
    mydb.commit()
    df_3 = pd.DataFrame(table_3,columns=("States", "Transaction_count"))


    fig_amount_3=px.bar(df_3,x="Transaction_count", y="States", title= "AVERAGE OF TRANSACTION COUNT", hover_name="States", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)


def top_chart_user_brandwise(table_name):

    mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe"
)

    cursor = mydb.cursor()

    #plot1
    query1=f'''SELECT Brands, SUM(Transaction_count) AS Transaction_count 
                    FROM {table_name} 
                    GROUP BY Brands
                    ORDER BY Transaction_count DESC 
                    LIMIT 10;'''
    cursor.execute(query1)
    table_0=cursor.fetchall()
    mydb.commit()

    Agg_user_df=pd.DataFrame(table_0,columns=("Brands", "Transaction_count"))
    
    col1,col2=st.columns(2)
    with col1:
        fig_amout_AU=px.bar(Agg_user_df, x="Transaction_count", y="Brands", title="TOP 10 BRANDWISE COUNT",orientation="h",
                                    color_discrete_sequence=px.colors.sequential.BuPu_r , height=650, width=600)
        st.plotly_chart(fig_amout_AU)

    #plot2
    
    Query_2=f'''SELECT Brands, AVG(Transaction_count) AS Transaction_count 
                    FROM {table_name} 
                    GROUP BY Brands
                    ORDER BY Transaction_count'''
    cursor.execute(Query_2)
    table_1=cursor.fetchall()
    mydb.commit()

    Agg_user_df1=pd.DataFrame(table_1,columns=("Brands","Transaction_count"))

    with col2:
        fig_amout_AU1=px.bar(Agg_user_df1, y="Transaction_count", x="Brands", title="AVERAGE OF BRANDWISE COUNT",
                                color_discrete_sequence=px.colors.sequential.Peach_r , height=650, width=600)
        st.plotly_chart(fig_amout_AU1)



    #plot3
    Query_3=f'''SELECT Brands, SUM(Percentage) AS Percentage
            FROM {table_name}
            GROUP BY Brands
            ORDER BY Percentage DESC
            LIMIT 10;'''
            
    cursor.execute(Query_3)
    table_2=cursor.fetchall()
    mydb.commit()

    Agg_user_df2=pd.DataFrame(table_2,columns=("Brands","Percentage"))

    col1,col2=st.columns(2)
    with col1:
        fig_percent_1=px.pie(data_frame=Agg_user_df2, names='Brands',values="Percentage", title="TOP 10 BRANDWISE PERCENTAGE",
                        height=650, width=600, hole=0.5)
        st.plotly_chart(fig_percent_1)


    #plot4
    query_4=f'''SELECT Brands, AVG(Percentage) AS Percentage
            FROM {table_name}
            GROUP BY Brands
            ORDER BY Percentage'''
            
    cursor.execute(query_4)
    table_3=cursor.fetchall()
    mydb.commit()

    Agg_user_df3=pd.DataFrame(table_3,columns=("Brands","Percentage"))

    with col2:
        fig_percent_2=px.pie(data_frame=Agg_user_df3, names='Brands',values="Percentage", title="AVERAGE OF BRANDWISE PERCENTAGE",
                        height=650, width=600, hole=0.5)
        st.plotly_chart(fig_percent_2)


def top_chart_registered_users(table_name, state):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe"
    )

    cursor = mydb.cursor()

    # Plot 1
    query1 = f'''
        SELECT Districts, SUM(RegisteredUsers) AS RegisteredUsers
        FROM {table_name}
        WHERE States='{state}'
        GROUP BY Districts
        ORDER BY RegisteredUsers DESC
        LIMIT 10;
    '''

    cursor.execute(query1)
    table_1 = cursor.fetchall()
    mydb.commit()
    df_1 = pd.DataFrame(table_1, columns=("Districts", "RegisteredUsers"))

    col1,col2=st.columns(2)
    with col1:
        fig_amount = px.bar(df_1, x="Districts", y="RegisteredUsers", title="TOP 10 OF REGISTERED USERS", hover_name="Districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # Plot 2
    query2 = f'''
        SELECT Districts, SUM(RegisteredUsers) AS RegisteredUsers
        FROM {table_name}
        WHERE States='{state}'
        GROUP BY Districts
        ORDER BY RegisteredUsers
        LIMIT 10;
    '''

    cursor.execute(query2)
    table_2 = cursor.fetchall()
    mydb.commit()
    df_2 = pd.DataFrame(table_2, columns=("Districts", "RegisteredUsers"))

    with col2:
        fig_amount_2 = px.bar(df_2, x="Districts", y="RegisteredUsers", title="LAST 10 OF REGISTERED USERS", hover_name="Districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)

    # Plot 3
    query3 = f'''
        SELECT Districts, AVG(RegisteredUsers) AS RegisteredUsers
        FROM {table_name}
        WHERE States='{state}'
        GROUP BY Districts
        ORDER BY RegisteredUsers;
    '''

    cursor.execute(query3)
    table_3 = cursor.fetchall()
    mydb.commit()
    df_3 = pd.DataFrame(table_3, columns=("Districts", "RegisteredUsers"))

    fig_amount_3 = px.bar(df_3, x="RegisteredUsers", y="Districts", title="AVERAGE OF REGISTERED USERS", hover_name="Districts", orientation="h",
                          color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)


def top_chart_app_opens(table_name, state):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe"
    )

    cursor = mydb.cursor()

    # Plot 1
    query1 = f'''
        SELECT Districts, SUM(AppOpens) AS AppOpens
        FROM {table_name}
        WHERE States='{state}'
        GROUP BY Districts
        ORDER BY AppOpens DESC
        LIMIT 10;
    '''

    cursor.execute(query1)
    table_1 = cursor.fetchall()
    mydb.commit()
    df_1 = pd.DataFrame(table_1, columns=("Districts", "AppOpens"))

    col1,col2=st.columns(2)
    with col1:
        fig_amount = px.bar(df_1, x="Districts", y="AppOpens", title="TOP 10 OF APP OPENS", hover_name="Districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # Plot 2
    query2 = f'''
        SELECT Districts, SUM(AppOpens) AS AppOpens
        FROM {table_name}
        WHERE States='{state}'
        GROUP BY Districts
        ORDER BY AppOpens
        LIMIT 10;
    '''

    cursor.execute(query2)
    table_2 = cursor.fetchall()
    mydb.commit()
    df_2 = pd.DataFrame(table_2, columns=("Districts", "AppOpens"))

    with col2:
        fig_amount_2 = px.bar(df_2, x="Districts", y="AppOpens", title="LAST 10 OF APP OPENS", hover_name="Districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)

    # Plot 3
    query3 = f'''
        SELECT Districts, AVG(AppOpens) AS AppOpens
        FROM {table_name}
        WHERE States='{state}'
        GROUP BY Districts
        ORDER BY AppOpens;
    '''

    cursor.execute(query3)
    table_3 = cursor.fetchall()
    mydb.commit()
    df_3 = pd.DataFrame(table_3, columns=("Districts", "AppOpens"))

    fig_amount_3 = px.bar(df_3, x="AppOpens", y="Districts", title="AVERAGE OF APP OPENS", hover_name="Districts", orientation="h",
                          color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)


def top_chart_top_registered_users(table_name):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe"
    )

    cursor = mydb.cursor()

    # Plot 1
    query1 = f'''
        SELECT States, SUM(RegisteredUsers) AS RegisteredUsers 
        FROM {table_name}
        GROUP BY States
        ORDER BY RegisteredUsers DESC
        LIMIT 10;
    '''

    cursor.execute(query1)
    table_1 = cursor.fetchall()
    mydb.commit()
    df_1 = pd.DataFrame(table_1, columns=("States", "RegisteredUsers"))

    col1,col2=st.columns(2)
    with col1:
        fig_amount = px.bar(df_1, x="States", y="RegisteredUsers", title="TOP 10 OF REGISTERED USERS", hover_name="States",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # Plot 2
    query2 = f'''
        SELECT States, SUM(RegisteredUsers) AS RegisteredUsers 
        FROM {table_name}
        GROUP BY States
        ORDER BY RegisteredUsers 
        LIMIT 10;
    '''

    cursor.execute(query2)
    table_2 = cursor.fetchall()
    mydb.commit()
    df_2 = pd.DataFrame(table_2, columns=("States", "RegisteredUsers"))

    with col2:
        fig_amount_2 = px.bar(df_2, x="States", y="RegisteredUsers", title="LAST 10 OF REGISTERED USERS", hover_name="States",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)

    # Plot 3
    query3 = f'''
        SELECT States, AVG(RegisteredUsers) AS RegisteredUsers 
        FROM {table_name}
        GROUP BY States
        ORDER BY RegisteredUsers;
    '''

    cursor.execute(query3)
    table_3 = cursor.fetchall()
    mydb.commit()
    df_3 = pd.DataFrame(table_3, columns=("States", "RegisteredUsers"))

    fig_amount_3 = px.bar(df_3, x="RegisteredUsers", y="States", title="AVERAGE OF REGISTERED USERS", hover_name="States", orientation="h",
                          color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)


#Streamlit Part

st.set_page_config(layout= "wide")

st.sidebar.header(":rainbow[**Welcome to the dashboard!**]")
st.title(":violet[PHONEPE PULSE DATA VISUALIZATION]")


with st.sidebar:

    select=option_menu("Main Menu",["HOME","DATA EXPLORATION","TOP CHARTS"])

if select == "HOME":
    
    col1,col2= st.columns(2)

    with col1:
        st.header("PHONEPE")
        st.subheader("INDIA'S BEST TRANSACTION APP")
        st.markdown("PhonePe  is an Indian digital payments and financial technology company")
        st.write("****FEATURES****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")
        st.markdown("[DOWNLOAD THE APP NOW](https://www.phonepe.com/app-download/)")
    with col2:
        st.video("D:\Phonepe\Phone Pe Ad.mp4")

    col3,col4= st.columns(2)
    
    with col3:
        st.image(Image.open(r"d:\Phonepe\phonepe1.jpg"),width=500)

    with col4:
        st.write("****Easy Transactions****")
        st.write("****One App For All Your Payments****")
        st.write("****Your Bank Account Is All You Need****")
        st.write("****Multiple Payment Modes****")
        st.write("****PhonePe Merchants****")
        st.write("****Multiple Ways To Pay****")
        st.write("****1.Direct Transfer & More****")
        st.write("****2.QR Code****")
        st.write("****Earn Great Rewards****")

    col5,col6= st.columns(2)

    with col5:
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.write("****No Wallet Top-Up Required****")
        st.write("****Pay Directly From Any Bank To Any Bank A/C****")
        st.write("****Instantly & Free****")

    with col6:
        st.video("D:\Phonepe\PhonePe Motion Graphics.mp4")

elif select == "DATA EXPLORATION":

    tab1, tab2, tab3 = st.tabs(["Aggregated Analysis", "Map Analysis", "Top Analysis"])

    with tab1:

       aggre_method = st.radio("Select The Method",["Aggregated Insurance Analysis", "Aggregated Transaction Analysis", "Aggregated User Analysis"])
       
       if aggre_method == "Aggregated Insurance Analysis":
            
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year for Aggregated Insurance Analysis", aggregate_insurance["Years"].min(), aggregate_insurance["Years"].max(), aggregate_insurance["Years"].min())
            Aggre_insur_tac_Y= Transaction_amount_count_Y(aggregate_insurance, years)
            
            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Aggregated Insurance Analysis", Aggre_insur_tac_Y["Quarter"].min(), Aggre_insur_tac_Y["Quarter"].max(), Aggre_insur_tac_Y["Quarter"].min())
            Transaction_amount_count_Q(Aggre_insur_tac_Y, quarters)
    
       elif aggre_method == "Aggregated Transaction Analysis":
            col1,col2=st.columns(2)
            with col1:
               
                years=st.slider("Select The Year for Aggregated Transaction Analysis", aggregate_transactions["Years"].min(), aggregate_transactions["Years"].max(), aggregate_transactions["Years"].min())
            Aggre_trans_tac_Y= Transaction_amount_count_Y(aggregate_transactions, years)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Aggregated Transaction Analysis", Aggre_trans_tac_Y["States"].unique())

            Aggre_trans_type(Aggre_trans_tac_Y, states)

            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Aggregated Transaction Analysis", Aggre_trans_tac_Y["Quarter"].min(), Aggre_trans_tac_Y["Quarter"].max(), Aggre_trans_tac_Y["Quarter"].min())
            Aggre_trans_tac_Y_Q=Transaction_amount_count_Q(Aggre_trans_tac_Y, quarters)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Aggregated Transaction Analysis for a Quarter", Aggre_trans_tac_Y_Q["States"].unique())

            Aggre_trans_type(Aggre_trans_tac_Y_Q, states)
       
       elif aggre_method == "Aggregated User Analysis":
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year for Aggregated User Analysis", aggregate_users["Years"].min(), aggregate_users["Years"].max(), aggregate_users["Years"].min())
            Aggre_user_Y= Aggre_user_plot_1(aggregate_users,years)

            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Aggregated User Analysis", Aggre_user_Y["Quarter"].min(), Aggre_user_Y["Quarter"].max(), Aggre_user_Y["Quarter"].min())
            Aggre_user_y_q=Aggre_user_plot_2(Aggre_user_Y, quarters)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Aggregated User Analysis", Aggre_user_y_q["States"].unique())
                Aggre_user_plot_3(Aggre_user_y_q,states)



    with tab2:

        map_method = st.radio("Select The Method",["Map Insurance Analysis", "Map Transaction Analysis", "Map User Analysis"])

        if map_method == "Map Insurance Analysis":
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year Map Insurance Analysis", map_insurance["Years"].min(), map_insurance["Years"].max(), map_insurance["Years"].min())
            map_insur_tac_Y= Transaction_amount_count_Y(map_insurance, years)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Map Insurance Analysis", map_insur_tac_Y["States"].unique())

            map_insur_district=map_dist(map_insur_tac_Y, states)

            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Map Insurance Analysis", map_insur_tac_Y["Quarter"].min(), map_insur_tac_Y["Quarter"].max(), map_insur_tac_Y["Quarter"].min())
            map_insur_tac_Y_Q=Transaction_amount_count_Q(map_insur_tac_Y, quarters)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Map Insurance", map_insur_tac_Y_Q["States"].unique())

            map_dist(map_insur_tac_Y_Q, states)

        elif map_method == "Map Transaction Analysis":
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year for Map Transaction Analysis", map_transactions["Years"].min(), map_transactions["Years"].max(), map_transactions["Years"].min())
            map_trans_tac_Y= Transaction_amount_count_Y(map_transactions, years)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Map Transaction Analysis", map_trans_tac_Y["States"].unique())

            map_trans_district=map_dist(map_trans_tac_Y, states)

            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Map Transaction Analysis", map_trans_tac_Y["Quarter"].min(), map_trans_tac_Y["Quarter"].max(), map_trans_tac_Y["Quarter"].min())
            map_trans_tac_Y_Q=Transaction_amount_count_Q(map_trans_tac_Y, quarters)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Map Transaction for a quarter", map_trans_tac_Y_Q["States"].unique())

            map_dist(map_trans_tac_Y_Q, states)


        elif map_method == "Map User Analysis":
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year for Map User Analysis", map_users["Years"].min(), map_users["Years"].max(), map_users["Years"].min())
            map_user_Y= map_user_plot_1(map_users, years)

            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Map User Analysis", map_user_Y["Quarter"].min(), map_user_Y["Quarter"].max(), map_user_Y["Quarter"].min())
            map_user_Y_Q=map_user_plot_2(map_user_Y, quarters)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Map User Analysis", map_user_Y_Q["States"].unique())
            map_user_plot_3(map_user_Y_Q, states)



    with tab3:

        top_method = st.radio("Select The Method",["Top Insurance Analysis", "Top Transaction Analysis", "Top User Analysis"])

        if top_method == "Top Insurance Analysis":
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year for Top Insurance Analysis", top_insurance["Years"].min(), top_insurance["Years"].max(), top_insurance["Years"].min())
            top_insur_tac_Y= Transaction_amount_count_Y(top_insurance, years)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Top Insurance Analysis", top_insur_tac_Y["States"].unique())
            top_plt_1(top_insur_tac_Y, states)

            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Top Insurance Analysis", top_insur_tac_Y["Quarter"].min(), top_insur_tac_Y["Quarter"].max(), top_insur_tac_Y["Quarter"].min())
            top_insur_Y_Q=Transaction_amount_count_Q(top_insur_tac_Y, quarters)
        
        elif top_method == "Top Transaction Analysis":
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year for Top Transaction Analysis", top_transactions["Years"].min(), top_transactions["Years"].max(), top_transactions["Years"].min())
            top_trans_tac_Y= Transaction_amount_count_Y(top_transactions, years)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Top Transaction Analysis", top_trans_tac_Y["States"].unique())
            top_plt_1(top_trans_tac_Y, states)

            col1,col2=st.columns(2)
            with col1:
                
                quarters=st.slider("Select The Quarter for Top Transaction Analysis", top_trans_tac_Y["Quarter"].min(), top_trans_tac_Y["Quarter"].max(), top_trans_tac_Y["Quarter"].min())
            top_trans_Y_Q=Transaction_amount_count_Q(top_trans_tac_Y, quarters)


        elif top_method == "Top User Analysis":
            col1,col2=st.columns(2)
            with col1:
                
                years=st.slider("Select The Year for Top User Analysis", top_users["Years"].min(), top_users["Years"].max(), top_users["Years"].min())
            top_user_Y= top_user_plt_1(top_users, years)

            col1,col2=st.columns(2)
            with col1:
                
                states=st.selectbox("Select the State for Top User Analysis", top_user_Y["States"].unique())
            top_user_plt_2(top_user_Y, states)


elif select == "TOP CHARTS":
    
    question= st.selectbox("Select the Question",["1. Transaction type wise analysis of Tranasaction amount and count of Aggregated Users",
                                                "2. Transaction Amount and Count of Aggregated Transaction",
                                                "3. Transaction Amount and Count of Map Transaction",
                                                "4. Brandwise Top 10 and average of Transaction Count and Percentage of Aggrecated User",
                                                "5. Transaction Count of Aggregated User",
                                                "6. Registered users of Map User",
                                                "7. App opens of Map User",
                                                "8. Registered users of Top User"])
    
    if question == "1. Transaction type wise analysis of Tranasaction amount and count of Aggregated Users":
        st.subheader("TRANSACTION TYPE")
        top_chart_transaction_type("aggregated_transactions")

    elif question == "2. Transaction Amount and Count of Aggregated Transaction":
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("aggregated_transactions")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_transactions")

    elif question == "3. Transaction Amount and Count of Map Transaction":
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_transactions")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("map_transactions")

    elif question == "4. Brandwise Top 10 and average of Transaction Count and Percentage of Aggrecated User":
        st.subheader("Brandwise Analysis")
        top_chart_user_brandwise("Aggregated_users")

    elif question == "5. Transaction Count of Aggregated User":
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_users")

    elif question == "6. Registered users of Map User":

        states=st.selectbox("Select the state", map_users["States"].unique())
        st.subheader("REGISTERED USERS")
        top_chart_registered_users("map_users",states)

    elif question == "7. App opens of Map User":

        states=st.selectbox("Select the state", map_users["States"].unique())
        st.subheader("REGISTERED USERS")
        top_chart_app_opens("map_users",states)

    elif question == "8. Registered users of Top User":

        st.subheader("REGISTERED USERS")
        top_chart_top_registered_users("top_users")

        