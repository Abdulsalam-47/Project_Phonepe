#Packages
import os
import json
import pandas as pd
import mysql.connector

#sql connection
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe"
)

cursor = mydb.cursor()

#aggre_insurance

path_agg_insur="D:/Phonepe/pulse/data/aggregated/insurance/country/india/state/"
agg_insur_list=os.listdir(path_agg_insur)

agg_insur_column={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}
for state in agg_insur_list:
    cur_states=path_agg_insur+state+"/"
    agg_year_list=os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            A=json.load(data)
            for i in A["data"]["transactionData"]:
                name=i["name"]
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                agg_insur_column["Transaction_type"].append(name)
                agg_insur_column["Transaction_count"].append(count)
                agg_insur_column["Transaction_amount"].append(amount)
                agg_insur_column["States"].append(state)
                agg_insur_column["Years"].append(year)
                agg_insur_column["Quarter"].append(int(file.strip(".json")))

aggre_insurance=pd.DataFrame(agg_insur_column)

aggre_insurance["States"]=aggre_insurance["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
aggre_insurance["States"]=aggre_insurance["States"].str.replace("-"," ")
aggre_insurance["States"]=aggre_insurance["States"].str.title()
aggre_insurance["States"]=aggre_insurance["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')

#aggre_insurance Table Creation

#aggregated_insurance
create_query1=create_query1 = '''
                                CREATE TABLE IF NOT EXISTS aggregated_insurance(
                                    States VARCHAR(255),
                                    Years INT,
                                    Quarter INT,
                                    Transaction_type VARCHAR(255),
                                    Transaction_count BIGINT,
                                    Transaction_amount BIGINT,
                                    PRIMARY KEY (States, Years, Quarter, Transaction_type)
                                )
                                '''

cursor.execute(create_query1)
mydb.commit()

insert_query1='''INSERT IGNORE INTO aggregated_insurance(States, Years, Quarter, Transaction_type, 
                                                    Transaction_count,
                                                    Transaction_amount)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
data=aggre_insurance.values.tolist()
cursor.executemany(insert_query1,data)
mydb.commit()

#aggre_transaction

path_agg_transaction="D:/Phonepe/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list=os.listdir(path_agg_transaction)

agg_tran_column={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}
for state in agg_tran_list:
    cur_states=path_agg_transaction+state+"/"
    agg_year_list=os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            B=json.load(data)
            for i in B["data"]["transactionData"]:
                name=i["name"]
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                agg_tran_column["Transaction_type"].append(name)
                agg_tran_column["Transaction_count"].append(count)
                agg_tran_column["Transaction_amount"].append(amount)
                agg_tran_column["States"].append(state)
                agg_tran_column["Years"].append(year)
                agg_tran_column["Quarter"].append(int(file.strip(".json")))

aggre_transaction=pd.DataFrame(agg_tran_column)

aggre_transaction["States"]=aggre_transaction["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
aggre_transaction["States"]=aggre_transaction["States"].str.replace("-"," ")
aggre_transaction["States"]=aggre_transaction["States"].str.title()
aggre_transaction["States"]=aggre_transaction["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#aggregated_transaction Table Creation

#aggregated_transaction
create_query2='''CREATE TABLE IF NOT EXISTS aggregated_transactions(States VARCHAR(255),
                                                                    Years INT,
                                                                    Quarter INT,
                                                                    Transaction_type VARCHAR(255),
                                                                    Transaction_count BIGINT,
                                                                    Transaction_amount BIGINT,
                                                                    UNIQUE KEY unique_transaction (States, Years, Quarter, Transaction_type)
                                                                    )
                                                                    '''

cursor.execute(create_query2)
mydb.commit()

insert_query2='''INSERT IGNORE INTO aggregated_transactions(States, Years, Quarter, Transaction_type, 
                                                    Transaction_count,
                                                    Transaction_amount)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
data1=aggre_transaction.values.tolist()
cursor.executemany(insert_query2,data1)
mydb.commit()

#aggre_user
path_agg_user="D:/Phonepe/pulse/data/aggregated/user/country/india/state/"
agg_user_list=os.listdir(path_agg_user)

agg_user_column={"States":[], "Years":[], "Quarter":[], "Brands":[], "Transaction_count":[], "Percentage":[]}
for state in agg_user_list:
    cur_states=path_agg_user+state+"/"
    agg_year_list=os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list=os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            C=json.load(data)
            
            try:
                for i in C["data"]["usersByDevice"]:
                    brand=i["brand"]
                    count=i["count"]
                    percentage=i["percentage"]
                    agg_user_column["Brands"].append(brand)
                    agg_user_column["Transaction_count"].append(count)
                    agg_user_column["Percentage"].append(percentage)
                    agg_user_column["States"].append(state)
                    agg_user_column["Years"].append(year)
                    agg_user_column["Quarter"].append(int(file.strip(".json")))

            except:
                pass

aggre_user=pd.DataFrame(agg_user_column)

aggre_user["States"]=aggre_user["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
aggre_user["States"]=aggre_user["States"].str.replace("-"," ")
aggre_user["States"]=aggre_user["States"].str.title()
aggre_user["States"]=aggre_user["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#aggregated_user Table Creation

#aggregated_users
create_query3='''CREATE TABLE IF NOT EXISTS aggregated_users(States VARCHAR(255),
                                                    Years INT,
                                                    Quarter INT,
                                                    Brands VARCHAR(255),
                                                    Transaction_count BIGINT,
                                                    Percentage FLOAT,
                                                     UNIQUE KEY `unique_combination` (States, Years, Quarter, Brands)
                                                    )
                                                    '''

cursor.execute(create_query3)
mydb.commit()

insert_query3='''INSERT IGNORE INTO aggregated_users(States, Years, Quarter, Brands, 
                                                    Transaction_count,
                                                    Percentage)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
data2=aggre_user.values.tolist()
cursor.executemany(insert_query3,data2)
mydb.commit()

#Map insurane

path_map_insur="D:/Phonepe/pulse/data/map/insurance/hover/country/india/state/"
map_insur_list=os.listdir(path_map_insur)

map_insur_column={"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}
for state in map_insur_list:
    cur_states=path_map_insur+state+"/"
    map_year_list=os.listdir(cur_states)
    
    for year in map_year_list:
        cur_year=cur_states+year+"/"
        map_file_list=os.listdir(cur_year)
        
        for file in map_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            D=json.load(data)

            for i in D["data"]["hoverDataList"]:
                    name=i["name"]
                    count=i["metric"][0]["count"]
                    amount=i["metric"][0]["amount"]
                    map_insur_column["Districts"].append(name)
                    map_insur_column["Transaction_count"].append(count)
                    map_insur_column["Transaction_amount"].append(amount)
                    map_insur_column["States"].append(state)
                    map_insur_column["Years"].append(year)
                    map_insur_column["Quarter"].append(int(file.strip(".json")))

map_insurance=pd.DataFrame(map_insur_column)

map_insurance["States"]=map_insurance["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
map_insurance["States"]=map_insurance["States"].str.replace("-"," ")
map_insurance["States"]=map_insurance["States"].str.title()
map_insurance["States"]=map_insurance["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#map_insurance Table Creation

#map_insurance
create_query4='''CREATE TABLE IF NOT EXISTS map_insurance(States VARCHAR(255),
                                                    Years INT,
                                                    Quarter INT,
                                                    Districts VARCHAR(255),
                                                    Transaction_count BIGINT,
                                                    Transaction_amount BIGINT,
                                                    UNIQUE KEY `unique_combination` (States, Years, Quarter, Districts)
                                                    )
                                                    '''

cursor.execute(create_query4)
mydb.commit()

insert_query4='''INSERT IGNORE INTO map_insurance(States, Years, Quarter, Districts, 
                                                    Transaction_count,
                                                    Transaction_amount)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
data3=map_insurance.values.tolist()
cursor.executemany(insert_query4,data3)
mydb.commit()

#Map transaction

path_map_tran="D:/Phonepe/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list=os.listdir(path_map_tran)

map_tran_column={"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}
for state in map_tran_list:
    cur_states=path_map_tran+state+"/"
    map_year_list=os.listdir(cur_states)
    
    for year in map_year_list:
        cur_year=cur_states+year+"/"
        map_file_list=os.listdir(cur_year)
        
        for file in map_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            E=json.load(data)

            for i in E["data"]["hoverDataList"]:
                    name=i["name"]
                    count=i["metric"][0]["count"]
                    amount=i["metric"][0]["amount"]
                    map_tran_column["Districts"].append(name)
                    map_tran_column["Transaction_count"].append(count)
                    map_tran_column["Transaction_amount"].append(amount)
                    map_tran_column["States"].append(state)
                    map_tran_column["Years"].append(year)
                    map_tran_column["Quarter"].append(int(file.strip(".json")))

map_transaction=pd.DataFrame(map_tran_column)

map_transaction["States"]=map_transaction["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
map_transaction["States"]=map_transaction["States"].str.replace("-"," ")
map_transaction["States"]=map_transaction["States"].str.title()
map_transaction["States"]=map_transaction["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#map_transaction Table Creation

#map_transaction
create_query5='''CREATE TABLE IF NOT EXISTS map_transactions(States VARCHAR(255),
                                                    Years INT,
                                                    Quarter INT,
                                                    Districts VARCHAR(255),
                                                    Transaction_count BIGINT,
                                                    Transaction_amount BIGINT,
                                                    UNIQUE KEY `unique_combination` (States, Years, Quarter, Districts)
                                                    )
                                                    '''

cursor.execute(create_query5)
mydb.commit()

insert_query5='''INSERT IGNORE INTO map_transactions(States, Years, Quarter, Districts, 
                                                    Transaction_count,
                                                    Transaction_amount)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
data4=map_transaction.values.tolist()
cursor.executemany(insert_query5,data4)
mydb.commit()


#Map_user
path_map_user="D:/Phonepe/pulse/data/map/user/hover/country/india/state/"
map_user_list=os.listdir(path_map_user)

map_user_column={"States":[], "Years":[], "Quarter":[], "Districts":[], "registeredUsers":[], "appOpens":[]}
for state in map_user_list:
    cur_states=path_map_user+state+"/"
    map_year_list=os.listdir(cur_states)
    
    for year in map_year_list:
        cur_year=cur_states+year+"/"
        map_file_list=os.listdir(cur_year)
        
        for file in map_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            F=json.load(data)

            for i in F["data"]["hoverData"].items():
                district=i[0]
                registeredUsers=i[1]["registeredUsers"]
                appOpens=i[1]["appOpens"]
                map_user_column["Districts"].append(district)
                map_user_column["registeredUsers"].append(registeredUsers)
                map_user_column["appOpens"].append(appOpens)
                map_user_column["States"].append(state)
                map_user_column["Years"].append(year)
                map_user_column["Quarter"].append(int(file.strip(".json")))

map_user=pd.DataFrame(map_user_column)

map_user["States"]=map_user["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
map_user["States"]=map_user["States"].str.replace("-"," ")
map_user["States"]=map_user["States"].str.title()
map_user["States"]=map_user["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#map_user Table Creation

#map_user
create_query6='''CREATE TABLE IF NOT EXISTS map_users(States VARCHAR(255),
                                                    Years INT,
                                                    Quarter INT,
                                                    Districts VARCHAR(255),
                                                    RegisteredUsers BIGINT,
                                                    AppOpens BIGINT,
                                                    UNIQUE KEY `unique_combination` (States, Years, Quarter, Districts)
                                                    )
                                                    '''

cursor.execute(create_query6)
mydb.commit()

insert_query6='''INSERT IGNORE INTO map_users(States, Years, Quarter, Districts, 
                                                    RegisteredUsers,
                                                    AppOpens)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
data5=map_user.values.tolist()
cursor.executemany(insert_query6,data5)
mydb.commit()


#Top insurance

path_top_insur="D:/Phonepe/pulse/data/top/insurance/country/india/state/"
top_insur_list=os.listdir(path_top_insur)

top_insur_column={"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
for state in top_insur_list:
    cur_states=path_top_insur+state+"/"
    top_year_list=os.listdir(cur_states)
    
    for year in top_year_list:
        cur_year=cur_states+year+"/"
        top_file_list=os.listdir(cur_year)
        
        for file in top_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            G=json.load(data)

            for i in G["data"]["pincodes"]:
                    entityname=i["entityName"]
                    count=i["metric"]["count"]
                    amount=i["metric"]["amount"]
                    top_insur_column["Pincodes"].append(entityname)
                    top_insur_column["Transaction_count"].append(count)
                    top_insur_column["Transaction_amount"].append(amount)
                    top_insur_column["States"].append(state)
                    top_insur_column["Years"].append(year)
                    top_insur_column["Quarter"].append(int(file.strip(".json")))

top_insurance=pd.DataFrame(top_insur_column)

top_insurance["States"]=top_insurance["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
top_insurance["States"]=top_insurance["States"].str.replace("-"," ")
top_insurance["States"]=top_insurance["States"].str.title()
top_insurance["States"]=top_insurance["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#top_insurance Table Creation

#top_insurance
create_query7 = '''
                    CREATE TABLE IF NOT EXISTS top_insurance (
                        States VARCHAR(255),
                        Years INT,
                        Quarter INT,
                        Pincodes VARCHAR(255),
                        Transaction_count BIGINT,
                        Transaction_amount BIGINT,
                        UNIQUE KEY `unique_combination` (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                    )
                '''

cursor.execute(create_query7)
mydb.commit()

insert_query7 = '''
    INSERT INTO top_insurance (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    Transaction_count = VALUES(Transaction_count),
    Transaction_amount = VALUES(Transaction_amount)'''

data6=top_insurance.values.tolist()
cursor.executemany(insert_query7,data6)
mydb.commit()


#Top Transaction

path_top_trans="D:/Phonepe/pulse/data/top/transaction/country/india/state/"
top_trans_list=os.listdir(path_top_trans)

top_trans_column={"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
for state in top_trans_list:
    cur_states=path_top_trans+state+"/"
    top_year_list=os.listdir(cur_states)
    
    for year in top_year_list:
        cur_year=cur_states+year+"/"
        top_file_list=os.listdir(cur_year)
        
        for file in top_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            H=json.load(data)

            for i in H["data"]["pincodes"]:
                    entityname=i["entityName"]
                    count=i["metric"]["count"]
                    amount=i["metric"]["amount"]
                    top_trans_column["Pincodes"].append(entityname)
                    top_trans_column["Transaction_count"].append(count)
                    top_trans_column["Transaction_amount"].append(amount)
                    top_trans_column["States"].append(state)
                    top_trans_column["Years"].append(year)
                    top_trans_column["Quarter"].append(int(file.strip(".json")))

top_transaction=pd.DataFrame(top_trans_column)

top_transaction["States"]=top_transaction["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
top_transaction["States"]=top_transaction["States"].str.replace("-"," ")
top_transaction["States"]=top_transaction["States"].str.title()
top_transaction["States"]=top_transaction["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#top_transactions Table Creation

#top_transactions
create_query8 = '''
                    CREATE TABLE IF NOT EXISTS top_transactions (
                        States VARCHAR(255),
                        Years INT,
                        Quarter INT,
                        Pincodes VARCHAR(255),
                        Transaction_count BIGINT,
                        Transaction_amount BIGINT,
                        UNIQUE KEY `unique_combination` (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                    )
                '''

cursor.execute(create_query8)
mydb.commit()

insert_query8 = '''
    INSERT INTO top_transactions (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    Transaction_count = VALUES(Transaction_count),
    Transaction_amount = VALUES(Transaction_amount)'''

data7=top_transaction.values.tolist()
cursor.executemany(insert_query8,data7)
mydb.commit()


#Top user

path_top_user="D:/Phonepe/pulse/data/top/user/country/india/state/"
top_user_list=os.listdir(path_top_user)

top_user_column={"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUsers":[]}
for state in top_user_list:
    cur_states=path_top_user+state+"/"
    top_year_list=os.listdir(cur_states)
    
    for year in top_year_list:
        cur_year=cur_states+year+"/"
        top_file_list=os.listdir(cur_year)
        
        for file in top_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            I=json.load(data)

            for i in I["data"]["pincodes"]:
                    entityname=i["name"]
                    registeredusers=i["registeredUsers"]
                    top_user_column["Pincodes"].append(entityname)
                    top_user_column["RegisteredUsers"].append(registeredusers)
                    top_user_column["States"].append(state)
                    top_user_column["Years"].append(year)
                    top_user_column["Quarter"].append(int(file.strip(".json")))

top_user=pd.DataFrame(top_user_column)

top_user["States"]=top_user["States"].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
top_user["States"]=top_user["States"].str.replace("-"," ")
top_user["States"]=top_user["States"].str.title()
top_user["States"]=top_user["States"].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#top_users Table Creation

#top_users
create_query9 = '''
                    CREATE TABLE IF NOT EXISTS top_users(
                        States VARCHAR(255),
                        Years INT,
                        Quarter INT,
                        Pincodes VARCHAR(255),
                        RegisteredUsers BIGINT,
                        UNIQUE KEY `unique_combination` (States, Years, Quarter, Pincodes, RegisteredUsers)
                    )
                '''

cursor.execute(create_query9)
mydb.commit()

insert_query9 = '''
    INSERT IGNORE INTO top_users(States, Years, Quarter, Pincodes, RegisteredUsers)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    RegisteredUsers = VALUES(RegisteredUsers)
'''

data8=top_user.values.tolist()
cursor.executemany(insert_query9,data8)
mydb.commit()