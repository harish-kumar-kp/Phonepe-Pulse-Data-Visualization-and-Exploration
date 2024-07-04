import os
import git
import json
import pandas as pd
import mysql.connector 
import plotly.express as px
import streamlit as st
import webbrowser
# Variable Initilizaion
x=whereS=whereY=whereQ=whereB=0
insightText=dfCol2=chartType=colTitle =yearSelected=quarterSelected=queryYear =queryQuarter =transactionSelected=brandSelected=deviceDetailSelected=""
colVarX = "State"
def createDirectory(workpath):
    if not os.path.exists(workpath):
        os.makedirs(workpath)
    os.chdir(workpath)
    presentPath = os.getcwd()
    localPath = presentPath+"/Data"
    dataPresent=(os.listdir(presentPath))
    if "Data" in dataPresent:
        st.write(':red[Clone Already Exist at Local Path]')
        pass
    else:
        # Clone a remote repository 
        repo_url = "https://github.com/PhonePe/pulse.git"
        local_path = localPath
        repo = git.Repo.clone_from(repo_url, local_path) 
        st.write(f':red[Repository Cloned at location: {local_path}]') 
    return localPath


db = mysql.connector.connect(
host ="localhost",
user = "root",
passwd = "root",
database = "phonepeDB"
)
mycursor = db.cursor()
#mycursor.execute("CREATE DATABASE phonepeDB") 

# aggragated Insurance
#--------------------------
#agg_insur_StatesPath=destinPath+'/data/aggregated/insurance/country/india/state/'
def aggragateInsurance(agg_insur_StatesPath):   
    mycursor.execute("CREATE TABLE IF NOT EXISTS agg_insurance (State VARCHAR(255),Year VARCHAR(255), Quarter VARCHAR(255),Insurance_type VARCHAR(255),Insurance_count BIGINT ,Insurance_amount BIGINT)")
    agg_insurList = os.listdir(agg_insur_StatesPath)
    agg_insurColumn= {"States":[], "Years":[], "Quarter":[], "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[] }
    for state in agg_insurList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =agg_insur_StatesPath+state+"/"
        year_agg_insurList = os.listdir(statePath)
        for year in year_agg_insurList:
            yearPath =agg_insur_StatesPath+state+"/"+year+"/"
            data_agg_insurList = os.listdir(yearPath)
            for data in data_agg_insurList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                dataFile = open(file)
                # returns JSON object as a dictionary
                dataDict = json.load(dataFile)
                for i in dataDict['data']['transactionData']:
                    agg_insurColumn['States'].append(stateName)
                    agg_insurColumn['Years'].append(year)
                    agg_insurColumn['Quarter'].append(quarter)
                    insurType=i['paymentInstruments'][0]['type']
                    agg_insurColumn['Insurance_type'].append(insurType)
                    insurAmount =round(i['paymentInstruments'][0]['amount'],2)
                    agg_insurColumn['Insurance_amount'].append(insurAmount)
                    insurCount =int(i['paymentInstruments'][0]['count'])
                    agg_insurColumn['Insurance_count'].append(insurCount)
                    #print(stateName , year ,quarter ,insurType ,insurAmount ,insurCount)
                    agg_insurSql = "INSERT INTO agg_insurance (State,Year,Quarter,Insurance_type,Insurance_count,Insurance_amount) VALUES (%s, %s ,%s,%s, %s ,%s)"
                    agg_insurVal = (stateName , year ,quarter ,insurType ,insurAmount ,insurCount)
                    mycursor.execute(agg_insurSql , agg_insurVal)
                dataFile.close()
    #return agg_insurColumn

# aggragated transaction
#--------------------------
#agg_trans_StatesPath=destinPath+'/data/aggregated/transaction/country/india/state/'
def aggragateTransaction(agg_trans_StatesPath):
    mycursor.execute("CREATE TABLE IF NOT EXISTS agg_transaction (State VARCHAR(255),Year VARCHAR(255), Quarter VARCHAR(255),Transaction_type VARCHAR(255),Transaction_count BIGINT ,Transaction_amount BIGINT)")
    agg_transColumn={'State':[], 'Year':[],'Quarter':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
    agg_transList = os.listdir(agg_trans_StatesPath)
    for state in agg_transList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =agg_trans_StatesPath+state+"/"
        year_agg_transList = os.listdir(statePath)
        for year in year_agg_transList:
            yearPath =agg_trans_StatesPath+state+"/"+year+"/"
            data_agg_transList = os.listdir(yearPath)
            for data in data_agg_transList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                dataFile = open(file)
                # returns JSON object as a dictionary
                aggTranDataDict = json.load(dataFile)
                # Iterating through the json
                for i in aggTranDataDict['data']['transactionData']:
                    agg_transColumn['State'].append(stateName)
                    agg_transColumn['Year'].append(year)
                    agg_transColumn['Quarter'].append(quarter)
                    transType=i['name']
                    agg_transColumn['Transacion_type'].append(transType)
                    transAmount =round(i['paymentInstruments'][0]['amount'],2)
                    agg_transColumn['Transacion_amount'].append(transAmount)
                    transCount=int(i['paymentInstruments'][0]['count'])
                    agg_transColumn['Transacion_count'].append(transCount)
                    #print(stateName , year ,quarter ,transType ,transAmount ,transCount)
                    agg_transSql = "INSERT INTO agg_transaction (State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount) VALUES (%s, %s ,%s,%s, %s ,%s)"
                    agg_transVal = (stateName , year ,quarter ,transType ,transAmount ,transCount)
                    mycursor.execute(agg_transSql , agg_transVal)
                # Closing file
                dataFile.close()
    #return agg_transColumn

# aggragated User
#--------------------------
#agg_user_StatesPath=destinPath+'/data/aggregated/user/country/india/state/'
def aggragateUser(agg_user_StatesPath):   
    mycursor.execute("CREATE TABLE IF NOT EXISTS agg_user (State VARCHAR(255),Year VARCHAR(255), Quarter VARCHAR(255),Brands VARCHAR(255),User_count BIGINT ,Percentage FLOAT)")
    agg_userList = os.listdir(agg_user_StatesPath)
    agg_userColumn= {"States":[], "Years":[], "Quarter":[], "Brands":[],"User_count":[], "Percentage":[]}
    for state in agg_userList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =agg_user_StatesPath+state+"/"
        year_agg_userList = os.listdir(statePath)
        for year in year_agg_userList:
            yearPath =agg_user_StatesPath+state+"/"+year+"/"
            data_agg_userList = os.listdir(yearPath)
            for data in data_agg_userList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                userdataFile = open(file)
                # returns JSON object as a dictionary
                userDataDict = json.load(userdataFile)
                #print(userDataDict)
                try:    
                    for i in userDataDict['data']['usersByDevice']:
                        agg_userColumn['States'].append(stateName)
                        agg_userColumn['Years'].append(year)
                        agg_userColumn['Quarter'].append(quarter)
                        userBrand=i['brand']
                        agg_userColumn['Brands'].append(userBrand)
                        userCount =i['count']
                        agg_userColumn['User_count'].append(userCount)
                        userPercentage =round(i['percentage'],3)
                        agg_userColumn['Percentage'].append(userPercentage)
                        #print(stateName , year ,quarter ,userBrand ,userCount ,userPercentage) 
                        agg_userSql = "INSERT INTO agg_user (State,Year,Quarter,Brands,User_count,Percentage) VALUES (%s,%s,%s,%s,%s,%s)"
                        agg_userVal = (stateName , year ,quarter ,userBrand ,userCount ,userPercentage)
                        mycursor.execute(agg_userSql , agg_userVal)             
                except:
                    pass
                userdataFile.close()
    #return agg_userColumn 

# map Insurance
#--------------------------
#map_insur_StatesPath=destinPath+'/data/map/insurance/hover/country/india/state/'
def mapInsurance(map_insur_StatesPath):
    mycursor.execute("CREATE TABLE IF NOT EXISTS map_insurance (State VARCHAR(50),Year VARCHAR(10), Quarter VARCHAR(5),District VARCHAR(50) ,Insurance_type VARCHAR(50),Insurance_count BIGINT ,Insurance_amount BIGINT)")
    map_insurList = os.listdir(map_insur_StatesPath)
    map_insurColumn= {"States":[], "Years":[], "Quarter":[],"District":[] , "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[] }
    for state in map_insurList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =map_insur_StatesPath+state+"/"
        year_map_insurList = os.listdir(statePath)
        for year in year_map_insurList:
            yearPath = map_insur_StatesPath+state+"/"+year+"/"
            data_map_insurList = os.listdir(yearPath)
            for data in data_map_insurList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                dataFile = open(file)
                # returns JSON object as a dictionary
                mapInsurDataFile = json.load(dataFile)
                for i in mapInsurDataFile['data']['hoverDataList']:
                    map_insurColumn['States'].append(stateName)
                    map_insurColumn['Years'].append(year)
                    map_insurColumn['Quarter'].append(quarter)
                    mapInsDistrict = (i['name']).title().replace(" And "," and ")
                    map_insurColumn['District'].append(mapInsDistrict)
                    mapInsType = i['metric'][0]['type']
                    map_insurColumn['Insurance_type'].append(mapInsType)
                    mapInsCount = i['metric'][0]['count']
                    map_insurColumn['Insurance_count'].append(mapInsCount)
                    mapInsAmount = round(i['metric'][0]['amount'],2)
                    map_insurColumn['Insurance_amount'].append(mapInsAmount)
                    #print(stateName , year ,quarter , mapInsDistrict ,mapInsType,mapInsCount,mapInsAmount,sep=("|"))
                    map_insuSql = "INSERT INTO map_insurance (State,Year,Quarter,District,Insurance_type,Insurance_count,Insurance_amount) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    amap_insuVal = (stateName , year ,quarter ,mapInsDistrict ,mapInsType ,mapInsCount,mapInsAmount)
                    mycursor.execute(map_insuSql , amap_insuVal)
                dataFile.close()
    #return map_insurColumn

# map transaction
#--------------------------
#map_trans_StatesPath=destinPath+'/data/map/transaction/hover/country/india/state/'
def mapTransaction(map_trans_StatesPath):   
    mycursor.execute("CREATE TABLE IF NOT EXISTS map_transaction (State VARCHAR(50),Year VARCHAR(10), Quarter VARCHAR(5),District VARCHAR(50),Transaction_type VARCHAR(50),Transaction_count BIGINT ,Transaction_amount BIGINT)")
    map_transList = os.listdir(map_trans_StatesPath)
    map_transColumn= {"States":[], "Years":[], "Quarter":[],"District":[] ,'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
    for state in map_transList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =map_trans_StatesPath+state+"/"
        year_map_transList = os.listdir(statePath)
        for year in year_map_transList:
            yearPath =map_trans_StatesPath+state+"/"+year+"/"
            data_map_transList = os.listdir(yearPath)
            for data in data_map_transList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                transdataFile = open(file)
                # returns JSON object as a dictionary
                transMapDataDict = json.load(transdataFile)
                for i in transMapDataDict['data']['hoverDataList']:
                    mapDistrict=(i['name']).title().replace(" And "," and ")
                    if stateName== "Andaman and Nicobar Islands":
                        stateName ="Andaman & Nicobar"
                    map_transColumn['District'].append(mapDistrict)
                    mapCount=i['metric'][0]['count']
                    map_transColumn['Transacion_count'].append(mapCount)
                    mapType=i['metric'][0]['type']
                    map_transColumn['Transacion_type'].append(mapType)
                    mapAmount=round(i['metric'][0]['amount'],2)
                    map_transColumn['Transacion_amount'].append(mapAmount)
                    map_transColumn['States'].append(stateName)
                    map_transColumn['Years'].append(year)
                    map_transColumn['Quarter'].append(quarter)
                    #print(stateName , year ,quarter , mapDistrict ,mapCount,mapType,mapAmount,sep=("|"))
                    map_transSql = "INSERT INTO map_transaction (State,Year,Quarter,District,Transaction_type,Transaction_count,Transaction_amount) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    map_transVal = (stateName , year ,quarter ,mapDistrict,mapType ,mapCount ,mapAmount)
                    mycursor.execute(map_transSql , map_transVal)
            transdataFile.close()    
    #return map_transColumn

# map User
#--------------------------
#map_user_StatesPath=destinPath+'/data/map/user/hover/country/india/state/'
def mapUser(map_user_StatesPath):
    mycursor.execute("CREATE TABLE IF NOT EXISTS map_User (State VARCHAR(50),Year VARCHAR(10), Quarter VARCHAR(5), District VARCHAR(50),RegisteredUsers BIGINT ,AppOpens BIGINT)")
    map_userList = os.listdir(map_user_StatesPath)
    map_userColumn= {"States":[], "Years":[], "Quarter":[],"District":[] ,'registeredUsers':[], 'appOpens':[]}
    for state in map_userList:
        stateName =((state.title()).replace("-&-"," and ")).replace("-"," ") 
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =map_user_StatesPath+state+"/"
        year_map_userList = os.listdir(statePath)
        for year in year_map_userList:
            yearPath =map_user_StatesPath+state+"/"+year+"/"
            data_map_userList = os.listdir(yearPath)
            for data in data_map_userList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                userdataFile = open(file)
                # returns JSON object as a dictionary
                userMapDataDict = json.load(userdataFile)
                for i in userMapDataDict['data']['hoverData']:
                    mapDistrict=i.title().replace(" And "," and ")
                    map_userColumn['District'].append(mapDistrict)
                    mapRegUsers= userMapDataDict['data']['hoverData'][i]['registeredUsers']
                    map_userColumn['registeredUsers'].append(mapRegUsers)
                    map_appOpens = userMapDataDict['data']['hoverData'][i]['appOpens']
                    map_userColumn['appOpens'].append(map_appOpens)
                    map_userColumn['States'].append(stateName)
                    map_userColumn['Years'].append(year)
                    map_userColumn['Quarter'].append(quarter)                    
                    #print(stateName , year ,quarter , mapDistrict ,mapRegUsers,map_appOpens,sep=("|"))
                    map_userSql = "INSERT INTO map_User (State,Year,Quarter,District,RegisteredUsers,AppOpens) VALUES (%s, %s ,%s,%s, %s ,%s)"
                    map_userVal = (stateName , year ,quarter ,mapDistrict ,mapRegUsers ,map_appOpens)
                    mycursor.execute(map_userSql , map_userVal)
                userdataFile.close()
    #return map_userColumn

# top Insurance
#--------------------------
#top_insur_StatesPath=destinPath+'/data/top/insurance/country/india/state/'
def topInsurance(top_insur_StatesPath):
    mycursor.execute("CREATE TABLE IF NOT EXISTS top_insurance (State VARCHAR(50),Year VARCHAR(10), Quarter VARCHAR(5),District VARCHAR(50) , Insurance_type VARCHAR(255),Insurance_count BIGINT ,Insurance_amount FLOAT)")
    top_insurList = os.listdir(top_insur_StatesPath)
    top_insurColumn= {"States":[], "Years":[], "Quarter":[],"District":[] , "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[] }
    for state in top_insurList:
        stateName =((state.title()).replace("-&-"," and ")).replace("-"," ") 
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =top_insur_StatesPath+state+"/"
        year_top_insurList = os.listdir(statePath)
        for year in year_top_insurList:
            yearPath = top_insur_StatesPath+state+"/"+year+"/"
            data_top_insurList = os.listdir(yearPath)
            for data in data_top_insurList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                dataFile = open(file)
                # returns JSON object as a dictionary
                topInsurDataFile = json.load(dataFile)
                for i in topInsurDataFile['data']['districts']: #
                    topInsuDistrict = (i['entityName']).title().replace(" And "," and ")
                    top_insurColumn['District'].append(topInsuDistrict)
                    topInsuType = i['metric']['type']
                    top_insurColumn['Insurance_type'].append(topInsuType)
                    topInsuCount = i['metric']['count']
                    top_insurColumn['Insurance_count'].append(topInsuCount)
                    topInsuAmount = round(i['metric']['amount'])
                    top_insurColumn['Insurance_amount'].append(topInsuAmount)
                    top_insurColumn['States'].append(stateName)
                    top_insurColumn['Years'].append(year)
                    top_insurColumn['Quarter'].append(quarter)
                    #print(stateName ,year ,quarter ,topInsuDistrict ,topInsuType ,topInsuCount , topInsuAmount ,sep=("|"))
                    agg_transSql = "INSERT INTO top_insurance (State,Year,Quarter,District,Insurance_type,Insurance_count,Insurance_amount) VALUES (%s, %s ,%s,%s, %s ,%s,%s)"
                    agg_transVal = (stateName , year ,quarter ,topInsuDistrict,topInsuType ,topInsuCount ,topInsuAmount)
                    mycursor.execute(agg_transSql , agg_transVal)
                dataFile.close()
    #return top_insurColumn

# top Transaction
#--------------------------
#top_trans_StatesPath=destinPath+'/data/top/transaction/country/india/state/'
def topTransaction(top_trans_StatesPath):
    mycursor.execute("CREATE TABLE IF NOT EXISTS top_transaction (State VARCHAR(50),Year VARCHAR(10), Quarter VARCHAR(5),District VARCHAR(50),Transaction_type VARCHAR(255),Transaction_count BIGINT ,Transaction_amount FLOAT)")
    top_transList = os.listdir(top_trans_StatesPath)
    top_transColumn= {"States":[], "Years":[], "Quarter":[],"District":[] , "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[] }
    for state in top_transList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =top_trans_StatesPath+state+"/"
        year_top_transList = os.listdir(statePath)
        for year in year_top_transList:
            yearPath = top_trans_StatesPath+state+"/"+year+"/"
            data_top_transList = os.listdir(yearPath)
            for data in data_top_transList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                dataFile = open(file)
                # returns JSON object as a dictionary
                topTransDataFile = json.load(dataFile)
                for i in topTransDataFile['data']['districts']:
                    toptransDistrict = (i['entityName']).title().replace(" And "," and ")
                    top_transColumn['District'].append(toptransDistrict)
                    toptransType = i['metric']['type']
                    top_transColumn['Transaction_type'].append(toptransType)
                    toptransCount = i['metric']['count']
                    top_transColumn['Transaction_count'].append(toptransCount)
                    toptransAmount = round(i['metric']['amount'],2)
                    top_transColumn['Transaction_amount'].append(toptransAmount) 
                    top_transColumn['States'].append(stateName)
                    top_transColumn['Years'].append(year)
                    top_transColumn['Quarter'].append(quarter)
                    #print(stateName ,year ,quarter ,toptransDistrict ,toptransType ,toptransCount , toptransAmount ,sep=("|"))
                    top_transSql = "INSERT INTO top_transaction (State,Year,Quarter,District,Transaction_type,Transaction_count,Transaction_amount) VALUES (%s, %s ,%s,%s, %s ,%s,%s)"
                    top_transVal = (stateName , year ,quarter ,toptransDistrict,toptransType ,toptransCount ,toptransAmount)
                    mycursor.execute(top_transSql , top_transVal)
                dataFile.close()
    #return top_transColumn

# top User
#--------------------------
##top_user_StatesPath=destinPath+'/data/top/user/country/india/state/'
def topUser(top_user_StatesPath):
    mycursor.execute("CREATE TABLE IF NOT EXISTS top_User (State VARCHAR(50),Year VARCHAR(10), Quarter VARCHAR(5),District VARCHAR(50),RegisteredUsers BIGINT )")
    top_userList = os.listdir(top_user_StatesPath)
    top_userColumn= {"States":[], "Years":[], "Quarter":[],"District":[] ,'registeredUsers':[], }
    for state in top_userList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =top_user_StatesPath+state+"/"
        year_top_userList = os.listdir(statePath)
        for year in year_top_userList:
            yearPath = top_user_StatesPath+state+"/"+year+"/"
            data_top_userList = os.listdir(yearPath)
            for data in data_top_userList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                dataFile = open(file)
                # returns JSON object as a dictionary
                topUserDataFile = json.load(dataFile)
                for i in topUserDataFile['data']['districts']:
                    topUserDistrict = (i['name']).title().replace(" And "," and ")
                    top_userColumn['District'].append(topUserDistrict)
                    topUserRegUser = i['registeredUsers']
                    top_userColumn['registeredUsers'].append(topUserRegUser)
                    top_userColumn['States'].append(stateName)
                    top_userColumn['Years'].append(year)
                    top_userColumn['Quarter'].append(quarter)
                    #print(stateName ,year ,quarter ,topUserDistrict ,topUserRegUser ,sep=("|"))
                    top_userSql = "INSERT INTO top_User (State,Year,Quarter,District ,RegisteredUsers) VALUES (%s, %s ,%s,%s, %s )"
                    top_userVal = (stateName , year ,quarter ,topUserDistrict ,topUserRegUser )
                    mycursor.execute(top_userSql , top_userVal)
                dataFile.close()
    #return top_userColumn

# this is a dummy tablr creation function as i have error with last table creation function so  i created a dummy function so all required tabls will get created intactly without missing any table
##top_user_StatesPath=destinPath+'/data/top/user/country/india/state/'
def dumTopUser(top_user_StatesPath):
    mycursor.execute("CREATE TABLE IF NOT EXISTS dummyTop_User (State VARCHAR(50),Year VARCHAR(10), Quarter VARCHAR(5),District VARCHAR(50),RegisteredUsers BIGINT )")
    top_userList = os.listdir(top_user_StatesPath)
    top_userColumn= {"States":[], "Years":[], "Quarter":[],"District":[] ,'registeredUsers':[], }
    for state in top_userList:
        stateName =((state.title()).replace("-&-"," & ")).replace("-"," ") 
        if stateName== "Dadra & Nagar Haveli & Daman & Diu":
            stateName ="Dadra and Nagar Haveli and Daman and Diu"
        statePath =top_user_StatesPath+state+"/"
        year_top_userList = os.listdir(statePath)
        for year in year_top_userList:
            yearPath = top_user_StatesPath+state+"/"+year+"/"
            data_top_userList = os.listdir(yearPath)
            for data in data_top_userList:
                quartSplit = data.split(".")
                quarter = quartSplit[0]
                file = yearPath+data
                # Opening JSON file
                dataFile = open(file)
                # returns JSON object as a dictionary
                topUserDataFile = json.load(dataFile)
                for i in topUserDataFile['data']['districts']:
                    topUserDistrict = (i['name']).title().replace(" And "," and ")
                    top_userColumn['District'].append(topUserDistrict)
                    topUserRegUser = i['registeredUsers']
                    top_userColumn['registeredUsers'].append(topUserRegUser)
                    top_userColumn['States'].append(stateName)
                    top_userColumn['Years'].append(year)
                    top_userColumn['Quarter'].append(quarter)
                    #print(stateName ,year ,quarter ,topUserDistrict ,topUserRegUser ,sep=("|"))
                    Dumtop_userSql = "INSERT INTO dummyTop_User (State,Year,Quarter,District ,RegisteredUsers) VALUES (%s, %s ,%s,%s, %s )"
                    Dumtop_userVal = (stateName , year ,quarter ,topUserDistrict ,topUserRegUser )
                    mycursor.execute(Dumtop_userSql , Dumtop_userVal)
                dataFile.close()
    #return top_userColumn


# streamlit
#--------------------------
def insiLoc(state="",district=""):
    if state=="Karnataka":
        stateTxt="  .Karnataka tops in result because it has City Bengaluru , the Silicon Valley of India which is has a Cosmopolatian as well as Metropolatian status that has highily paid Techies from all over the Country and even few Foriegniers as Students , Fashion Professionals and Bussiness people"
        if district=="Bengaluru Urban District":
            districtTxt="  .Bengaluru Urban District is the major contributor for the count of numbers or amount generated by Phone Pe in the state of Karnataka as this particular District has a huge population of Techies and many migrant low profile manpower Employees from accross the the country who help the Infrastructure development of the Silicon City"  
        else:
            districtTxt=""
    elif state=="Maharashtra":
        stateTxt=" .Maharashtra tops in result because it has Cities like Mumbai and Pune .Mumbai, the capital of Maharashtra and often described as the New York of India or Manhattan of India,is the financial capital and the most populous city of India with an estimated city proper population of 12.5 million (1.25 crore).The city is the entertainment, fashion, and commercial centre of India. Mumbai hosts the largest urban economy of any city in India. is considered the financial capital of India with the headquarters of almost all major banks, financial institutions, insurance companies and mutual funds being based in the city. India's largest stock exchange Bombay Stock Exchange, the oldest in Asia,is also located in the city."
        if district=="Pune District":
            districtTxt="Pune is the second-largest city in the state of Maharashtra after Mumbai, and is an important city in terms of its economic and industrial growth. Once the centre of power for the Maratha Empire, Pune’s rich historical past has made it the cultural capital of Maharashtra. Home to many colleges and universities, Pune is considered a prestigious educational destination, winning itself the title of “The Oxford of the East”. Pune has emerged as a new startup hub in recent years with information technology (IT), engineering and automotive companies sprouting. The city is also known for cultural activities such as classical music, theatre, sports and literature.7 According to the Ease of Living Index 2020, Pune was ranked as the second-best city to live in India."
        else:
            districtTxt=""
    elif state=="Telangana":
        stateTxt="  .Telangana tops in result because it has City Hydrabad, which is India's secod biggest Tech Hub with a lots of masses highily paid Techies from all over the Country and the city gives the employment for many migrant low profile manpower Employees from accross the the country who help the Infrastructure development of the City"
        if district=="Hyderabad District":
            districtTxt="  .The Hyderabad district houses some of the most prestigious industries.With cosmopolitan culture, the city has congenial and hospitable environment for growth and development of SSI and Tiny industries.City has lots of tourist attraction"
        else:
            districtTxt=""
    elif state=="Uttar Pradesh":
        stateTxt="  .Uttar Pradesh is the most populous state in India with a population of nearly 240 million people. The economy of Uttar Pradesh is the fifth largest among states in India.Uttar Pradesh is a favoured tourist destination in India with Varanasi, considered to be one of the oldest living city of the world, a holy place for devotees of Lord Shiva and Taj Mahal, one of the eight Wonders of the World, is also located here in Agra. In 2022, domestic tourist arrivals in the state stood at 317.91 million.Varanasi, Agra, Ayodhya, Mathura and Prayagraj were among the most visited cities"
        if district=="Bengaluru Urban District":
            districtTxt=""
        else:
            districtTxt=""
    elif state=="Delhi":
        stateTxt="  .The service sector is the most important part of Delhi’s economy, and it is the city’s largest employer. Manufacturing has remained significant, after a surge in the 1980s. Agriculture once contributed significantly to the economy of the national capital territory, but now it is of little importance1. Delhi is one of the fastest-growing states in the country, with a Gross State Domestic Product (GSDP) of Rs 9.23 trillion (US$ 123.90 billion) in 2021-2022. The state’s GSDP increased at a CAGR of 8.89% between 2015-16 and 2021-2022."
        if district=="Bengaluru Urban District":
            districtTxt=""
        else:
            districtTxt=""
    elif state=="West Bengal":
        stateTxt="  .The economy of West Bengal is a mixed middle-income developing social market economy and the largest Eastern Indian economy with a substantial public sector. It is the India's sixth-largest economy by nominal GDP. West Bengal is the primary business and financial hub of Eastern India."
        if district=="Bengaluru Urban District":
            districtTxt=""
        else:
            districtTxt=""   
    elif state=="Bihar":
        stateTxt="  .The economy of Bihar is one of the fastest-growing in India. It is largely service-based, with a significant share of agricultural and industrial sectors1. The GDP of the state was ₹7,45,310 crores (US$94 billion) at the current market price (2022–23). Bihar’s economy is largely based on its agriculture, and the state also has a small industrial sector. According to the Gross State Domestic Product (GSDP), the Bihar economy has increased at a compound annual growth rate of 13.27% from 2015 to 2016 and from 2019-to 20202"
        if district=="Bengaluru Urban District":
            districtTxt=""
        else:
            districtTxt=""
    elif state=="Jharkhand":
        stateTxt="  .Jharkhand is one of the leading states in India in terms of economic growth. The state's gross state domestic product (GSDP) stood at Rs. 3.63 trillion (US$ 48.63 billion) in 2021-22. Jharkhand is one of the richest mineral zones in the world and boasts of 40% and 29% of India's mineral and coal reserves, respectively. The economy depends mostly on mineral resources, industries, agricultural, and tourism sectors. The State’s Gross Domestic Product amounted to US $ 14 billion in 2004, which moved to US $ 22.46 billion in 2010-112."
        if district=="Bengaluru Urban District":
            districtTxt=""
        else:
            districtTxt=""   
    elif state=="Rajasthan":
        stateTxt="  .Rajasthan is a mineral-rich state with a diversified economy that includes agriculture, mining, and tourism. It produces gold, silver, sandstone, limestone, marble, rock phosphate, copper, and lignite. Rajasthan is the second-largest producer of cement and contributes to salt production in India"
        if district=="Ganganagar District":
            districtTxt=" .Agriculture is the backbone of the economy of Ganganagar district due to the presence of Gang canal. More than half of their populations are engaged in agriculture in order to earn their livelihood. Oil seeds, sugarcane, cotton and food grains are the main agricultural crops which help in flourishing of the oil, cotton and sugar factories"
        else:
            districtTxt="" 
    elif state=="":
        stateTxt=""
        if district=="":
            districtTxt=""
        else:
            districtTxt=""   

    return str(stateTxt+districtTxt)


#streamlit page_config
st.set_page_config(page_title="PhonePe Dashboard", page_icon="https://asset.brandfetch.io/idcE0OdG8i/idzxSpanBR.png", layout="wide", initial_sidebar_state="auto", menu_items=None)
col1 ,col2,col3,col4 = st.columns(4)
with col1:
    st.image("https://dishaelectricals.com/wp-content/uploads/2023/11/sss-scaled.webp")   
with col2:
    st.write("")
    st.write(":violet[**_DATA VISUALIZATION AND EXPLORATION_ :**  A User-Friendly Tool Using Streamlit and Plotly]")
with col3:
    st.write("")
with col4:
    st.write(":red[RBI's Expense in printing Currancy Notes for FY 2021-22 FY: ₹4,984.8 Cr.]")
    st.page_link('https://www.indiatimes.com/worth/news/rbi-spent-nearly-5000-crore-rupees-on-banknote-printing-in-fy22-572767.html',  label=":red[_Read more.._]", use_container_width=None)

    #st.write("Data compiled from RBI’s annual reports shows the cost of printing currency notes in 2021-22 at ₹4,984.8 crore was almost 1.5 times the ₹2,063.16 crore spent in 2008-09. In fiscal year 2021-22, RBI spent 24 per cent more on printing notes than it did in 2020-21 (Rs 4,012.09 crore), and that too for fewer notes supplied from its printing presses.")
with st.sidebar:
    st.image("https://i0.wp.com/spicyip.com/wp-content/uploads/2023/06/PhonePe_Logo.svg_.png")
    st.subheader(":green[Welcome to PhonePe Dashboard]")
    st.divider()

    workpath = str(st.text_input(":orange[Please Enter your Local Drive Path to clone the Date from GitHub]",))
    
    submit = st.button("Clone GitHub")
    if submit:
        if  workpath =="":
            st.write(":red[please Enter a Valid Path]")
        else:
            createDirectory(workpath)

    st.divider()
    st.write(":orange[Please click the below \n Button to  Clean the \n Data and Store it]")
    clean_n_store = st.button("Clean and Store")
    destinPath=workpath+"/Data"
    if clean_n_store:
        mycursor.execute("SHOW TABLES")
        myresult = mycursor.fetchall()
        if len(myresult)==0:
            #aggragated
            agg_insur_StatesPath=destinPath+'/data/aggregated/insurance/country/india/state/'
            aggragateInsurance(agg_insur_StatesPath)

            agg_trans_StatesPath=destinPath+'/data/aggregated/transaction/country/india/state/'
            aggragateTransaction(agg_trans_StatesPath)

            agg_user_StatesPath=destinPath+'/data/aggregated/user/country/india/state/'
            aggragateUser(agg_user_StatesPath)        

            #map
            map_insur_StatesPath=destinPath+'/data/map/insurance/hover/country/india/state/'
            mapInsurance(map_insur_StatesPath)

            map_trans_StatesPath=destinPath+'/data/map/transaction/hover/country/india/state/'
            mapTransaction(map_trans_StatesPath)

            map_user_StatesPath=destinPath+'/data/map/user/hover/country/india/state/'
            mapUser(map_user_StatesPath)      

            #top
            top_trans_StatesPath=destinPath+'/data/top/transaction/country/india/state/'
            topTransaction(top_trans_StatesPath)

            top_user_StatesPath=destinPath+'/data/top/user/country/india/state/'
            topUser(top_user_StatesPath)

            top_insur_StatesPath=destinPath+'/data/top/insurance/country/india/state/'
            topInsurance(top_insur_StatesPath)

            top_user_StatesPath=destinPath+'/data/top/user/country/india/state/'
            dumTopUser(top_user_StatesPath)

            db.commit()
            
            dbStoreStatus = 1
        else:
            st.write(":red[Already Cleaned and Stored]") 
            pass     
        
    st.divider()
    st.write(":violet[Project by \n HARISH KUMAR K P ]")
    st.write(":violet[email : \n harishk_kotte@rediffmail.com]")
    sql=""  
mycursor.execute("SHOW TABLES")
myresult = mycursor.fetchall()
if len(myresult)==0:
    st.write(":red[No Data Availabl to Display , Please Click on Clean and Store in Sidebar]")
else:#
    querySelected = st.selectbox(
    "",#:red[**Select The Quarter**] :point_down: 
    ("1. What is the Aggragate Transaction Count for selected Transaction type of all States for Selected Year and its Quarter?", 
        "2. What is the Aggragate Transaction Amount for selected Transaction type of all States for Selected Year and its Quarter?", 
        "3. What is the UserCount or Percentage of Specific Brand Phone Users in all the states for Selected Year and its Quarter?",
        "4. What are the top 5 Districts in Transaction Amount and their Corrosponding States for Selected Year and its Quarter?", 
        "5. What is the Top RegisterdUsers Districts of all State for Selected Year and its Quarter?", 
        "6. What are the Top 5 Districts in AppOpen Count and their Corrosponding States for Selected Year and its Quarter?",
        "7. What is the Total Count of State's District? List Maximum on First for Selected Year and its Quarter", 
        "8. Which State has Highest Insurace Count for Selected Year and its Quarter?", 
        "9. Which State has Highest Insurace Amount for Selected Year and its Quarter?",
        "10.Which State's Disctrict was in Top in the Insurance Amount For All the 9 Quarters now and in past?",),
    placeholder="Select Your Query Here ...",
    )

    col1 ,col2,col3,col4,col5  = st.columns(5 ,gap="small")
    with col1:
            if querySelected == "10.Which State's Disctrict was in Top in the Insurance Amount For All the 9 Quarters now and in past?":
                st.write("")
            else:
                yearSelected = st.selectbox(
        "",#:blue[**Select The Year**] :point_down: 
        ("2018", 
            "2019", 
            "2020",
            "2021", 
            "2022", 
            "2023",
            "2024",),
        placeholder="Select Your Query Here ...",
        )
            
    with col2:
            if querySelected == "10.Which State's Disctrict was in Top in the Insurance Amount For All the 9 Quarters now and in past?":
                st.write("")
            else:
                quarterSelected = st.selectbox(
        "",#:orange[**Select The Quarter**] :point_down: 
        ("Quarter 1", 
            "Quarter 2", 
            "Quarter 3",
            "Quarter 4",),
        placeholder="Select Quarter Here ...",
        )        
    with col3:
            if querySelected == "10.Which State's Disctrict was in Top in the Insurance Amount For All the 9 Quarters now and in past?":
                st.write("")
            elif querySelected == "1. What is the Aggragate Transaction Count for selected Transaction type of all States for Selected Year and its Quarter?"  :
                    transactionSelected = st.selectbox("",#:green[**Select Transaction Type**] :point_down: 
                                                       ("Recharge & bill payments", "Peer-to-peer payments", 
                                                        "Merchant payments","Financial Services","Others",),
                                                        placeholder="Select Quarter Here ...",)
            elif querySelected == "2. What is the Aggragate Transaction Amount for selected Transaction type of all States for Selected Year and its Quarter?"  :
                    transactionSelected = st.selectbox("",#:green[**Select Transaction Type**] :point_down: 
                                                       ("Recharge & bill payments", "Peer-to-peer payments", 
                                                        "Merchant payments","Financial Services","Others",),
                                                        placeholder="Select Quarter Here ...",)
            elif querySelected == "3. What is the UserCount or Percentage of Specific Brand Phone Users in all the states for Selected Year and its Quarter?"  :
                    brandSelected = st.selectbox("",#:green[**Select Device Brand**] :point_down: 
                                                       ("Apple", "Samsung","Motorola",  "Lenovo","Asus","Huawei","OnePlus",
                                                        "Oppo","Vivo","Realme","Gionee","Infinix","Tecno","Others",),                                                         
                                                        placeholder="Select Quarter Here ...",)
            else:
                st.write("")    
    with col4:
        if querySelected == "3. What is the UserCount or Percentage of Specific Brand Phone Users in all the states for Selected Year and its Quarter?":
                    deviceDetailSelected = st.selectbox("",#:grey[**Select User_count / Percentage**] :point_down: 
                                                       ("User_count", "Percentage",),
                                                        placeholder="Select Quarter Here ...",)
        else:
            st.write("")  

    with col5:
        st.write("")

    if querySelected == "1. What is the Aggragate Transaction Count for selected Transaction type of all States for Selected Year and its Quarter?":
        x =1
        sql = "SELECT State ,Year,Quarter,Transaction_type,Transaction_count FROM agg_transaction WHERE (Year, Quarter,Transaction_type) = ("+yearSelected+","+quarterSelected[-1]+",'"+transactionSelected+"' ) ORDER BY Transaction_count DESC" 
    elif querySelected == "2. What is the Aggragate Transaction Amount for selected Transaction type of all States for Selected Year and its Quarter?":
        x = 2
        sql = "SELECT State ,Year,Quarter,Transaction_type,Transaction_amount FROM agg_transaction WHERE (Year, Quarter,Transaction_type) = ("+yearSelected+","+quarterSelected[-1]+",'"+transactionSelected+"' ) ORDER BY Transaction_amount DESC"
    elif querySelected == "3. What is the UserCount or Percentage of Specific Brand Phone Users in all the states for Selected Year and its Quarter?":
        x =3
        sql = "SELECT State ,Year,Quarter,Brands,"+deviceDetailSelected+" FROM agg_user WHERE (Year, Quarter , Brands) = ("+yearSelected+","+quarterSelected[-1]+" ,'"+brandSelected+"') ORDER BY "+deviceDetailSelected+" DESC"
        
    elif querySelected == "4. What are the top 5 Districts in Transaction Amount and their Corrosponding States for Selected Year and its Quarter?":
        x =4
        #sql = "SELECT State ,Year,Quarter,SUM(Transaction_amount ) AS Total_Transaction_Amount  FROM map_transaction  WHERE (Year, Quarter ) = ("+yearSelected+","+quarterSelected[-1]+")  GROUP BY State ORDER BY Total_Transaction_Amount DESC LIMIT 10 "
        sql = "SELECT State,Year,Quarter,District,Transaction_amount FROM map_transaction  WHERE (Year, Quarter ) = ("+yearSelected+","+quarterSelected[-1]+")  ORDER BY Transaction_amount DESC LIMIT 5"
    elif querySelected == "5. What is the Top RegisterdUsers Districts of all State for Selected Year and its Quarter?":
        x =5
        sql = "SELECT State ,Year,Quarter,District, RegisteredUsers  FROM map_User  WHERE (Year, Quarter ) = ("+yearSelected+","+quarterSelected[-1]+" )  ORDER BY RegisteredUsers DESC LIMIT 10" #,District 
        #sql = "SELECT State ,Year,Quarter ,SUM(RegisteredUsers ) AS TotalRegisteredUsers FROM map_User  WHERE (Year, Quarter ) = (2024 , 1 )  GROUP BY State ORDER BY TotalRegisteredUsers DESC LIMIT 10"
    elif querySelected == "6. What are the Top 5 Districts in AppOpen Count and their Corrosponding States for Selected Year and its Quarter?":
        x =6
        #sql = "SELECT State ,Year,Quarter,District,Transaction_amount FROM top_transaction WHERE (Year, Quarter) = ("+yearSelected+","+quarterSelected[-1]+") ORDER BY Transaction_amount DESC limit 3"
        sql = "SELECT State ,Year,Quarter,District,AppOpens as AppOpenCount  FROM map_User  WHERE (Year, Quarter ) = ("+yearSelected+","+quarterSelected[-1]+" )  ORDER BY AppOpenCount DESC LIMIT 5"
    elif querySelected == "7. What is the Total Count of State's District? List Maximum on First for Selected Year and its Quarter":
        x =7
        sql = "SELECT State ,Year,Quarter,COUNT(District) AS DistrictCount FROM map_transaction  WHERE (Year, Quarter ) = ("+yearSelected+","+quarterSelected[-1]+")  GROUP BY State ORDER BY DistrictCount DESC"
        
    elif querySelected == "8. Which State has Highest Insurace Count for Selected Year and its Quarter?":
        x =8  
        sql = "SELECT State ,Year,Quarter,Insurance_count FROM agg_insurance  WHERE (Year, Quarter ) = ("+yearSelected+","+quarterSelected[-1]+") ORDER BY Insurance_count DESC"
        
    elif querySelected == "9. Which State has Highest Insurace Amount for Selected Year and its Quarter?":
        x =9
        sql = "SELECT State ,Year,Quarter,Insurance_amount FROM agg_insurance  WHERE (Year, Quarter ) = ("+yearSelected+","+quarterSelected[-1]+" ) ORDER BY Insurance_amount DESC"
        
    elif querySelected == "10.Which State's Disctrict was in Top in the Insurance Amount For All the 9 Quarters now and in past?":
        x=10
        sql = "SELECT State ,Year, Quarter ,District, Insurance_amount  FROM top_insurance  ORDER BY Insurance_amount DESC LIMIT 9"

    mycursor.execute(sql)
    myresult = mycursor.fetchall()

    col1 ,col2  = st.columns(2,gap="small")
    with col1:
            if x==1:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State","Transaction_count"]]
                colVar ="Transaction_count"
                colVarX = "State"
                colTitle ="Transaction count of "+transactionSelected +" Type on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.pie(dfCol2, values=colVar, names='State',title = colTitle)
                try:
                    insightText=":green[The State "+dfCol2['State'][0]+" Shows Transaction count of "+str(dfCol2['Transaction_count'][0])+" of Transaction Type '"+transactionSelected+"' on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0])+".]" # 
                except:
                    pass
            elif x==2:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State","Transaction_amount"]]
                colVar ="Transaction_amount"
                colVarX = "State"
                colTitle = "Transaction Amount of "+transactionSelected +" Type on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.bar(dfCol2, y=colVar, x=colVarX,color=colVarX, title=colTitle)
                try:
                    insightText=":green[The State "+dfCol2['State'][0]+" Shows Transaction amount of Rs. "+str(dfCol2['Transaction_amount'][0])+" of Transaction Type '"+transactionSelected+"' on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0])+".]"
                except:
                    pass
            elif x==3:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State",deviceDetailSelected]]
                colVar =deviceDetailSelected
                colVarX = "State"
                colTitle = deviceDetailSelected+" of "+brandSelected+" Brand Phone Users on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.density_heatmap(dfCol2, x=colVarX, y=colVar,text_auto=True,title=colTitle)
                try:
                    insightText=":green[In the year "+yearSelected+" on "+quarterSelected+" "+dfCol2['State'][0]+" State shows higher "+deviceDetailSelected+" of "+str(dfCol2[deviceDetailSelected][0])+" '"+brandSelected+"' brand device owned PhonePe users "+".Generally by brand value the owner's Geographical State can be catogorised on Economic profiling ,beside this the data can be used for Technical Reserch Team on Application development for the specific device brand and its compatibilty ,efficiency can be analysed.] "
                except:
                    pass
            elif x==4:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State","Transaction_amount","District"]]
                colVar ="Transaction_amount"
                colVarX = "District"
                colTitle = "The top 5 Districts in Transaction Amount on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.line(dfCol2, x=colVarX, y=colVar,title=colTitle)
                try:
                    insightText=":green[The state "+dfCol2['State'][0]+"'s "+dfCol2['District'][0]+" is on the top for Transaction Amount of Rs " +str(dfCol2['Transaction_amount'][0])+" on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0],dfCol2['District'][0])+".]"
                except:
                    pass
            elif x==5:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State","RegisteredUsers","District"]]
                colVar ="RegisteredUsers"
                colVarX = "District"
                colTitle = "Top "+colVar +" Districts of all State on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.funnel(dfCol2, x=colVar ,y=colVarX,color=dfCol2['State'], title=colTitle)
                try:
                    insightText=":green[The state "+dfCol2['State'][0]+"'s "+dfCol2['District'][0]+" is on the top for Registered Users with the count of " +str(dfCol2['RegisteredUsers'][0])+" on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0],dfCol2['District'][0])+".]"
                except:
                    pass
            elif x==6:
                dfCol2 = pd.DataFrame(myresult,columns = mycursor.column_names)[["State","AppOpenCount","District"]]
                colVar ="AppOpenCount"
                colVarX = "District"
                colTitle = "Top 5 Districts in "+colVar+" and their States on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.scatter(dfCol2, x=colVarX, y=colVar,color=dfCol2['State'], title=colTitle)
                try:
                    insightText=":green[The state "+dfCol2['State'][0]+"'s "+dfCol2['District'][0]+" is on the top for AppOpen Count with the count of " +str(dfCol2['AppOpenCount'][0])+" on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0],dfCol2['District'][0])+".]"
                except:
                    pass
            elif x==7:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State","DistrictCount"]]
                colVar ="DistrictCount"
                colVarX = "State"
                colTitle = "Total "+colVar+" of State's and their list on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.funnel(dfCol2, x=colVarX ,y=colVar,color=dfCol2['State'], title=colTitle)
                try:
                    insightText=":green[The state "+dfCol2['State'][0]+" is on the top for District Count with the count of " +str(dfCol2['DistrictCount'][0])+" on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0])+".]" # 
                except:
                    pass
            elif x==8:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State","Insurance_count"]]
                colVar ="Insurance_count"
                colVarX = "State"
                colTitle = colVarX+"s that has "+colVar+" on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.box(dfCol2, x=colVarX, y=colVar,color=colVarX, title=colTitle)
                try:
                    insightText=":green[The state "+dfCol2['State'][0]+" is on the top for Insurance Count with the count of " +str(dfCol2['Insurance_count'][0])+" on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0])+".]"
                except:
                    pass
            elif x==9:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["State","Insurance_amount"]]
                colVar ="Insurance_amount"
                colVarX = "State"
                colTitle =colVarX +"s That has Highest "+colVar+" on PhonPe in "+quarterSelected+" of year "+yearSelected
                fig = px.pie(dfCol2, values=colVar, names='State',hole=.5, title=colTitle)
                try:
                    insightText=":green[The state "+dfCol2['State'][0]+" is on the top for Insurance Amount of Rs. " +str(dfCol2['Insurance_amount'][0])+" on PhonPe in "+quarterSelected+" of year "+yearSelected+insiLoc(dfCol2['State'][0])+".]"
                except:
                    pass
            elif x==10:
                dfCol2 = pd.DataFrame(myresult ,columns = mycursor.column_names )[["Year","State","District"]]
                colVar ="Year"
                colVarX ="State" 
                colTitle = "State's Disctrict that is in Top for Insurance Amount For All the 9 Quarters now and in past on PhonPe"
                fig = px.bar(dfCol2, x=dfCol2['District'], y=colVar, color=colVar, title=colTitle)
                try:
                    insightText=":green[Karnataka's Bengaluru Urban District is all Top in the list of for last 8 Quarters in the past and recent 9th quarter too,maybe because Bengaluru is the Silicon City of India and Everyone is aware of benifits of Insurance and opted for higher Premium Amount.]"
                except:
                    pass

            st.plotly_chart(fig, theme="streamlit", use_container_width=False)


               
    with col2:
        
        mapFig = px.choropleth(
            dfCol2,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color=colVar,
            title = colTitle ,
            color_continuous_scale='Greens'
        )

        mapFig.update_geos(fitbounds="locations", visible=True)
        #fig.show()
        st.plotly_chart(mapFig,title = colTitle , use_container_width=True)
    col1 ,col2  = st.columns(2)
    with col1:
        dfCol1 = pd.DataFrame(myresult ,columns = mycursor.column_names )
        st.dataframe(dfCol1)

    with col2:
        st.subheader(":green[The Insight of Question] "+str(x)+" :green[explains that:]")
        st.write(insightText)

    #=========================================================================================== Custom Query Creator===================================================    
    #reading Database to get value for select box
    AllStateList=["All"]
    AllDistrictsList=["All"]

    mycursor.execute("SELECT * from top_insurance")
    myCusResult = mycursor.fetchall()
    cus_sel_df = pd.DataFrame(myCusResult ,columns = mycursor.column_names )
    
    stateLst=cus_sel_df['State'].unique()
    AllStateList.extend(stateLst)
    StateTuple = tuple(AllStateList)

    distLst=cus_sel_df['District'].unique()
    AllDistrictsList.extend(distLst)
    DistrictTuple = tuple(AllDistrictsList)

    st.divider()
    st.divider()
    st.subheader(":grey[Customized Data Exploration]")
    
    whereState=0
    cusStateSelected=cusYearSelected=cusQuarterSelected=tableColumnVar=cusColumnSelected=cusBrandSelected=cusTransactionTypeSelected=cusDistrictSelected=whereBrand=stateWhere=yearWhere=quarterWhere=brandWhere=""
    cusDistrictLst=["All","Recharge & bill payments", "Peer-to-peer payments","Merchant payments","Financial Services","Others"]
    cusDistrictLstTup=tuple(cusDistrictLst)
    col1 ,col2,col3 ,col4  = st.columns(4)
    with col1:
        cusTableSelected = st.selectbox(":red[Select Table]",("Aggregated Insurance", "Aggregated Transaction", "Aggregated User","Map Insurance", "Map Transaction", "Map User","Top Insurance","Top Transaction","Top User"),placeholder="Select Your Query Here ...",)
              
    with col2:
        st.write("")
        
    col1 ,col2,col3 ,col4,col5 ,col6,col7   = st.columns(7)
    with col1:
        cusModeSelected = st.selectbox(":green[Select Mode] ",("All Data","Selective Data",),placeholder="Select Your Query Here ...",)             
    with col2:
        if cusModeSelected=="Selective Data":
            cusStateSelected = st.selectbox(":blue[Select State]",StateTuple,placeholder="Select Your Query Here ...",)
        else:
            st.write("")
    with col3:
        if cusModeSelected=="Selective Data":
            cusYearSelected = st.selectbox(":blue[Select Year]",("All","2018","2019", "2020","2021","2022","2023","2024",),placeholder="Select Your Query Here ...",)
        else:
            st.write("")
    with col4:
        if cusModeSelected=="Selective Data":
            cusQuarterSelected = st.selectbox(":blue[Select Quarter]",("All","Quarter 1","Quarter 2", "Quarter 3","Quarter 4",),placeholder="Select Your Query Here ...",) 
        else:
            st.write("")
    with col5:
        if cusModeSelected=="Selective Data":
            if cusTableSelected =="Aggregated User":
                cusBrandSelected = st.selectbox(":blue[Select Device Brand]",("All","Apple", "Samsung","Motorola",  "Lenovo","Asus","Huawei","OnePlus", "Oppo","Vivo","Realme","Gionee","Infinix","Tecno","Others",),placeholder="Select Quarter Here ...",)  
            elif cusTableSelected =="Aggregated Transaction" or cusTableSelected =="Map Transaction":
                cusTransactionTypeSelected = st.selectbox(":blue[Select Trasaction Type]",("All","Recharge & bill payments", "Peer-to-peer payments","Merchant payments","Financial Services","Others",),placeholder="Select Quarter Here ...",)  
            elif cusTableSelected =="Map Insurance" or cusTableSelected =="Map User" or cusTableSelected =="Top Insurance" or cusTableSelected =="Top Transaction" or cusTableSelected =="Top User":                   
                cusDistrictSelected = st.selectbox(":blue[Select District]",DistrictTuple,placeholder="Select Quarter Here ...",)  
            else:
                st.write("") 
        else:
            st.write("")                                                                                                                                                                
    with col6:
        st.write("")
          
    with col7:
        if cusTableSelected=="Aggregated Insurance":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("Insurance_amount","Insurance_count","All",),placeholder="Select Your Query Here ...",) 
            selTable="agg_insurance"
            if cusColumnSelected=="All":
                tableColumnVar=",Insurance_amount,Insurance_count"
            elif cusColumnSelected=="Insurance_amount":
                tableColumnVar = ",Insurance_amount"
            elif cusColumnSelected=="Insurance_count":
                tableColumnVar=",Insurance_count" 

        elif cusTableSelected=="Aggregated Transaction":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("Transaction_amount","Transaction_count","All",),placeholder="Select Your Query Here ...",) 
            selTable="agg_transaction"
            if cusColumnSelected=="All":
                tableColumnVar=",Transaction_type,Transaction_amount,Transaction_count"
            elif cusColumnSelected=="Transaction_amount":
                tableColumnVar = ",Transaction_type,Transaction_amount"
            elif cusColumnSelected=="Transaction_count":
                tableColumnVar=",Transaction_type,Transaction_count"    
                
        elif cusTableSelected=="Aggregated User":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("User_count","Percentage","All",),placeholder="Select Your Query Here ...",) 
            selTable="agg_user"
            if cusColumnSelected=="All":
                tableColumnVar=",Brands,User_count,Percentage"
            elif cusColumnSelected=="User_count":
                tableColumnVar =",Brands,User_count"
            elif cusColumnSelected=="Percentage":
                tableColumnVar=",Brands,Percentage"    

        elif cusTableSelected=="Map Insurance":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("Insurance_amount","Insurance_count","All",),placeholder="Select Your Query Here ...",) 
            selTable="map_insurance"
            if cusColumnSelected=="All":
                tableColumnVar=",District,Insurance_amount,Insurance_count"
            elif cusColumnSelected=="Insurance_amount":
                tableColumnVar = ",District,Insurance_amount"
            elif cusColumnSelected=="Insurance_count":
                tableColumnVar=",District,Insurance_count"    

        elif cusTableSelected=="Map Transaction":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("Transaction_amount","Transaction_count","All",),placeholder="Select Your Query Here ...",) 
            selTable="agg_transaction"
            if cusColumnSelected=="All":
                tableColumnVar=",Transaction_type,Transaction_amount,Transaction_count"
            elif cusColumnSelected=="Transaction_amount":
                tableColumnVar = ",Transaction_type,Transaction_amount"
            elif cusColumnSelected=="Transaction_count":
                tableColumnVar=",Transaction_type,Transaction_count"    

        elif cusTableSelected=="Map User":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("RegisteredUsers","AppOpens","All",),placeholder="Select Your Query Here ...",) 
            selTable="map_user"
            if cusColumnSelected=="All":
                tableColumnVar=",District,RegisteredUsers,AppOpens"
            elif cusColumnSelected=="RegisteredUsers":
                tableColumnVar = ",District,RegisteredUsers"
            elif cusColumnSelected=="AppOpens":
                tableColumnVar=",District,AppOpens"    

        elif cusTableSelected=="Top Insurance":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("Insurance_amount","Insurance_count","All",),placeholder="Select Your Query Here ...",) 
            selTable="top_insurance"
            if cusColumnSelected=="All":
                tableColumnVar=",District,Insurance_amount,Insurance_count"
            elif cusColumnSelected=="Insurance_amount":
                tableColumnVar = ",District,Insurance_amount"
            elif cusColumnSelected=="Insurance_count":
                tableColumnVar=",District,Insurance_count"    

        elif cusTableSelected=="Top Transaction":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("Transaction_amount","Transaction_count","All",),placeholder="Select Your Query Here ...",) 
            selTable="top_transaction"
            if cusColumnSelected=="All":
                tableColumnVar=",District,Transaction_amount,Transaction_count"
            elif cusColumnSelected=="Transaction_amount":
                tableColumnVar = ",District,Transaction_amount"
            elif cusColumnSelected=="Transaction_count":
                tableColumnVar=",District,Transaction_count"    

        elif cusTableSelected=="Top User":
            cusColumnSelected = st.selectbox(":orange[Select Column]",("District","RegisteredUsers","All",),placeholder="Select Your Query Here ...",) 
            selTable="top_user"
            if cusColumnSelected=="All":
                tableColumnVar=",District,RegisteredUsers"
            elif cusColumnSelected=="District":
                tableColumnVar = ",District"
            elif cusColumnSelected=="RegisteredUsers":
                tableColumnVar=",RegisteredUsers" 
         

    where=and1=and2=and3=""

    if cusModeSelected=="All Data":
        whereBlock=""
        columnVar="*"
    else:
        columnVar="State ,Year,Quarter"+tableColumnVar

        if cusStateSelected =="All":
            stateWhere =""
            whereS=1
        else:
            stateWhere ="State='"+cusStateSelected+"'"
            whereS=3

        if cusYearSelected =="All":
            yearWhere =""
            whereY=1
        else:
            yearWhere ="Year="+cusYearSelected
            whereY=6

        if cusQuarterSelected =="All":
            quarterWhere =""
            whereQ=1
        else:
            quarterWhere ="Quarter="+cusQuarterSelected[-1]+" "
            whereQ=9
        if cusTableSelected=="Aggregated User":
            if cusBrandSelected =="All":
                brandWhere =""
                whereB=1
            else:
                brandWhere ="Brands='"+cusBrandSelected+"'"
                whereB=12
        elif cusTableSelected=="Aggregated Transaction" or cusTableSelected=="Map Transaction":
            if cusTransactionTypeSelected =="All":
                brandWhere =""
                whereB=1
            else:
                brandWhere ="Transaction_type='"+cusTransactionTypeSelected+"'"
                whereB=12
        elif cusTableSelected=="Map Insurance" or cusTableSelected=="Map User" or cusTableSelected=="Top Insurance" or cusTableSelected=="Top Transaction" or cusTableSelected=="Top User":
            if cusDistrictSelected =="All" or cusDistrictSelected =="Select A State":
                brandWhere =""
                whereB=1
            else:
                brandWhere ="District='"+cusDistrictSelected+" District"+"'"
                whereB=12
        else:
            whereB=1

        whereState = int(whereS*whereY*whereQ*whereB)
        if whereState == 1:
            where=""
            and1=""
            and2=""
            and3=""
        elif whereState == 3 or whereState == 6 or whereState == 9 or whereState == 12:
            where=" WHERE "
            and1=""
            and2=""
            and3=""
        elif whereState == 18 or whereState == 27 or whereState == 36:
            where=" WHERE "
            and1=" AND "
            and2=""
            and3=""
        elif whereState == 54 or whereState == 72 : 
            where=" WHERE "
            and1=""
            and2=" AND "
            and3=""
        elif whereState == 108 :
            where=" WHERE "
            and1=""
            and2=""
            and3=" AND "
        elif whereState == 162 :
            where=" WHERE "
            and1=" AND "
            and2=" AND "
            and3=""
        elif whereState == 648 :
            where=" WHERE "
            and1=""
            and2=" AND "
            and3=" AND "
        elif whereState == 1944 :
            where=" WHERE "
            and1=" AND "
            and2=" AND "
            and3=" AND "
    whereBlock=where+stateWhere+and1+yearWhere+and2+quarterWhere+and3+brandWhere

    cusSql="SELECT "+columnVar+" FROM "+selTable+ whereBlock 
    st.divider() 
    mycursor.execute(cusSql)
    myCusResult = mycursor.fetchall()
    cus_df = pd.DataFrame(myCusResult ,columns = mycursor.column_names )
    try:
        col1 ,col2,col3 ,col4,col5 ,col6,col7 ,col8  = st.columns(8)
        with col1:
            cusMapColorSelected = st.selectbox(":rainbow[Select colour]",("blues","reds","greens","oranges","greys","rainbow",),placeholder="Select Your Query Here ...",)
        with col2:
            st.write("")
        with col3:
            st.write("")
        with col4:
            st.write("")
        with col5:
            st.write("")
        with col6:
            cusChartParamSelected = st.selectbox(":rainbow[Select Parameter]",("State","Year","Quarter","District","Transaction_type","Brands"),placeholder="Select Your Query Here ...",)
        with col7:
            cusChartColourSelected = st.selectbox(":rainbow[Select Chart Colour]",(cusColumnSelected,"State","District"),placeholder="Select Your Query Here ...",)
        with col8:
            cusChartTypeSelected = st.selectbox(":rainbow[Select Chart Type]",("BarChart","PieChart","DonutChart","FunnelChart","LineChart","ScatterChart","BoxChart","Density_Heatmap"),placeholder="Select Your Query Here ...",)
           
        cusCollVar=cusColumnSelected
        col1 ,col2 = st.columns(2)
        with col1:
            cusMapFig = px.choropleth(
                cus_df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='State',
                color=cusCollVar,
                title = "choropleth map" ,
                color_continuous_scale = cusMapColorSelected
            )

            cusMapFig.update_geos(fitbounds="locations", visible=True)
            #fig.show()
            st.plotly_chart(cusMapFig,title = colTitle , use_container_width=True)
                
        with col2:
            if cusChartTypeSelected=="BarChart":
                cusChartFig = px.bar(cus_df, x=cusChartParamSelected, y=cusCollVar , color=cusChartColourSelected ,title = "BarChart")
            if cusChartTypeSelected=="PieChart":
                cusChartFig = px.pie(cus_df, values=cusCollVar, names=cusChartParamSelected,color=cusChartColourSelected ,title = "PieChart")    
            if cusChartTypeSelected=="DonutChart":
                cusChartFig = px.pie(cus_df, values=cusCollVar, names=cusChartParamSelected,color=cusChartColourSelected ,hole=.5 ,title = "PieChart") 
            if cusChartTypeSelected=="FunnelChart":                
                cusChartFig = px.funnel(cus_df, x=cusCollVar,y=cusChartParamSelected,color=cusChartColourSelected, title="FunnelChart")
            if cusChartTypeSelected=="LineChart":                    
                cusChartFig = px.line(cus_df, x=cusChartParamSelected, y=cusCollVar,title="Line Chart")
            if cusChartTypeSelected=="ScatterChart":
                cusChartFig = px.scatter(cus_df, x=cusChartParamSelected, y=cusCollVar,color=cusChartColourSelected, title="Scatter Chart")                
            if cusChartTypeSelected=="BoxChart":
                cusChartFig = px.box(cus_df, x=cusChartParamSelected, y=cusCollVar,color=cusChartColourSelected, title="BoxChart")                    
            if cusChartTypeSelected=="Density_Heatmap":
                cusChartFig = px.density_heatmap(cus_df, x=cusChartParamSelected, y=cusCollVar,text_auto=True,title="Density Heatmap")
                
            st.plotly_chart(cusChartFig,title = colTitle , use_container_width=True)

    except:
        st.write(":red[If the map and chart disappear , Please select other option except 'All' in the 'Select Column' Dropdown Menu]")    
    st.dataframe(cus_df)
    st.divider()
    st.write(":red[Generated SQL Query :] "+cusSql+";")
    st.divider()
    