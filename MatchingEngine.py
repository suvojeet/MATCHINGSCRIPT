#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 23:31:23 2021

@author: suvojeetpal
"""

import fuzzy
import editdistance
import configparser
import re
import mysql.connector
import pandas as pd
import numpy as np
import sys

config = configparser.ConfigParser()
config.read('perweight.ini')

A1=config['THRESHOLD']['A1']
B=config['THRESHOLD']['B']
exactName=config['PERSON-WEIGHT']['CU|NAME|XACT']
nysciisName = config['PERSON-WEIGHT']['CU|NAME|NYSCIIS']
phoneticName= config['PERSON-WEIGHT']['CU|NAME|PHONETIC']
metaPhName=config['PERSON-WEIGHT']['CU|NAME|METAPHONE']
suffixMatch=config['PERSON-WEIGHT']['CU|SUFFIX|XACT']
suffixMisMatch=config['PERSON-WEIGHT']['CU|SUFFIX|DIFF']
exactDob=config['PERSON-WEIGHT']['CU|DOB|XACT']
ddmmswap=config['PERSON-WEIGHT']['CU|DOB|DDMMSWP']
dobDiff=config['PERSON-WEIGHT']['CU|DOB|DIFF']
exactGender=config['PERSON-WEIGHT']['CU|GENDER|XACT']
diffGender=config['PERSON-WEIGHT']['CU|GENDER|DIFF']
IDSSNExact=config['PERSON-WEIGHT']['ID|SSN|0EDITDST']
IDSSN1DIFF=config['PERSON-WEIGHT']['ID|SSN|1EDITDST']
IDSSN2DIFF=config['PERSON-WEIGHT']['ID|SSN|2EDITDST']
IDSSN3DIFF=config['PERSON-WEIGHT']['ID|SSN|3EDITDST']
IDSSN4DIFF=config['PERSON-WEIGHT']['ID|SSN|4EDITDST']
IDSSN5DIFF=config['PERSON-WEIGHT']['ID|SSN|5EDITDST']
IDSSN6DIFF=config['PERSON-WEIGHT']['ID|SSN|6EDITDST']
IDSSN7DIFF=config['PERSON-WEIGHT']['ID|SSN|7EDITDST']
IDSSN8DIFF=config['PERSON-WEIGHT']['ID|SSN|8EDITDST']


def _matchCustomerName_(reqCustName,dbReqName):
    
   
    soundex = fuzzy.Soundex(4)
    
    reqSndexMyName=soundex(reqCustName)
    reqNysiisName=fuzzy.nysiis(reqCustName)

    dbSndexMyName=soundex(dbReqName)
    dbNysiisName=fuzzy.nysiis(dbReqName)

    if reqCustName == dbReqName:
            namescore= exactName
    else :
        if reqSndexMyName == dbSndexMyName:
            namescore=phoneticName
        elif reqNysiisName == dbNysiisName:
            namescore=nysciisName
        else:
            namescore=0
    
    return int(namescore)
    
            
def _matchCustomerId_(reqcustId,dbcustId):
    
    print(reqcustId) 
    identScore=0
    idEDist= editdistance.eval(reqcustId,dbcustId)
    
    if  idEDist == 0 :
        identScore = IDSSNExact
    elif idEDist == 1:
        identScore = IDSSN1DIFF
    elif idEDist == 2:
        identScore = IDSSN2DIFF
    elif idEDist == 3:
        identScore = IDSSN3DIFF
    elif idEDist == 4:
        identScore = IDSSN4DIFF
    elif idEDist == 5:
        identScore = IDSSN5DIFF
    elif idEDist == 6:
        identScore = IDSSN6DIFF
    elif idEDist == 7:
        identScore = IDSSN7DIFF
    elif idEDist == 8:
        identScore = IDSSN8DIFF
    else:
        identScore = -898
        
    return int(identScore)
    
def _matchCustomerDateDOB_(reqDOBdate,dbDOBdate):
    
    if reqDOBdate is not None and dbDOBdate is not None:
        dobScore=0
        dobEditDst= editdistance.eval(re.sub("[^0-9]", "", reqDOBdate),re.sub("[^0-9]", "", dbDOBdate))
    
        if  dobEditDst == 0 :
            dobScore = exactDob
        elif reqDOBdate[0:2] == dbDOBdate[2:4] and reqDOBdate[4:8] == dbDOBdate[4:8] :
            dobScore = ddmmswap
        else:
            dobScore = dobDiff
        
        return int(dobScore)

def _matchCustomerGender_(reqGender,dbGender):
    
    if reqGender is not None and dbGender is not None:
        genderScore=0
        if reqGender == dbGender :
            genderScore = exactGender
        else:
            genderScore = diffGender
        
        return int(genderScore)

def _matchCustomerSuffix_(reqcustSuffix,dbcustSuffix):
    
    if reqcustSuffix is not None and dbcustSuffix is not None:
        suffixScore=0
        if reqcustSuffix == dbcustSuffix :
            suffixScore = suffixMatch
        else:
            suffixScore = suffixMisMatch
        
        return int(suffixScore)
    
    
def _fetchMatchRecordFromDF_(inputParam):
    mydb = mysql.connector.connect(user='root', password='rootadmin',
                              host='localhost',
                              database='CUSTOMERHUB')
    mycursor = mydb.cursor()
    iselect_query="""select concat_ws(":",CCID,first_name,last_name,IFNULL(suffix,''),IFNULL(gender,''),IFNULL(ssn,''),IFNULL(dob,'')) AS 'MatchingList'
         FROM CUSTOMERHUB.CUSTOMER WHERE bucket_hash_key between %s and %s;"""

    inputParam1 = int(inputParam+"000000000000")
    inputParam2 = int(inputParam+"999999999999")

    df = pd.read_sql(iselect_query, con=mydb, params=(inputParam1,inputParam2))
    custList=df.values.tolist()
    return custList

#df_sales = pd.DataFrame(np.random.randint(low = 13, high = 148, size = 918843, dtype= 'uint8'), columns =['MatchingList'])

def _matchFunction_ (firstName,lastName,suffix,gender,ssn,dob,inputParam):
   
    custDbList=_fetchMatchRecordFromDF_(inputParam)
    
    totalscore=0
 
    parseDBcustList=[]
    suspectList=[]
    for cust in custDbList:
        for indx in cust:
            parseDBcustList.append(indx.split(":"))
        
    for custRec in parseDBcustList:
        totalscore=0
        totalscore= _matchCustomerName_(firstName,custRec[1])
        totalscore=totalscore +_matchCustomerName_(lastName,custRec[2])
        totalscore=totalscore +_matchCustomerSuffix_(suffix,custRec[3])
        totalscore=totalscore +_matchCustomerGender_(gender,custRec[4])
        totalscore=totalscore +_matchCustomerId_(ssn,custRec[5])
        totalscore=totalscore +_matchCustomerDateDOB_(dob,custRec[6])
        
        percentileScore=totalscore/100
        
        if percentileScore > int(A1):
                suspectList=[]
                A1Match='CCID: '+custRec[0]+' | Score : '+ str(totalscore/100)+ ' | Match Category :'+' A1'
                suspectList.append(A1Match)
                break;
        elif percentileScore < int(A1) and percentileScore > int(B) :
                BMatch='CCID: '+custRec[0]+' | Score : '+ str(totalscore/100)+ ' | Match Category :'+' B'
                suspectList.append(BMatch)
        else:
                print(' ') 
       

    return suspectList



def main(sys):
    scriptName=sys.argv[0]
    custDetails=sys.argv[1]
    print('Hello :'+custDetails)
    #custDetails='Suvojeet:Pal:::809-09-8876:08/16/1984:838065'
    reqCustList=[]
    reqCustList.append(custDetails.split(':'))
    matchList=_matchFunction_(reqCustList[0][0],reqCustList[0][1],reqCustList[0][2],reqCustList[0][3],reqCustList[0][4],reqCustList[0][5],reqCustList[0][6])

    print(matchList)


if __name__ == '__main__':
    main(sys)