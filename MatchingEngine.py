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
import vaex as vx
from functools import lru_cache

config = configparser.ConfigParser()
config.read('/Users/suvojeetpal/SpringBootCloud/CustomerDataAccountHub-1/src/main/resources/perweight.ini')

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

iselect_query="""select concat_ws(":",CCID,first_name,last_name,IFNULL(suffix,''),IFNULL(gender,''),IFNULL(ssn,''),IFNULL(dob,'')) AS 'MatchingList'
         FROM CUSTOMERHUB.customer WHERE bucket_hash_key between %s and %s;"""
         
def _getDBConnection_():

    mydb=mysql.connector.connect(user='root', password='root1234',
                              host='greentechdbdev.ca3wll1ihwpn.us-east-1.rds.amazonaws.com',
                              database='CUSTOMERHUB')
    return mydb;

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

    if reqcustId is not None and dbcustId is not None:
        identScore=0
        idEDist= editdistance.eval(re.sub("[^0-9]", "", reqcustId),re.sub("[^0-9]", "", dbcustId))

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


def _matchCustomerAddress_(reqAddress,dbAddress):
    addressScore=0
    if reqAddress is not None :
        return int(addressScore)

@lru_cache(maxsize=100000)
def _fetchMatchRecordFromDF_(inputParam1,inputParam2):
    
    mydb = _getDBConnection_()
   
    for chunk in pd.read_sql(iselect_query, con=mydb, params=(inputParam1,inputParam2),chunksize=100000):
        vaex_df=vx.from_pandas(df=chunk, copy_index=False)
        return vaex_df.MatchingList.values

#df_sales = pd.DataFrame(np.random.randint(low = 13, high = 148, size = 918843, dtype= 'uint8'), columns =['MatchingList'])

def _compareMatching_ (firstName,lastName,suffix,gender,ssn,dob,inputParam1,inputParam2):

    custDbList=_fetchMatchRecordFromDF_(inputParam1,inputParam2)
    totalscore=0
 
    parseDBcustList=[]
    suspectList=[]
    for cust in custDbList:
        parseDBcustList.append(str(cust).split(":"))

    for custRec in parseDBcustList:
        totalscore=0
        totalscore= _matchCustomerName_(firstName,custRec[1])
        totalscore=totalscore +_matchCustomerName_(lastName,custRec[2])
        totalscore=totalscore +_matchCustomerSuffix_(suffix,custRec[3])
        totalscore=totalscore +_matchCustomerGender_(gender,custRec[4])
        if custRec[5] is not None:
            totalscore=totalscore +_matchCustomerId_(ssn,custRec[5])
        else: 
            totalscore=totalscore +_matchCustomerAddress_(address,custRec[5])
        totalscore=totalscore +_matchCustomerDateDOB_(dob,custRec[6])

        percentileScore=totalscore/100

        if percentileScore > int(A1):
                suspectList=[]
                A1Match=custRec[0]+':A1~'+ str(totalscore/100)
                suspectList.append(A1Match)
                break;
        elif percentileScore < int(A1) and percentileScore > int(B) :
                BMatch=custRec[0]+':B~'+ str(totalscore/100)
                suspectList.append(BMatch)
        else:
                print(' ')


    return suspectList

def _matchFunction_ (argv):
   
    custDetails=argv
    reqCustList=[]
    reqCustList.append(custDetails.split(':'))
    suspectList=_compareMatching_(reqCustList[0][0],reqCustList[0][1],reqCustList[0][2],reqCustList[0][3],reqCustList[0][4],reqCustList[0][5],reqCustList[0][6],reqCustList[0][7])

    return suspectList


def main(sys):
    scriptName=sys.argv[0]
    custDetails=sys.argv[1]
    matchList=_matchFunction_(sys.argv[1])

    print(matchList)


if __name__ == '__main__':
    main(sys)
