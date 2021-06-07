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
import fuzzymatcher
import traceback

class MatchProcessing:

    config = configparser.ConfigParser()
    config.read('/Users/suvojeetpal/git/customerdatahub-git/CustomerManagement/src/main/resources/perweight.ini')
    
    configNickNames = configparser.ConfigParser()
    configNickNames.read('/Users/suvojeetpal/git/customerdatahub-git/CustomerManagement/src/main/resources/perweight.ini')
    
    
    
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
    
    iselect_query="""select concat_ws(":",cui,first_name,last_name,IFNULL(suffix,''),IFNULL(gender,''),IFNULL(REGEXP_REPLACE(cust_external_id, '[^[:alnum:]]+', ''),''),IFNULL(dob,'')) AS 'MatchingList'
             FROM CUSTOMERHUB.Customer WHERE bucket_hash_key between %s and %s;"""
    
    
    iselect_addr="""select cui,Replace(addr.addr_line_one,',',' ') as addr_line_one,IFNULL(addr.addr_line_two,'') as addr_line_two,addr.city,IFNULL(addr.state,'') as state,addr.postal_code,addr.country
             from CUSTOMERHUB.Customer cust, CUSTOMERHUB.Address addr where addr.cust_id=cust.cust_id and cust.cui=%s;""";

    iselect_ssn="""select concat_ws(":",cui,first_name,last_name,IFNULL(suffix,''),IFNULL(gender,''),IFNULL(REGEXP_REPLACE(cust_external_id, '[^[:alnum:]]+', ''),''),IFNULL(dob,'')) AS 'MatchingList'
            FROM CUSTOMERHUB.Customer WHERE cust_external_id=%s;"""  
       
    def _getDBConnection_(self):
        from mysql.connector import pooling
        
        try:
            connection_pool=pooling.MySQLConnectionPool(pool_name='pynative_pool',pool_size=1,
                                                   pool_reset_session=True,host='greentechdbdev.ca3wll1ihwpn.us-east-1.rds.amazonaws.com',
                                                   database='CUSTOMERHUB',user='root', password='root1234')
    
            connection_objt = connection_pool.get_connection()
            
        except:
            print('Pool is exhausted')
    
        return connection_objt;
    
    def _matchCustomerName_(self,reqCustName,dbReqName):
    
    
        soundex = fuzzy.Soundex(4)
    
        reqSndexMyName=soundex(reqCustName)
        reqNysiisName=fuzzy.nysiis(reqCustName)
    
        dbSndexMyName=soundex(dbReqName)
        dbNysiisName=fuzzy.nysiis(dbReqName)
    
        if reqCustName == dbReqName:
                namescore= self.exactName
        else :
            if reqSndexMyName == dbSndexMyName:
                namescore=self.phoneticName
            elif reqNysiisName == dbNysiisName:
                namescore=self.nysciisName
            else:
                namescore=0
    
        return int(namescore)
    
    
    def _matchCustomerId_(self,reqcustId,dbcustId):
    
        if reqcustId is not None and dbcustId is not None:
            identScore=0
            idEDist= editdistance.eval(re.sub("[^0-9]", "", reqcustId),re.sub("[^0-9]", "", dbcustId))
            
            if  idEDist == 0 :
                identScore = self.IDSSNExact
            elif idEDist == 1:
                identScore = self.IDSSN1DIFF
            elif idEDist == 2:
                identScore = self.IDSSN2DIFF
            elif idEDist == 3:
                identScore = self.IDSSN3DIFF
            elif idEDist == 4:
                identScore = self.IDSSN4DIFF
            elif idEDist == 5:
                identScore = self.IDSSN5DIFF
            elif idEDist == 6:
                identScore = self.IDSSN6DIFF
            elif idEDist == 7:
                identScore = self.IDSSN7DIFF
            elif idEDist == 8:
                identScore = self.IDSSN8DIFF
            else:
                identScore = 0
    
            return int(identScore)
    
    def _matchCustomerDateDOB_(self,reqDOBdate,dbDOBdate):
    
        if reqDOBdate is not None and dbDOBdate is not None:
            dobScore=0
            dobEditDst= editdistance.eval(re.sub("[^0-9]", "", reqDOBdate),re.sub("[^0-9]", "", dbDOBdate))
    
            if  dobEditDst == 0 :
                dobScore = self.exactDob
            elif reqDOBdate[0:2] == dbDOBdate[2:4] and reqDOBdate[4:8] == dbDOBdate[4:8] :
                dobScore = self.ddmmswap
            else:
                dobScore = self.dobDiff
    
            return int(dobScore)
    
    def _matchCustomerGender_(self,reqGender,dbGender):
    
        if reqGender is not None and dbGender is not None:
            genderScore=0
            if reqGender == dbGender :
                genderScore = self.exactGender
            else:
                genderScore = self.diffGender
    
            return int(genderScore)
    
    def _matchCustomerSuffix_(self,reqcustSuffix,dbcustSuffix):
    
        if reqcustSuffix is not None and dbcustSuffix is not None:
            suffixScore=0
            if reqcustSuffix == dbcustSuffix :
                suffixScore = self.suffixMatch
            else:
                suffixScore = self.suffixMisMatch
    
            return int(suffixScore)
    
    
    def _matchCustomerAddress_(self,reqAddress,ccid):
        
        addressScore=0
        cutReqAddr=[]
       
        tmpccid="111111111111111"
        left_on = ['AddressLineOne', 'AddressLineTwo','City_Name','State','PostalCode','Country']
        right_on = ['DBAddressLineOne','DBAddressLineTwo','DBCity_Name','DBState','DBPostalCode','DBCountry']
        cutReqAddr.append((tmpccid+","+reqAddress).split(","))
        custAllDBAddr=[]
        mydb = self._getDBConnection_()  
        addressScoreBoast=50
        #ccid=2888087888390197660
        if reqAddress is not None :
            df_Addr=pd.read_sql(self.iselect_addr, con=mydb ,params=(ccid,),chunksize=1000)
            for i,addr in enumerate(df_Addr):
                fulladdr=str(addr['cui'].iloc[i])+","+str(addr['addr_line_one'].iloc[i])+","+str(addr['addr_line_two'].iloc[i])+","+str(addr['city'].iloc[i])+","+str(addr['state'].iloc[i])+","+str(addr['postal_code'].iloc[i])+","+str(addr['country'].iloc[i])
                custAllDBAddr.append(fulladdr.split(","))
       
        if custAllDBAddr != []:
            reqAddress=pd.DataFrame(cutReqAddr,columns = ['Reqccid','AddressLineOne','AddressLineTwo','City_Name','State','PostalCode','Country'])
            dbAddress=pd.DataFrame(custAllDBAddr,columns = ['DBccId','DBAddressLineOne','DBAddressLineTwo','DBCity_Name','DBState','DBPostalCode','DBCountry'])
       
        #print (reqAddress)
        #print (dbAddress)
            matched_results = fuzzymatcher.fuzzy_left_join(reqAddress,
                                                          dbAddress,
                                                          left_on,
                                                          right_on,
                                                          left_id_col='Reqccid',
                                                          right_id_col='DBccId')
       
            addressScore=addressScoreBoast+matched_results.best_match_score.values.item(0)*1000
            
            if(addressScore > 0) and addressScore is not None:
                return int(addressScore)
            else:
                return 0;
    
    
    
   
    def _fetchMatchRecordFromDF_(self,inputParam1,inputParam2,ssn):
        
        try:
            vaex_df=[]
            mydb = self._getDBConnection_()
            if mydb.is_connected():
                for chunk in pd.read_sql(self.iselect_query, con=mydb, params=(inputParam1,inputParam2),chunksize=100000):
                    vaex_df=vx.from_pandas(df=chunk, copy_index=False)
            if len(vaex_df) == 0:
                for chunk in pd.read_sql(self.iselect_ssn, con=mydb, params=(ssn,),chunksize=100000):
                    vaex_df=vx.from_pandas(df=chunk, copy_index=False)
                
        except:
            print('Error: Matching Engine in _fetchMatchRecordFromDF_ function')
            print(sys.exc_info()[0])
        finally:
        # closing database connection.
            if mydb.is_connected():
                  mydb.close()
                  
        if len(vaex_df) != 0:
            return vaex_df.MatchingList.values
        else:
            return vaex_df
        
    
    
    def _compareMatching_ (self,firstName,lastName,suffix,gender,ssn,dob,reqaddress,inputParam1,inputParam2):
        suspectList=[]
        
        try:
            
            custDbList=self._fetchMatchRecordFromDF_(inputParam1,inputParam2,ssn)
            parseDBcustList=[]
    
            #if custDbList is null search by ssn an address
            if custDbList is not None:
                for cust in custDbList:
                    parseDBcustList.append(str(cust).split(":"))
    
            for custRec in parseDBcustList:
    
                ssnscore=0
                totalscore=0
                addressScore=0
                #full name match need to do
                totalscore= self._matchCustomerName_(firstName,custRec[1])
                #middle name need to matcprint('suvojeet')
               
                totalscore=totalscore+self._matchCustomerName_(lastName,custRec[2])
                
                totalscore=totalscore+self._matchCustomerSuffix_(suffix,custRec[3])
                
    
                isAddrMatchingreq=False;
                
                if ssn is not None and len(ssn) > 0 :
                    ssnscore=self._matchCustomerId_(ssn,custRec[5])
                    totalscore=totalscore + ssnscore
                    
                    if int(ssnscore) == 0:
                        isAddrMatchingreq=True
                else: 
                    isAddrMatchingreq=True
    
                if  isAddrMatchingreq == False and int(ssnscore) < int(self.IDSSNExact) and int(ssnscore) >= int(self.IDSSN1DIFF):
                    isAddrMatchingreq=True
                
                totalscore=totalscore +self._matchCustomerDateDOB_(dob,custRec[6])
                
                genderScore=self._matchCustomerGender_(gender,custRec[4])
                totalscore=totalscore + genderScore
                
                #Calculating Address 
    
                if(isAddrMatchingreq):
                    addressScore=self._matchCustomerAddress_(reqaddress,custRec[0])
                if addressScore is None:
                    addressScore=0
                    
                totalscore=totalscore+addressScore
                percentileScore=totalscore/100
    
                if percentileScore > int(self.A1):
                        suspectList=[]
                        A1Match=custRec[0]+':A1~'+ str(percentileScore)
                        suspectList.append(A1Match)
                        break;
                elif percentileScore < int(self.A1) and percentileScore > int(self.B) :
                        BMatch=custRec[0]+':B~'+ str(percentileScore)
                        suspectList.append(BMatch)
                else:
                        pass
        except:
            print('Error: Matching Engine in _compareMatching_ function')
            print(sys.exc_info()[0])
            print(traceback.format_exc())
    
        return suspectList
    
 
