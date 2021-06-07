#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 10 14:13:26 2021

@author: suvojeetpal
"""
import sys
from MatchingEngine import MatchProcessing

class MatchingBatch:



    def _matchFunction_ (self,argv):
        custDetails=sys.argv[1]
        
        try:
            reqCustList=[]
            reqCustList.append(custDetails.split(':'))
            #print(reqCustList[0][0])
            engineInstance=MatchProcessing()
    
            suspectList=engineInstance._compareMatching_(reqCustList[0][0],reqCustList[0][1],reqCustList[0][2],reqCustList[0][3],reqCustList[0][4],reqCustList[0][5],
                                          reqCustList[0][6],reqCustList[0][7],reqCustList[0][8])
            
        except Exception:
            print('Error: Matching Engine in _matchFunction_ function')
            #print(traceback.format_exc())
            print(sys.exc_info()[0])
        return suspectList


    def main(self,req):
        matchList=self._matchFunction_(req)
        print(matchList)


    
 
class_instance = MatchingBatch()
class_instance.main(str(sys.argv))

