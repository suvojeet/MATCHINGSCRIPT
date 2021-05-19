
from flask import Flask,request
from flask_restful import Resource,Api
from MatchingEngine import MatchProcessing
import json

app = Flask(__name__)
api = Api(app)

class MatchingEngine(Resource):
    
    
        def post(self,matchparam=""):
            req_json = request.get_json(cache=True)
            #matchparam=''
            #dictPosition = 1
            print(req_json)
            print(req_json['firstname'])
            #print(matchparam)
            engineInstance=MatchProcessing()
            suspectList=engineInstance._compareMatching_(req_json['firstname'],req_json['lastName'],req_json['suffix'],
                                                         req_json['gender'],req_json['ssn'],req_json['dob'],req_json['address'],req_json['inputparam1'],req_json['inputparam2'])
                
            jsonStr = json.dumps(suspectList)
           
            return {'Suspect': jsonStr}, 201

        
api.add_resource(MatchingEngine,'/matchingengine',methods=['POST'])
#api.add_resource(Multi,'/multi/<int:num>')

if __name__ =='__main__':
   app.run(debug=True)
