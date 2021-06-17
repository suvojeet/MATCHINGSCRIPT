from flask import Blueprint,request
from MatchingEngine import MatchProcessing
import json

service = Blueprint('service', __name__)

@service.route('/matchingengine', methods=['POST'])
def match():
    req_json = request.get_json(cache=True)
    print("Hello")
    print(req_json['firstname'])
    engine = MatchProcessing()
    suspects = engine._compareMatching_(req_json['firstname'],req_json['lastName'],req_json['suffix'],
                                                 req_json['gender'],req_json['custExternalId'],req_json['dob'],req_json['address'],req_json['inputparam1'],req_json['inputparam2'])
    jsonstr = json.dumps(suspects)

    return {'Suspect': jsonstr}, 200
