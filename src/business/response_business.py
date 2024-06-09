from src.utils.general.time import timeStamp
import json

class ResponseBusiness: 

    def __init__(self, statusCode, statusMessage, payload): 
        self.__statusCode = statusCode
        self.__statusMessage = statusMessage
        self.__payload = payload

    def getResponse(self):
        payloadResponse = {
            "data": self.__payload,
            "time": timeStamp(),
            "statusMessage": self.__statusMessage
        }

        return {
            "statusCode": self.__statusCode,
            "body": json.dumps(payloadResponse)
        }