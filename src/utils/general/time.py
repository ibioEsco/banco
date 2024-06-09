from datetime import datetime
import pytz

def timeStamp():
    return datetime.now(pytz.timezone('America/Bogota')).strftime('%Y-%m-%dT%H:%M:%S')

