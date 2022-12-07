from NorenRestApiPy.NorenApi import NorenApi
from threading import Timer
import pandas as pd
import time
import concurrent.futures

api = None
class StarApiPy(NorenApi):
    
    def __init__(self, *args, **kwargs):
        super(StarApiPy, self).__init__(host='https://starapi.prostocks.com/NorenWClientTP', websocket='wss://starapi.prostocks.com/NorenWS/', eodhost='https://star.prostocks.com/chartApi/getdata')
        global api
        api = self