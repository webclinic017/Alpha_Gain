from importlib.resources import path
import matplotlib.pyplot as plt
import pandas as pd
from scipy.signal import argrelextrema

from utils import util_functions as util

from utils import broker_api_functions as baf

from scipy.stats import linregress
import logging
import requests
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


# Main function is called by default, and the first function to be executed
if __name__ == "__main__":

    # Read the system configration file that contains logs informations, and telegram ids
    configFileH = open('conf/config.ini')
    configList = configFileH.readlines()
    configFileH.close()
    configDict = {}

    # Store the configuraiton files in a dictionary for reusablity. 
    for configItem in configList:
        configItem = configItem.strip('\n').strip('\r').split('=')
        if(len(configItem) > 1):            
            configDict[str(configItem[0])] = configItem[1]    
    
    logEnableFlag = True if configDict['ENABLE_LOG_FLAG'] == 'True' else False 
    testingFlag = True if configDict['TESTING_FLAG'] == 'True' else False
    
    TELG_ADMIN_ID = configDict['TELG_ADMIN_ID']

    programName = configDict['PATTERNS_BASED_ENTRY_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['PATTERNS_BASED_ENTRY_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']
    
    currDate = util.get_date_time_formatted("%Y-%m-%d")

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = configDict['ADMIN_TRADE_BROKER'])    
    
    exitAllPosFlag = False
    
    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isKiteConnected):           
       
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)
        
        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            # Verify whether the connection to MySQL database is open
            cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)
            
            sysSettings = util.load_constant_variables(mySQLCursor, 'SYS_SETTINGS')
            currentTime = util.get_date_time_formatted("%H%M")
            
            if (testingFlag or ((int(currentTime) >= int(sysSettings['SYSTEM_START_TIME'])) and (int(currentTime) <= int(sysSettings['SYSTEM_END_TIME'])))):
                try:

                    instList = util.get_all_fno_inst_list(mySQLCursor)  
                    interval =  '5minute'
                    patternSignalList = []
                    start=datetime.datetime.now()
                    for instRow in instList:
                        instrumentToken = instRow[0]
                        
                        patternSignalList = util.get_advance_decline_ratio(kite, instrumentToken, interval, patternSignalList)
                    
                    sellCount = patternSignalList.count('Sell')
                    buyCount = patternSignalList.count('Buy')
                    totalCount = len(patternSignalList)

                    buyRatio = ( buyCount / totalCount ) * 100
                    sellRatio = ( sellCount / totalCount ) * 100

                    print(f"{buyRatio}")
                    print(f"{sellRatio}")
                    end = datetime.datetime.now()
                    print (f"finished in {end - start}")

                except Exception as e:
                    alertMsg = "ERROR: Failed in main block: " + str(e)
                    logging.info(alertMsg)

            elif (int(currentTime) > int(sysSettings['SYSTEM_END_TIME'])):           
                programExitFlag = 'Y'
            
            # util.update_program_running_status(cnx, mySQLCursor,programName, 'ACTIVE')
            util.disconnect_db(cnx, mySQLCursor)


    util.logger(logEnableFlag, "info", "Program ended")
