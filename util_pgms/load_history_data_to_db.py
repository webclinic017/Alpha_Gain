import pandas as pd
from utils import util_functions as util
from utils import broker_api_functions as baf
import logging
import datetime
from sqlalchemy import create_engine

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

    programName = configDict['BT_PATTERNS_BASED_ENTRY_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(programName) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']
    
    currDate = util.get_date_time_formatted("%Y-%m-%d")

    # Connect to Kite ST
    kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = configDict['ADMIN_TRADE_BROKER'])    
    

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isKiteConnected):          
        
       
        try:
            reportDate = util.get_system_date()
            instList = util.get_all_fno_inst_list(mySQLCursor, limitCnt=200)
            interval = '15minute'
            fromDate = util.get_bt_from_date_based_interval(interval, dateNow=reportDate)
            toDate = reportDate   
            instListLocal = []

            engine = create_engine('mysql+mysqlconnector://core_pgm_prd:AyNX9CMjEb32zbZt~@alphagain.in:3306/PROD', echo=False)
            
            startTime=datetime.datetime.now()
            logging.info(f"Started at {startTime}") 

            for instRow in instList:                      
                df['INTERVAL_TIME'] = interval
                df['STOCK_NAME'] = instRow[2]
                instrumentToken = instRow[0]
                histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, interval)               
                df = pd.DataFrame(histRecords)
                df.date = df.date + pd.Timedelta('05:30:00')                
                             
                df['STOCK_NAME'] = util.get_date_time_formatted("%d-%m-%Y %H:%M:%S")
                df.to_sql(name='HISTORICAL_STOCK_DATA', con=engine, if_exists = 'append', index=False)                    
                endTime = datetime.datetime.now()

            logging.info(f"Finished in {endTime - startTime}")
            

        except Exception as e:
            alertMsg = "Unable to load the historical data (main block): " + str(e)
            logging.info(alertMsg)
     

    util.logger(logEnableFlag, "info", "Program ended")