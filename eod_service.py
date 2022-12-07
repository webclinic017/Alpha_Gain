#Import required python libraries
import logging
from config import Config
from utils import util_functions as util
from utils import broker_api_functions as baf
from utils import trade_scoring_copy as tsc
import requests
import os
import pipes
import base64
# from github import Github
# from github import InputGitTreeElement
import datetime
from datetime import timedelta
import pandas as pd


# # Delete all the pending and open signals at the end of day.
# def delete_pending_signals(cnx, mySQLCursor):
#     try:
#         sql = "DELETE FROM TRADE_SIGNALS WHERE SIGNAL_STATUS IN ('PENDING', 'OPEN')"
#         mySQLCursor.execute(sql)
#         cnx.commit()
#     except Exception as e:
#         logging.info("DB FAILURE: UNABLE TO DELETE DATA FROM TRADE_SIGNALS : "+str(e))

# Make all the pending and open signals as expired at the end of day.
def update_pending_signals(cnx, mySQLCursor): 
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)

    response = {}
    try:
        updateQuery = "UPDATE TRADE_SIGNALS SET SIGNAL_STATUS = 'EXPIRED' WHERE SIGNAL_STATUS IN ('PENDING', 'OPEN')"
        mySQLCursor.execute(updateQuery)
        cnx.commit()
        response['status'] = 'success'
        response['remarks'] = 'SIGNAL_STATUS is set as expired for all the pending and open signals'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to make the SIGNAL_STATUS as expired. The error is, ' + str(e)

    return response        


def invalidate_token_keys(): 
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.connect_mysql_db()
    updateOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S") 
    response = {}
    try:
        updateQuery = f"UPDATE USR_TRADE_ACCOUNTS SET ACCESS_TOKEN_VALID_FLAG = 'N', UPDATED_ON='{updateOn}' WHERE ACCESS_TOKEN_VALID_FLAG = 'Y'"
        mySQLCursor.execute(updateQuery)
        cnx.commit()
        response['status'] = 'success'
        response['remarks'] = 'ACCESS_TOKEN_VALID_FLAG is set as invalid for all users'
    except Exception as e:
        response['status'] = 'failed'
        response['remarks'] = 'Unable to make the ACCESS_TOKEN_VALID_FLAG as invalid. The error is, ' + str(e)
    return response

# MySQL database details to which backup to be done. Make sure below user having enough privileges to take databases backup.
def backup_mysql_db_github():    
    response = {}
    try:
        BACKUP_PATH = '/apps/backup/database'
        # BACKUP_PATH = 'D:/alphagain_development'
        
        
        # Getting current DateTime to create the separate backup folder like "20180817-123433".
        currDate = util.get_date_time_formatted("%d%m%Y")
        
        # Checking if backup folder already exists or not. If not exists will create it.
        try:
            os.stat(BACKUP_PATH)
        except:
            os.mkdir(BACKUP_PATH)
        
        # Code for checking if you want to take single database backup or assinged multiple backups in DB_NAME.
        print ("Starting backup of database " + Config.DB_DATABASE)
        
        # Starting actual database backup process.
        backupFileName = Config.DB_DATABASE + "_" + currDate + ".sql"

        dumpcmd = f"mysqldump --column-statistics=0 -h {Config.DB_HOST} -u  {Config.DB_USER_ID} -p{Config.DB_PASSWORD} {Config.DB_DATABASE} >  {pipes.quote(BACKUP_PATH)}/{backupFileName}"
        os.system(dumpcmd)

        gzipcmd = "gzip " + pipes.quote(BACKUP_PATH) + "/" + backupFileName
        os.system(gzipcmd)
        
        response['status'] = 'success'
        response['remarks'] = "DB backup have been created in '" + BACKUP_PATH + "' directory"


        # gitUser = configDict['GIT_USER']
        # gitPassword = configDict['GIT_PASSWORD']
        # g = Github(login_or_token=gitUser, password=gitPassword)        
        # authRepo = g.get_user()
        # repo = authRepo.get_repo('db_backup') # repo name

        # file_list = [
        #     '/apps/backup/database/'+ backupFileName
        # ]
        # file_names = [
        #     backupFileName
        # ]
        # commit_message = 'Updated the DB on ' + currDate
        # master_ref = repo.get_git_ref('heads/master')
        # master_sha = master_ref.object.sha
        # base_tree = repo.get_git_tree(master_sha)

        # element_list = list()
        # for i, entry in enumerate(file_list):
        #     with open(entry) as input_file:
        #         data = input_file.read()
            
        #     element = InputGitTreeElement(file_names[i], '100644', 'blob', data)
        #     element_list.append(element)

        # tree = repo.create_git_tree(element_list, base_tree)
        # parent = repo.get_git_commit(master_sha)
        # commit = repo.create_git_commit(commit_message, tree, [parent])
        # master_ref.edit(commit.sha)
        # response['status'] = 'success'
        # response['remarks'] = "DB backup have been created in '" + BACKUP_PATH + "' directory"

    except Exception as e:
        response['status'] = 'error'
        response['remarks'] = 'Unable to create DB backup. The error is, ' + str(e)

    return response

def refresh_dev_database():    
    response = {}
    try:
        # Connect to MySQL database
        try:
            cnx, mySQLCursor = util.connect_mysql_db()
            updateQuery = "DROP DATABASE IF EXISTS DEV"
            mySQLCursor.execute(updateQuery)
            cnx.commit()
        except:
            cnx, mySQLCursor = util.connect_mysql_db()
            pass
        
        cnx, mySQLCursor = util.connect_mysql_db()
        updateQuery = "CREATE DATABASE DEV"
        mySQLCursor.execute(updateQuery)
        cnx.commit()

        dumpcmd = f"mysqldump -h {Config.DB_HOST} -u {Config.DB_USER_ID}  -p{Config.DB_PASSWORD} PROD | mysql -h {Config.DB_HOST} -u {Config.DB_USER_ID}  -p{Config.DB_PASSWORD} DEV"
        os.system(dumpcmd)
        response['status'] = 'success'
        response['remarks'] = "The development database has been refreshed with prod copy"

    except Exception as e:
        response['status'] = 'error'
        response['remarks'] = 'Unable to create DB backup. The error is, ' + str(e)

    return response


def get_mean_reversal_instruments(kite, cnx, mySQLCursor, strategyId, tradeAccount):
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)
    updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
    toDate = util.get_lookup_date(0)
    fromDate = util.get_from_date_based_interval('day')
    response = {}
    try:
        # # Before inserting the update instruments list, delete all the instruments first  
        # deleteQuery = f"DELETE FROM USER_DEFINED_INSTRUMENTS WHERE STRATEGY_ID ='{strategyId}'"
        # mySQLCursor.execute(deleteQuery)
        # cnx.commit()
        
        instList = util.get_all_fno_inst_list(mySQLCursor, limitCnt=200)
        
        for instRow in instList:
            instrumentToken = instRow[0]
            tradeSymbol = instRow[1]                            
            stockName = instRow[2]
            histRecords = baf.get_historical_data(kite, instrumentToken, fromDate, toDate, 'day')
            df = pd.DataFrame(histRecords)
            op = df['open']
            hi = df['high']
            lo = df['low']
            cl = df['close']
            vol = df['volume']
            
            df = tsc.get_ti_for_bullish_reversal(df, op, hi, lo, cl, vol)   
            lastRec = df.tail(1)
            roc = lastRec['ROC1'].values[0]
            rsiBuySellSignal = tsc.get_RSI2_buy_sell_signal(lastRec)            
            
            if (rsiBuySellSignal == 'Sell' or rsiBuySellSignal == 'Buy'):
                logging.info(f"{stockName}: {rsiBuySellSignal}")
                print(f"{stockName}: {rsiBuySellSignal}")
                # insertVal = []
                # insertQuery = f"INSERT INTO USER_DEFINED_INSTRUMENTS (STRATEGY_ID, TRADE_ACCOUNT, INSTRUMENT_TOKEN, TRADING_SYMBOL, STOCK_NAME, UPDATED_ON, RE_ENTRY_FLAG, BUY_SELL, ROC) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # insertVal.insert(0, str(strategyId))
                # insertVal.insert(1, str(tradeAccount))
                # insertVal.insert(2, str(instrumentToken))
                # insertVal.insert(3, str(tradeSymbol))
                # insertVal.insert(4, str(stockName))
                # insertVal.insert(5, str(updatedOn))       
                # insertVal.insert(6, str('YES'))       
                # insertVal.insert(7, str(rsiBuySellSignal))
                # insertVal.insert(8, str(roc))
                # mySQLCursor.execute(insertQuery, insertVal)
                # cnx.commit()

        response['status'] = 'success'
        response['remarks'] = "Next Day\'s instruments for all strategy has been added"

    except Exception as e:
        response['status'] = 'error'
        response['remarks'] = 'Unable to insert get_mean_reversal_instruments, ' + str(e)

    return response

def add_next_day_instruments(cnx, mySQLCursor):
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor)
    updatedOn = util.get_date_time_formatted("%Y-%m-%d %H:%M:%S")
    
    # Get next trading date
    nextTradingDate = util.get_next_trading_date(mySQLCursor)
    response = {}
    try:
        for row in util.get_active_strategy_id(mySQLCursor):
            strategyId = row[0]
            # Get the list of stocks for long term trading, based on sector and stocks ranking
            instList = util.get_dynamic_insturments_list(mySQLCursor, strategyId)
            for instRow in instList:
                instrumentToken = instRow[0]
                tradeSymbol = instRow[1]                            
                stockName = instRow[2]         
                mptRefId = instRow[4]         
                insertVal = []

                insertQuery = "INSERT INTO SHORT_LISTED_INSTRUMENTS(DATE, STRATEGY_ID, INSTRUMENT_TOKEN, TRADING_SYMBOL, \
                                STOCK_NAME, MPT_ID, UPDATED_ON) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                
                insertVal.insert(0, str(nextTradingDate))       
                insertVal.insert(1, str(strategyId))
                insertVal.insert(2, str(instrumentToken))
                insertVal.insert(3, str(tradeSymbol))
                insertVal.insert(4, str(stockName))
                insertVal.insert(5, str(mptRefId))
                insertVal.insert(6, str(updatedOn))       
                mySQLCursor.execute(insertQuery, insertVal)
                cnx.commit()
        response['status'] = 'success'
        response['remarks'] = "Next Day\'s instruments for all strategy has been added"

    except Exception as e:
        response['status'] = 'error'
        response['remarks'] = 'Unable to insert add_next_day_instruments, ' + str(e)

    return response



# Main function is called by default, and the first function to be executed
if __name__ == "__main__":   # __name__ is a built-in variable which evaluates to the name of the current module 
    
    telegramAdminIds = Config.TELG_ADMIN_ID
    adminTradeAccount = Config.ADMIN_TRADE_ACCOUNT
    adminTradeBroker = Config.ADMIN_TRADE_BROKER
    programName = os.path.splitext(os.path.basename(__file__))[0]
    sysDict = {}
    sysDict['programName'] = programName
    sysDict['telegramAdminIds'] = telegramAdminIds

    # Initialized the log files 
    util.initialize_logs(str(programName) + '.log')

    programExitFlag = True

    programExitFlag = 'N'
    
    currDate = util.get_date_time_formatted("%Y-%m-%d")

    #Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db()

    response = backup_mysql_db_github()
    util.add_logs(cnx, mySQLCursor, 'UPDATE', response['remarks'], sysDict)

    response = refresh_dev_database()    
    util.add_logs(cnx, mySQLCursor, 'UPDATE', response['remarks'], sysDict)

    currentTime = util.get_date_time_formatted("%H%M")
    if ( int(currentTime) >= 1629 ):
        response = invalidate_token_keys()
        util.add_logs(cnx, mySQLCursor, 'UPDATE', response['remarks'], sysDict)

    # response = update_pending_signals(cnx, mySQLCursor)
    # util.add_logs(cnx, mySQLCursor, 'UPDATE', response['remarks'], sysDict)

    # response = add_next_day_instruments(cnx, mySQLCursor, configDict)
    # util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', response['remarks'], telgUpdateFlag='N', programName=programName)

    # Connect to Kite ST
    # kite, isKiteConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = adminTradeBroker)
    # response = get_mean_reversal_instruments(kite, cnx, mySQLCursor, 'ALPHA_1A', 'R0428')
    # util.add_logs(cnx, mySQLCursor, 'UPDATE', response['remarks'], sysDict)

    util.add_logs(cnx, mySQLCursor, 'ALERT', f'Program {programName} completed', sysDict)
    
    util.disconnect_db(cnx, mySQLCursor)