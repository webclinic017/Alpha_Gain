# from pypf.chart import PFChart
from pypf.instrument import YahooSecurity
from utils import util_functions as util
from utils import broker_api_functions as baf
import datetime
import numpy as np
import logging
# import required modules
import json
import pandas as pd
from bson import json_util


"""Classes to generate point and figure charts."""
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal

import logging
import pypf.terminal_format



class HistoricalData():
    """Security instrument that uses Yahoo as the datasource."""

    def __init__(self, symbol, exchangeToken=None, interval='1d', period=10):
        """Initialize the security."""

        self.historical_data = OrderedDict()
        self.interval = interval
        self.period = period
        self.symbol = symbol
        self.exchangeToken = exchangeToken
        self.fromDate = util.get_lookup_date(300)
        self.toDate = util.get_lookup_date(0)
        

    def get_historical_data(self, apiConnObj, adminTradeBroker):
        interval = 'day'
        niftyHistRecords = baf.get_historical_data(apiConnObj, self.symbol, self.fromDate, self.toDate, interval, broker=adminTradeBroker, exchangeToken=self.exchangeToken)  
        jsonData = json.dumps(niftyHistRecords, indent=4, default=json_util.default)
        for data in json.loads(jsonData):
                    
            date = int(str(data['date']['$date'])[:10])
            date = datetime.fromtimestamp(date).strftime('%Y-%m-%d %H:%M')
            # date = datetime.strptime(date, "%Y-%m-%d").date()
            tmpData = {}
            tmpData['Date'] = date
            tmpData['Open'] = Decimal(data['open'])
            tmpData['High'] = Decimal(data['high'])
            tmpData['Low'] = Decimal(data['low'])
            tmpData['Close'] = Decimal(data['close'])
            tmpData['Volume'] = Decimal(data['volume'])
            
            self.historical_data[str(date)] = tmpData


class PFChart(object):
    """Base class for point and figure charts."""

    TWOPLACES = Decimal('0.01')

    def __init__(self, security, box_size=.01, duration=1, method='HL',
                 reversal=3, style=False, trend_lines=False,
                 debug=False):
        """Initialize common functionality."""
        self.security = security
        self.method = method
        self.duration = duration
        self.box_size = Decimal(box_size)
        self.reversal = int(reversal)
        self.trend_lines = trend_lines
        self.style = style

        self.historical_data = []
        self.chart_data = []
        self.scale = OrderedDict()

        self.current_date = ''
        self.current_open = ''
        self.current_high = ''
        self.current_low = ''
        self.current_close = ''

        self.open_field = None
        self.high_field = None
        self.low_field = None
        self.close_field = None
        self.volume_field = None
        self.date_field = None

        self.current_signal = None
        self.current_status = None
        self.current_action = None
        self.current_move = None
        self.current_column_index = None
        self.current_scale_index = None
        self.current_scale_value = None
        self.current_direction = None
        self.current_close = None
        self._support_lines = []
        self._resistance_lines = []

        self.chart_meta_data = OrderedDict()

        self._chart = ''

        self._log = logging.getLogger(self.__class__.__name__)
        if debug is True:
            self._log.setLevel(logging.DEBUG)
            self._log.debug(self)

    @property
    def chart(self):
        """Get the chart."""
        return self._chart

    @chart.setter
    def chart(self, value):
        self._chart = value

    def create_chart(self, dump=False):
        """Populate the data and create the chart."""
        self._set_historical_data()
        self._set_price_fields()
        self._set_scale()
        self._set_chart_data()

        # with open('mycsvfile.csv', 'w') as f:  
        headerCompleted = True
        # Open a file with access mode 'a'
        file_object = open('sample.csv', 'a')
        
        prev_trend_reversal = ''
        prev_trend_date_value = ''
        box_count = 0
        for data in self.chart_meta_data:
            if (self.chart_meta_data[data]['trend_reversal'] == 'UTR' and self.chart_meta_data[data]['trend_reversal'] != 'DTR'):
                if (self.chart_meta_data[data]['trend_reversal'] == 'UTR'):
                    box_count = 0
                    prev_trend_date_value = ''
                box_count = box_count + int(self.chart_meta_data[data]['move'])                
                prev_trend_date_value = data
                prev_trend_reversal = 'UTR'
                
                self.chart_meta_data[prev_trend_date_value]['utr_resistance'] = self.chart_meta_data[data]['prior_high']

                if (prev_trend_date_value != '' and box_count != 0):
                    self.chart_meta_data[prev_trend_date_value]['box_count'] = box_count    
                if (int(self.chart_meta_data[prev_trend_date_value]['box_count']) >= 15):
                    self.chart_meta_data[prev_trend_date_value]['anchor_column'] = 'Y'

            elif (self.chart_meta_data[data]['trend_reversal'] == 'DTR' and self.chart_meta_data[data]['trend_reversal'] != 'UTR'):
                if (self.chart_meta_data[data]['trend_reversal'] == 'DTR'):
                    box_count = 0
                    prev_trend_date_value = ''                
                box_count = box_count + int(self.chart_meta_data[data]['move'])                
                prev_trend_date_value = data
                prev_trend_reversal = 'DTR'
                
                self.chart_meta_data[prev_trend_date_value]['dtr_support'] = self.chart_meta_data[data]['prior_low']

                if (prev_trend_date_value != '' and box_count != 0):
                    self.chart_meta_data[prev_trend_date_value]['box_count'] = box_count                    
                if (int(self.chart_meta_data[prev_trend_date_value]['box_count']) >= 15):
                    self.chart_meta_data[prev_trend_date_value]['anchor_column'] = 'Y'


            elif (self.chart_meta_data[data]['trend_reversal'] == '' and (prev_trend_reversal == 'UTR' or prev_trend_reversal == 'DTR')):
                box_count = box_count + int(self.chart_meta_data[data]['move'])  
                if (prev_trend_date_value != '' and box_count != 0):
                    self.chart_meta_data[prev_trend_date_value]['box_count'] = box_count         
                
                if (int(self.chart_meta_data[prev_trend_date_value]['box_count']) >= 15):
                    self.chart_meta_data[prev_trend_date_value]['anchor_column'] = 'Y'

            else:
                if (prev_trend_date_value != '' and box_count != 0):
                    self.chart_meta_data[prev_trend_date_value]['box_count'] = box_count

                    if (int(self.chart_meta_data[prev_trend_date_value]['box_count']) >= 15):
                        self.chart_meta_data[prev_trend_date_value]['anchor_column'] = 'Y'

        file_object.write("date, signal , status, action, move, column_index, scale_index, scale_value, direction, prior_high, prior_low, \
            close_price, trend_reversal, box_count, utr_resistance, dtr_support, anchor_column\n")
        
        for data in self.chart_meta_data:
               
            file_object.write(str(data) +","+ str(self.chart_meta_data[data]['signal']) + ","+ str(self.chart_meta_data[data]['status']) +"," \
                    + str(self.chart_meta_data[data]['action']) + ","+ str(self.chart_meta_data[data]['move']) +"," \
                        + str(self.chart_meta_data[data]['column_index']) + ","+ str(self.chart_meta_data[data]['scale_index']) +"," \
                           + str(self.chart_meta_data[data]['scale_value']) + ","+ str(self.chart_meta_data[data]['direction']) +"," \
                             + str(self.chart_meta_data[data]['prior_high']) + ","+ str(self.chart_meta_data[data]['prior_low']) + "," \
                                 + str(self.chart_meta_data[data]['current_close_price']) + "," + str(self.chart_meta_data[data]['trend_reversal']) + "," 
                                 + str(self.chart_meta_data[data]['box_count']) + "," + str(self.chart_meta_data[data]['utr_resistance']) + "," 
                                 + str(self.chart_meta_data[data]['dtr_support']) + "," + str(self.chart_meta_data[data]['anchor_column']) +  "\n")

        file_object.close()

        self.chart = self._get_chart()
        if dump:
            print(self.chart)

    def _get_chart(self):
        self._set_current_state()
        chart = ""
        chart += "\n"
        chart += self._get_chart_title()

        index = len(self.chart_data[0]) - 1
        first = True
        scale_right = None
        while index >= 0:
            for column in self.chart_data:
                if index in column:
                    if first:
                        scale_value = column[index]
                        if index == self.current_scale_index:
                            scale_left = (self._style('red',
                                          self._style('bold', '{:7.2f}'))
                                          .format(scale_value))
                            scale_right = (self._style('red',
                                           self._style('bold', '<< '))
                                           + self._style('red',
                                                         self._style('bold',
                                                                     '{:.2f}'))
                                           .format(self.current_close))
                        else:
                            scale_left = '{:7.2f}'.format(scale_value)
                            scale_right = '{:.2f}'.format(scale_value)
                        chart = chart + scale_left + '| '
                        first = False
                    else:
                        chart = chart + ' ' + column[index][0]
                else:
                    chart += '  '

            chart += '   |' + scale_right
            chart += "\n"
            index -= 1
            first = True
        return chart

    def _get_chart_title(self):
        self._set_current_prices()
        title = ""
        title = title + "  " + self._style('bold',
                                           self._style('underline',
                                                       self.security.symbol))
        title = (title
                 + "  ({:} o: {:.2f} h: {:.2f} l: {:.2f} c: {:.2f}"
                 .format(self.current_date, self.current_open,
                         self.current_high, self.current_low,
                         self.current_close)
                 + ")\n")

        title = (title
                 + "  "
                 + str((self.box_size * 100).quantize(PFChart.TWOPLACES))
                 + "% box, ")
        title = title + str(self.reversal) + " box reversal, "
        title = title + str(self.method) + " method\n"
        title = (title
                 + "  signal: "
                 + self._style('bold', self.current_signal)
                 + " status: " + self._style('bold', self.current_status)
                 + "\n\n")
        return title

    def _get_month(self, date_value):
        datetime_object = datetime.strptime(date_value, '%Y-%m-%d %H:%M')
        month = str(datetime_object.month)
        if month == '10':
            month = 'A'
        elif month == '11':
            month = 'B'
        elif month == '12':
            month = 'C'
        return self._style('bold', self._style('red', month))

    def _get_scale_index(self, value, direction):
        index = 0
        while index < len(self.scale):
            if self.scale[index] == value:
                return index
            elif self.scale[index] > value:
                if direction == 'x':
                    return index - 1
                else:
                    return index
            index += 1

    def _get_status(self, signal, direction):
        if signal == 'buy' and direction == 'x':
            status = 'bull confirmed'
        elif signal == 'buy' and direction == 'o':
            status = 'bull correction'
        elif signal == 'sell' and direction == 'o':
            status = 'bear confirmed'
        elif signal == 'sell' and direction == 'x':
            status = 'bear correction'
        else:
            status = 'none'
        return status

    def _set_chart_data(self):
        self._log.info('generating chart')
        self.chart_data = []
        self.chart_meta_data = OrderedDict()
        self._support_lines = []
        self._resistance_lines = []
        self.chart_data.append(self.scale)

        column = OrderedDict()
        column_index = 1
        direction = 'x'
        index = None
        month = None
        signal = 'none'
        prior_high_index = len(self.scale) - 1
        prior_low_index = 0
        box_count = ''

        for row in self.historical_data:
            day = self.historical_data[row]
            action = 'none'
            trend_reversal = ''
            move = 0
            current_month = self._get_month(day[self.date_field])
            current_close_price = day[self.close_field]
            if index is None:
                # First day - set the starting index based
                # on the high and 'x' direction
                index = self._get_scale_index(day[self.high_field], 'x')
                column[index] = ['x', day[self.date_field]]
                month = current_month
                continue

            if direction == 'x':
                scale_index = self._get_scale_index(day[self.high_field], 'x')

                if scale_index > index:
                    # new high
                    action = 'x'
                    move = scale_index - index

                    if signal != 'buy' and scale_index > prior_high_index:
                        signal = 'buy'

                    first = True
                    while index < scale_index:
                        index += 1
                        if first:
                            if current_month != month:
                                column[index] = [current_month,
                                                 day[self.date_field]]
                            else:
                                column[index] = ['x', day[self.date_field]]
                            first = False
                        else:
                            column[index] = ['x', day[self.date_field]]
                    month = current_month
                else:
                    # check for reversal
                    x_scale_index = scale_index
                    scale_index = self._get_scale_index(day[self.low_field],
                                                        'o')
                    if index - scale_index >= self.reversal:
                        # reversal
                        action = 'reverse x->o'
                        trend_reversal = 'UTR'
                        move = index - scale_index

                        if signal != 'sell' and scale_index < prior_low_index:
                            signal = 'sell'

                        prior_high_index = index
                        self._resistance_lines.append([column_index,
                                                       prior_high_index + 1])
                        self.chart_data.append(column)
                        column_index += 1
                        column = OrderedDict()
                        direction = 'o'
                        first = True
                        while index > scale_index:
                            index -= 1
                            if first:
                                if current_month != month:
                                    column[index] = [current_month,
                                                     day[self.date_field]]
                                else:
                                    column[index] = ['d', day[self.date_field]]
                                first = False
                            else:
                                column[index] = ['d', day[self.date_field]]
                        month = current_month
                    else:
                        # no reversal - reset the scale_index
                        scale_index = x_scale_index
            else:
                # in an 'o' column
                scale_index = self._get_scale_index(day[self.low_field], 'o')
                if scale_index < index:
                    # new low
                    action = 'o'
                    move = index - scale_index

                    if signal != 'sell' and scale_index < prior_low_index:
                        signal = 'sell'

                    first = True
                    while index > scale_index:
                        index -= 1
                        if first:
                            if current_month != month:
                                column[index] = [current_month,
                                                 day[self.date_field]]
                            else:
                                column[index] = ['o', day[self.date_field]]
                            first = False
                        else:
                            column[index] = ['o', day[self.date_field]]
                    month = current_month
                else:
                    # check for reversal
                    o_scale_index = scale_index
                    scale_index = self._get_scale_index(day[self.high_field],
                                                        'x')
                    if scale_index - index >= self.reversal:
                        # reversal
                        action = 'reverse o->x'
                        trend_reversal = 'DTR'
                        move = scale_index - index

                        if signal != 'buy' and scale_index > prior_high_index:
                            signal = 'buy'

                        prior_low_index = index
                        self._support_lines.append([column_index,
                                                    prior_low_index - 1])
                        self.chart_data.append(column)
                        column_index += 1
                        column = OrderedDict()
                        direction = 'x'
                        first = True
                        while index < scale_index:
                            index += 1
                            if first:
                                if current_month != month:
                                    column[index] = [current_month,
                                                     day[self.date_field]]
                                else:
                                    column[index] = ['u', day[self.date_field]]
                                first = False
                            else:
                                column[index] = ['u', day[self.date_field]]
                        month = current_month
                    else:
                        # no reversal - reset the scale_index
                        scale_index = o_scale_index

            # Store the meta data for the day
            status = self._get_status(signal, direction)
            scale_value = (self.scale[scale_index]
                           .quantize(PFChart.TWOPLACES))
            prior_high = self.scale[prior_high_index]
            prior_low = self.scale[prior_low_index]

            self._store_base_metadata(day, signal, status, action, move,
                                      column_index, scale_index, scale_value,
                                      direction, prior_high, prior_low, current_close_price, trend_reversal)
            
        self.chart_data.append(column)

        if len(self.chart_data[1]) < self.reversal:
            self.chart_data.pop(1)
            for line in self._support_lines:
                line[0] = line[0] - 1
            for line in self._resistance_lines:
                line[0] = line[0] - 1

        if self.trend_lines:
            self._set_trend_lines()

        return self.chart_data

    def _is_complete_line(self, start_point, line_type='support'):
        c_index = start_point[0]
        s_index = start_point[1]
        while c_index < len(self.chart_data):
            if s_index in self.chart_data[c_index]:
                return False
            c_index += 1
            if line_type == 'support':
                s_index += 1
            else:
                s_index -= 1
        if c_index - start_point[0] > 2:
            return True
        else:
            return False

    def _set_trend_lines(self):
        for start_point in self._support_lines:
            c_index = start_point[0]
            s_index = start_point[1]
            if self._is_complete_line(start_point, 'support'):
                while c_index < len(self.chart_data):
                    self.chart_data[c_index][s_index] = [self._style('blue',
                                                         '.'), '']
                    c_index += 1
                    s_index += 1

        for start_point in self._resistance_lines:
            c_index = start_point[0]
            s_index = start_point[1]
            if self._is_complete_line(start_point, 'resistance'):
                while c_index < len(self.chart_data):
                    self.chart_data[c_index][s_index] = [self._style('blue',
                                                         '.'), '']
                    c_index += 1
                    s_index -= 1

    def _set_current_prices(self):
        day = next(reversed(self.historical_data))
        current_day = self.historical_data[day]
        self.current_date = current_day[self.date_field]
        self.current_open = (current_day[self.open_field]
                             .quantize(PFChart.TWOPLACES))
        self.current_high = (current_day[self.high_field]
                             .quantize(PFChart.TWOPLACES))
        self.current_low = (current_day[self.low_field]
                            .quantize(PFChart.TWOPLACES))
        self.current_close = (current_day[self.close_field]
                              .quantize(PFChart.TWOPLACES))

    def _set_current_state(self):
        current_meta_index = next(reversed(self.chart_meta_data))
        current_meta = self.chart_meta_data[current_meta_index]
        self.current_signal = current_meta['signal']
        self.current_status = current_meta['status']
        self.current_action = current_meta['action']
        self.current_move = current_meta['move']
        self.current_column_index = current_meta['column_index']
        self.current_scale_index = current_meta['scale_index']
        self.current_scale_value = current_meta['scale_value']
        self.current_direction = current_meta['direction']

    def _set_historical_data(self):
        # self._log.info('setting historical data')
        # if len(self.security.historical_data) == 0:
        #     self.security.populate_data()
        
        # if self.security.interval == '30min':
        #     days = int(self.duration * ())
        # elif self.security.interval == '1d':
        #     days = int(self.duration * 252)
        # elif self.security.interval == '1wk':
        #     days = int(self.duration * 52)
        # elif self.security.interval == '1mo':
        #     days = int(self.duration * 12)
        # if len(self.security.historical_data) > days:
        #     offset = len(self.security.historical_data) - days
        #     i = 0
        #     while i < offset:
        #         self.security.historical_data.popitem(False)
        #         i += 1
        #     self.historical_data = self.security.historical_data
        # else:
        self.historical_data = self.security.historical_data

    def _set_price_fields(self):
        if self.method == 'HL':
            self.high_field = 'High'
            self.low_field = 'Low'
        else:
            self.high_field = 'Close'
            self.low_field = 'Close'
        self.open_field = 'Open'
        self.close_field = 'Close'
        self.volume_field = 'Volume'
        self.date_field = 'Date'

    def _set_scale(self):
        row = next(iter(self.historical_data))
        day = self.historical_data[row]
        highest = day[self.high_field]
        lowest = day[self.low_field]

        for row in self.historical_data:
            day = self.historical_data[row]
            if day[self.high_field] > highest:
                highest = day[self.high_field]
            if day[self.low_field] < lowest:
                lowest = day[self.low_field]

        temp_scale = []
        current = Decimal(.01)
        temp_scale.append(current)

        while current < highest:
            value = current + (current * self.box_size)
            temp_scale.append(value)
            current = value

        slice_point = 0
        for index, scale_value in enumerate(temp_scale):
            if scale_value > lowest:
                slice_point = index - 1
                break

        temp_scale = temp_scale[slice_point:]

        self.scale = OrderedDict()
        for index, scale_value in enumerate(temp_scale):
            self.scale[index] = scale_value

    def _store_base_metadata(self, day, signal, status, action, move,
                             column_index, scale_index, scale_value,
                             direction, prior_high, prior_low, current_close_price, trend_reversal):
        date_value = day['Date']
        self.chart_meta_data[date_value] = {}
        self.chart_meta_data[date_value]['signal'] = signal
        self.chart_meta_data[date_value]['status'] = status
        self.chart_meta_data[date_value]['action'] = action
        self.chart_meta_data[date_value]['move'] = move
        self.chart_meta_data[date_value]['column_index'] = column_index
        self.chart_meta_data[date_value]['scale_index'] = scale_index
        self.chart_meta_data[date_value]['scale_value'] = scale_value
        self.chart_meta_data[date_value]['direction'] = direction
        self.chart_meta_data[date_value]['prior_high'] = prior_high
        self.chart_meta_data[date_value]['prior_low'] = prior_low
        self.chart_meta_data[date_value]['current_close_price'] = current_close_price
        self.chart_meta_data[date_value]['trend_reversal'] = trend_reversal
        self.chart_meta_data[date_value]['box_count'] = ''
        self.chart_meta_data[date_value]['dtr_support'] = ''
        self.chart_meta_data[date_value]['utr_resistance'] = ''
        self.chart_meta_data[date_value]['anchor_column'] = ''
        
                        
        self._store_custom_metadata(day)

    def _store_custom_metadata(self, day):
        pass

    def _style(self, style, message):
        if self.style:
            method = getattr(pypf.terminal_format, style)
            return method(message)
        else:
            return message

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
    programName = configDict['SIGNAL_SVC_PGM_NAME']

    # Initialized the log files 
    util.initialize_logs(str(configDict['SIGNAL_SVC_PGM_NAME']) + '.log')

    programExitFlag = 'N'    

    # Connect to MySQL database
    cnx, mySQLCursor = util.connect_mysql_db(configDict)
    
    adminTradeAccount = configDict['ADMIN_TRADE_ACCOUNT']
    adminTradeBroker = configDict['ADMIN_TRADE_BROKER'] 
    # Connect to Kite ST
    apiConnObj, isAPIConnected = baf.connect_broker_api(cnx, mySQLCursor, adminTradeAccount, broker = adminTradeBroker)    

    # If the broker is not connected, raise an alert to admin and exit the program; otherwise proceed with further processing
    if (isAPIConnected):           
        
        alertMsg = 'The program (' + programName.replace('_','\_') + ')  started at ' + str(util.get_date_time_formatted("%d-%m-%Y %H:%M:%S"))
        
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', alertMsg, telgUpdateFlag='Y', programName=programName)
        
        # Continuously run the program until the exit flag turns to Y
        while programExitFlag != 'Y': 
            try:
                
                security = HistoricalData('2953217', exchangeToken='1047', interval='1d', period=300)
                security.get_historical_data(apiConnObj, adminTradeBroker)

                chart = PFChart(security, box_size=.005, duration=1, method='HL', reversal=3, style=False, trend_lines=True, debug=False)
                print(chart)
                chart.create_chart(dump=True)                        
                          
                programExitFlag = 'Y'
            except Exception as e:
                print('Signal Service failed (main block): '+ str(e))
                programExitFlag = 'Y'
                
    else:
        alertMsg = 'Unable to connect admin trade account from signal service. There will be no signals generated for today until the issue is fixed.'
        util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'ERROR', alertMsg, telgUpdateFlag='Y', programName=programName)    
        
    
    # Verify whether the connection to MySQL database is open
    cnx, mySQLCursor = util.verify_db_connection(cnx, mySQLCursor, configDict)

    util.update_program_running_status(cnx, mySQLCursor,programName, 'INACTIVE')
    util.send_alerts(logEnableFlag, cnx, mySQLCursor, configDict['TELG_ADMIN_ID'], 'INFO', 'Program ended', telgUpdateFlag='N', programName=programName)    
    util.disconnect_db(cnx, mySQLCursor)