from smartapi import SmartWebSocket

# feed_token=092017047
FEED_TOKEN="0954964142"
CLIENT_CODE="S608881"

token="nse_cm|2885&nse_cm|1594&nse_cm|11536&nse_cm|3045"
task="sfi"   # mw|sfi|dp

ss = SmartWebSocket(FEED_TOKEN, CLIENT_CODE)

def on_message(ws, message):
    print("Ticks: {}".format(message))
    print(message[0]['ak'])
    
def on_open(ws):
    print("on open")
    ss.subscribe(task,token)
    
def on_error(ws, error):
    print(error)
    
def on_close(ws):
    print("Close")

# Assign the callbacks.
ss._on_open = on_open
ss._on_message = on_message
ss._on_error = on_error
ss._on_close = on_close

ss.connect()