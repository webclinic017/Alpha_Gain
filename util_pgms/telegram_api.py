
# importing all required libraries
from telethon.sync import TelegramClient
from telethon.tl.types import InputPeerUser, InputPeerChannel
from telethon import TelegramClient, sync, events
 
  
# get your api_id, api_hash, token
# from telegram as described above
api_id = '18447800'
api_hash = 'fbdc697ce6307ff439dee9bc843bc6ce'
token = '5372717376:AAEayf6ru4H-lZUOCQK0DZsITlifG_PGzp4'
message = "Working..."
 
# your phone number
phone = '+97433478529'
  
# creating a telegram session and assigning
# it to a variable client
client = TelegramClient('session', api_id, api_hash)
  
# connecting and building the session
client.connect()
 
# in case of script ran first time it will
# ask either to input token or otp sent to
# number or sent or your telegram id
if not client.is_user_authorized():
  
    client.send_code_request(phone)
    # signing in the client
    client.sign_in(phone, input('enter your id:'))
  
try:
    destination_channel_username='suresh_alphagain'
    entity=client.get_entity(destination_channel_username)
    client.send_message(entity=entity,message="Hi")    
except Exception as e:
     
    # there may be many error coming in while like peer
    # error, wrong access_hash, flood_error, etc
    print(e);
 
# disconnecting the telegram session
client.disconnect()