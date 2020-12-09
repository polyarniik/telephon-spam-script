import asyncio
import os
import random
import sqlite3
import threading
import time

import schedule
import telethon
import telethon.sync
from telethon.errors import ChannelsTooMuchError, SessionPasswordNeededError, rpcerrorlist
from telethon.tl.functions.channels import JoinChannelRequest

account_chat_hour = {}
account_chat_hour_message = {}
account_chat_day = {}
account_chat_day_message = {}
account_chat_week = {}
account_chat_week_message = {}
joined_account_chats_hour = {}
joined_account_chats_day = {}
joined_account_chats_week = {}
sessionHour = 'pennyHour'
sessionDay = 'pennyDay'
sessionWeek = 'pennyWeek'


def start():
    def get_data(file, account_chats, account_message):
        account = ''
        lines = file.readlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip().split('\n')[0]
            if len(line) > 0:
                if '=' in line:
                    account = line.split('=', maxsplit=1)[1]
                    account_chats[account] = []
                elif 'messageStart' in line:
                    message = ''
                    i += 1
                    line = lines[i]
                    while not ('messageEnd' in line):
                        message += line
                        i += 1
                        line = lines[i]
                    account_message[account] = message
                else:
                    account_chats[account].append(line)
            i += 1

    def get_accounts_chats_messages(day):
        if day == 1:
            package = 'hour_message'
        elif day == 2:
            package = 'day_message'
        else:
            package = 'week_message'
        try:
            for file in os.listdir(package):
                if file.endswith('.txt'):
                    file = open(os.path.join(package, file))
                    if day == 1:
                        get_data(file, account_chat_hour, account_chat_hour_message)
                    elif day == 2:
                        get_data(file, account_chat_day, account_chat_day_message)
                    else:
                        get_data(file, account_chat_week, account_chat_week_message)
        except:
            pass

    def authorization(client, phone_number):
        if not client.is_user_authorized():
            print(phone_number)
            client.sign_in(phone_number)
            try:
                client.sign_in(code=input('Enter code: '))
            except SessionPasswordNeededError:
                try:
                    client.sign_in(password=input('Enter password: '))
                except rpcerrorlist.PasswordHashInvalidError:
                    authorization(client, phone_number)
            except rpcerrorlist.PhoneCodeInvalidError:
                authorization(client, phone_number)

    async def do_join(day):
        if day == 1:
            account_chats = account_chat_hour
        elif day == 2:
            account_chats = account_chat_day
        else:
            account_chats = account_chat_week
        for account in account_chats:
            if day == 1:
                joined_account_chats_hour[account] = []
            elif day == 2:
                joined_account_chats_day[account] = []
            else:
                joined_account_chats_week[account] = []

            if day == 1:
                client = telethon.TelegramClient(sessionHour, account.split(',')[0].strip(),
                                                 account.split(',')[1].strip())
            elif day == 2:
                client = telethon.TelegramClient(sessionDay, account.split(',')[0].strip(),
                                                 account.split(',')[1].strip())
            else:
                client = telethon.TelegramClient(sessionWeek, account.split(',')[0].strip(),
                                                 account.split(',')[1].strip())
            await client.connect()
            client_dialogs = []
            for dialog in await client.get_dialogs():
                client_dialogs.append(dialog.entity.username)
            for chat in account_chats[account]:
                if chat not in client_dialogs:
                    try:
                        print("'" + chat + "'")
                        await client(JoinChannelRequest(chat))
                        time.sleep(random.randrange(30, 120))
                    except ChannelsTooMuchError as e:
                        time.sleep(310)
                        try:
                            await client(JoinChannelRequest(chat))
                            time.sleep(10)
                        except:
                            continue

                    except rpcerrorlist.FloodWaitError as e:
                        print('sleep' + str(e.seconds))
                        time.sleep(e.seconds + 20)
                        try:
                            await client(JoinChannelRequest(chat))
                            time.sleep(10)
                        except:
                            continue
                    except rpcerrorlist.ChannelPrivateError:
                        continue
                    except:
                        continue
                if day == 1:
                    joined_account_chats_hour[account].append(chat)
                elif day == 2:
                    joined_account_chats_day[account].append(chat)
                else:
                    joined_account_chats_week[account].append(chat)

            await client.disconnect()

    async def send_messages(day):
        if day == 1:
            account_chats = joined_account_chats_hour
            account_message = account_chat_hour_message
        elif day == 2:
            account_chats = joined_account_chats_day
            account_message = account_chat_day_message
        else:
            account_chats = joined_account_chats_week
            account_message = account_chat_week_message
        for account in account_chats:
            message = account_message[account]
            if day == 1:
                client = telethon.TelegramClient(sessionDay, account.split(',')[0].strip(),
                                                 account.split(',')[1].strip())
            elif day == 2:
                client = telethon.TelegramClient(sessionDay, account.split(',')[0].strip(),
                                                 account.split(',')[1].strip())
            else:
                client = telethon.TelegramClient(sessionWeek, account.split(',')[0].strip(),
                                                 account.split(',')[1].strip())
            await client.connect()
            client_dialog = []
            for dialog in await client.get_dialogs():
                client_dialog.append(dialog.entity.username)
            n = 1
            for chat in account_chats[account]:
                if n % 50 == 0:
                    n = 1
                    time.sleep(random.randrange(300, 600))
                n += 1

                try:
                    await client.send_message(chat, message)
                    time.sleep(random.randrange(1, 3))
                except rpcerrorlist.ChatAdminRequiredError:
                    continue
                except rpcerrorlist.FloodWaitError as e:
                    time.sleep(e.seconds + 10)
                    try:
                        await client.send_message(chat, message)
                        time.sleep(random.randrange(1, 3))
                    except:
                        continue
                except rpcerrorlist.UsernameNotOccupiedError:
                    continue
                except ValueError:
                    continue
                except rpcerrorlist.SlowModeWaitError as e:
                    time.sleep(e.seconds + 10)
                    try:
                        await client.send_message(chat, message)
                    except:
                        time.sleep(random.randrange(3, 10))
                        try:
                            await client.send_message(chat, message)
                        except:
                            pass
            await client.disconnect()

    get_accounts_chats_messages(1)
    get_accounts_chats_messages(2)
    get_accounts_chats_messages(3)

    # hour accounts authorization
    for account in account_chat_hour:
        client = telethon.TelegramClient(sessionHour, account.split(',')[0].strip(), account.split(',')[1].strip())
        try:
            client.connect()
            authorization(client, account.split(',')[2].strip())
            client.disconnect()
        except sqlite3.OperationalError:
            try:
                client.connect()
                authorization(client, account.split(',')[2].strip())
                client.disconnect()
            except:
                pass

    # day accounts authorization
    for account in account_chat_day:
        client = telethon.TelegramClient(sessionDay, account.split(',')[0].strip(), account.split(',')[1].strip())
        try:
            client.connect()
            authorization(client, account.split(',')[2].strip())
            client.disconnect()
        except sqlite3.OperationalError:
            try:
                client.connect()
                authorization(client, account.split(',')[2].strip())
                client.disconnect()
            except:
                pass

    # week accounts authorization
    for account in account_chat_week:
        client = telethon.TelegramClient(sessionWeek, account.split(',')[0].strip(), account.split(',')[1].strip())
        try:
            client.connect()
            authorization(client, account.split(',')[2].strip())
            client.disconnect()
        except sqlite3.OperationalError:
            try:
                client.connect()
                authorization(client, account.split(',')[2].strip())
                client.disconnect()
            except:
                pass
    time.sleep(30)

    # join to chats
    asyncio.run(do_join(1))
    asyncio.run(do_join(2))
    asyncio.run(do_join(3))

    def thread_send(day):
        asyncio.run(send_messages(day))

    def send(day):
        threading.Thread(target=thread_send, args=(day,)).start()

    # automatic launch of methods at a specific time
    schedule.every().hour.at(':49').do(send, 1)
    schedule.every().day.at('16:00').do(send, 2)
    schedule.every().monday.at('14:37').do(send, 3)
    while True:
        schedule.run_pending()
        time.sleep(10)


while True:
    try:
        start()
    except ConnectionError:
        time.sleep(300)
