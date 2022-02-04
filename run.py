import aiohttp
import discord
from crabalert import Crabalert
from datetime import datetime
from datetime import timezone
from config import (
    SPAN_TIMESTAMP,
    SNOWTRACE_API_KEY
)
import urllib
from crabalert_observers import crabalert_observer
from utils import (
    HEADERS,
    is_valid_marketplace_transaction
)
import json
from commands import commands
import time
import logging
import sys
import asyncio
from config import WAITING_BEFORE_RECONNECT
from discord.http import HTTPClient
from discord.http import Route, HTTPException, LoginFailure, DiscordClientWebSocketResponse
from discord import user
import json
import os
from crabalert_observers import observers

logger = None

if len(sys.argv) > 1 and sys.argv[1] == "debug":
    logger = logging.getLogger('discord')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename='logs/discord.log', encoding='utf-8', mode='w')
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
    logger.addHandler(handler)

    logger = logging.getLogger('web3')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename='logs/web3.log', encoding='utf-8', mode='w')
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
    logger.addHandler(handler)

    logger = logging.getLogger('asyncio')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename='logs/asyncio.log', encoding='utf-8', mode='w')
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
    logger.addHandler(handler)

    logger = logging.getLogger('aiohttp')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename='logs/aiohttp.log', encoding='utf-8', mode='w')
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
    logger.addHandler(handler)

    logger = logging.getLogger('crabalert')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename='logs/crabalert.log', encoding='utf-8', mode='w')
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
    logger.addHandler(handler)

def safe_json(data): 
    if data is None: 
        return True 
    elif isinstance(data, (bool, int, float)): 
        return True 
    elif isinstance(data, (tuple, list)): 
        return all(safe_json(x) for x in data) 
    elif isinstance(data, dict): 
        return all(isinstance(k, str) and safe_json(v) for k, v in data.items()) 
    return False 

def run_client(*args, **kwargs):
    global logger
    intents = discord.Intents().all()
    intents.reactions = True
    intents.members = True
    intents.guilds = True

    variables = None
    bot = None
    if os.path.isfile("variables.json"):
        variables = json.load(open("variables.json"))
    try:
        lst_observers = [] #[observer() for observer in observers.values()]
        bot = Crabalert(command_prefix="!", intents=intents, variables=variables, crabalert_observers=lst_observers)
        loop = bot.loop#asyncio.get_event_loop(
        for command in commands.values():
            bot.add_cog(command(bot))
        if logger is not None:
            logger.info("Bot is about to start...")
        loop.run_until_complete(bot.start(*args, **kwargs))
    except SystemExit as ex_exception:
        print("destroyed")
        if (ex_exception.code == 1):
            variables = {k:v for k,v in bot.variables.items() if "sem_" not in k and safe_json(v)}
            f_variables = open("variables.json", "w+")
            json.dump(variables, f_variables)
            f_variables.close()
        exit(ex_exception.code)
    except KeyboardInterrupt:
        print("destroyed")
        exit(1)
    except Exception as e:
        if logger is not None:
            logger.debug("This exception happened during bot exec: ", str(e))
        print("Error", e)  # or use proper logging
    if bot is not None:
        variables = {k:v for k,v in bot.variables.items() if "sem_" not in k and safe_json(v) and k != "already_seen"}
        f_variables = open("variables.json", "w+")
        json.dump(variables, f_variables)
        f_variables.close()
        exit(1)
    """
    variables = {k:v for k,v in bot.variables.items() if "sem_" not in k}
    asyncio.run(bot._close_all_tasks())
    try:
        asyncio.run(bot.close())
    except Exception as e:
        if logger is not None:
            logger.debug("This exception happened when closing bot: ", str(e))
    print("Waiting until restart")
    time.sleep(WAITING_BEFORE_RECONNECT)
    """


if __name__ == "__main__":
    intents = discord.Intents().all()
    intents.reactions = True
    intents.members = True
    intents.guilds = True

    
    #bot = Crabalert(command_prefix="!", intents=intents)
    #for command in commands.values():
    #    bot.add_cog(command(bot))
    #time.sleep(2)
    #client.run('OTMyNDc4NjQ1ODE2MTQzOTA5.YeTkaQ.AzMetNL0LwuPYh7NOFrZvhG4VzQ')
    run_client('OTMyNDc4NjQ1ODE2MTQzOTA5.YeTkaQ.AzMetNL0LwuPYh7NOFrZvhG4VzQ', reconnect=False)
