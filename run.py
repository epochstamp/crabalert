import discord
from crabalert import Crabalert
from datetime import datetime
from datetime import timezone
from config import (
    SPAN_TIMESTAMP,
    SNOWTRACE_API_KEY
)
import urllib
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

def run_client(bot: Crabalert, *args, **kwargs):
    global logger
    loop = bot.loop#asyncio.get_event_loop()
    while True:
        try:
            loop.run_until_complete(bot.start(*args, **kwargs))
        except SystemExit as ex_exception:
            print("destroyed")
            exit(ex_exception.code)
        except KeyboardInterrupt:
            print("destroyed")
            exit(1)
        except Exception as e:
            if logger is not None:
                logger.debug("This exception happened: ", e)
            print("Error", e)  # or use proper logging
        asyncio.run(bot._close_all_tasks())
        print("Waiting until restart")
        time.sleep(WAITING_BEFORE_RECONNECT)

if __name__ == "__main__":
    intents = discord.Intents().all()
    intents.reactions = True
    intents.members = True
    intents.guilds = True

    
    bot = Crabalert(command_prefix="!", intents=intents)
    for command in commands.values():
        bot.add_cog(command(bot))
    time.sleep(2)
    #client.run('OTMyNDc4NjQ1ODE2MTQzOTA5.YeTkaQ.AzMetNL0LwuPYh7NOFrZvhG4VzQ')
    logger.info("Bot is starting")
    run_client(bot, 'OTMyNDc4NjQ1ODE2MTQzOTA5.YeTkaQ.AzMetNL0LwuPYh7NOFrZvhG4VzQ', reconnect=False)
