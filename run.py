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

if __name__ == "__main__":
    intents = discord.Intents().all()
    intents.reactions = True
    intents.members = True
    intents.guilds = True

    
    client = Crabalert(command_prefix="!", intents=intents)
    for command in commands.values():
        client.add_cog(command(client))
    time.sleep(5)
    client.run('OTMyNDc4NjQ1ODE2MTQzOTA5.YeTkaQ.AzMetNL0LwuPYh7NOFrZvhG4VzQ')
    print("destroyed")
