import discord
from numpy import block
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