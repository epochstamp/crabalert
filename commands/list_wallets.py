import asyncio
import discord
from config import ID_SERVER, WALLET_PATTERN
import re
import sqlite3 as sl
from utils import (
    open_database,
    execute_query,
    close_database
)
from table2ascii import table2ascii as t2a, PresetStyle

class ListWallets(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="lw")
    @discord.ext.commands.has_any_role('Admin')
    async def list_wallets(self, ctx):
        guild = self.bot.get_guild(ID_SERVER)
        connection = open_database()
        data = execute_query(
            connection, f"SELECT discord_id, from_wallet, received_timestamp, duration FROM last_received_payment",
        )
        field_names = ["Discord Tag", "Wallet address", "Last payment date", "Duration"]
        x = []
        for d in data:
            try:
                print(type(d[0]))
                member = guild.get_member(int(d[0]))
            except:
                pass
        output = t2a(
            header=field_names,
            body=x,
            style=PresetStyle.thin_compact
        )
        #embed = discord.Embed(title="Wallet addresses per member", description=f"```\n{output}\n```")
        asyncio.create_task(ctx.send(f"```\n{output}\n```"))
        close_database(connection)