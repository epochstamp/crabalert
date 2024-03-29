import asyncio
import discord
from config import ID_SERVER, ID_COMMAND_CENTER
import re
import sqlite3 as sl
from utils import (
    open_database,
    execute_query,
    close_database,
    in_channel
)
from table2ascii import table2ascii as t2a, PresetStyle

class ListWallets(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="lw")
    @discord.ext.commands.has_any_role('Admin')
    @in_channel(ID_COMMAND_CENTER)
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
                member = guild.get_member(int(d[0]))
                x.append([member, d[1], d[2], d[3]])
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