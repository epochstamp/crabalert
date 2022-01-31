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

class Whitelist(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="wl")
    @discord.ext.commands.has_any_role('Admin')
    async def whitelist(self, ctx, member_id: int):
        guild = self.bot.get_guild(ID_SERVER)
        connection = open_database()
        data = execute_query(
            connection, f"UPDATE last_received_payment SET received_timestamp=0, duration=-1 where discord_id={member_id}",
        )
        connection.commit()
        #embed = discord.Embed(title="Wallet addresses per member", description=f"```\n{output}\n```")
        asyncio.create_task(ctx.send(f"{guild.get_member(member_id).name} has been whitelisted"))
        close_database(connection)
