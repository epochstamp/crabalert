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
    async def list_wallets(self, ctx):
        guild = self.bot.get_guild(ID_SERVER)
        member = await guild.fetch_member(ctx.author.id)
        roles_str = [str(role) for role in member.roles]
        if "Admin" not in roles_str and "Moderator" not in roles_str:
            print("You cannot execute that command.")
            return
        connection = open_database()
        data = execute_query(
            connection, f"SELECT discord_id, from_wallet, received_timestamp FROM last_received_payment",
        )
        field_names = ["Discord Tag", "Wallet address", "Last payment date"]
        x = []
        for d in data:
            try:
                member = await guild.fetch_member(d[0])
                x.append([member, d[1], d[2]])
            except:
                pass
        output = t2a(
            header=field_names,
            body=x,
            style=PresetStyle.thin_compact
        )
        #embed = discord.Embed(title="Wallet addresses per member", description=f"```\n{output}\n```")
        await ctx.send(f"```\n{output}\n```")
        close_database(connection)