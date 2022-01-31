from datetime import datetime, timezone
import discord
import humanize
from config import ID_SERVER, WALLET_PATTERN, DURATION_TRIAL
import re
import sqlite3 as sl
from utils import (
    open_database,
    execute_query,
    close_database
)
import asyncio

class DurationTrial(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot


    @discord.ext.commands.command(name="trial")
    @discord.ext.commands.has_any_role('Verified')
    async def register(self, ctx, wallet: str = ""):
        guild = self.bot.get_guild(ID_SERVER)
        member = guild.get_member(ctx.author.id)
        roles_str = [str(role) for role in member.roles]
        if "Alerted" in roles_str:
            asyncio.create_task(ctx.channel.send(f'You already suscribed to alerts.'))
            return
        if "Admin" in roles_str or "Moderator" in roles_str:
            asyncio.create_task(ctx.channel.send(f'Are you kidding ser ?'))
            return
        discord_id = ctx.author.id
        connection = open_database()
        data = execute_query(
            connection, f"SELECT discord_id FROM trials WHERE discord_id = '{discord_id}'",
        )
        # gets the number of rows affected by the command executed
        rowcount = len(data)
        if rowcount == 0:
            try:
                dt = datetime.now(timezone.utc)
                utc_time = dt.replace(tzinfo=timezone.utc)
                current_timestamp = utc_time.timestamp()
                insert_wallet = f"INSERT INTO trials (discord_id, start_trial, duration_trial) VALUES('{discord_id}', '{current_timestamp}', '{DURATION_TRIAL}')"
                status = execute_query(connection, insert_wallet)
                if status == 1:
                    asyncio.create_task(ctx.channel.send(f'Your trial has just started for a duration of {humanize.naturaldelta(DURATION_TRIAL)} starting from now'))
            except Exception as e:
                asyncio.create_task(ctx.channel.send(f'Something went wrong. Please open a ticket and send the following error message : {str(e)}.'))
        else:
            asyncio.create_task(ctx.channel.send(f'You already had a trial subscription. Please follow the #instructions to suscribe to the alerts.'))
        close_database(connection)
