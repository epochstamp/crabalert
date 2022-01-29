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

class DurationTrial(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="trial")
    async def register(self, ctx, wallet: str = ""):
        guild = self.bot.get_guild(ID_SERVER)
        member = await guild.fetch_member(ctx.author.id)
        roles_str = [str(role) for role in member.roles]
        print(humanize.naturaldelta(DURATION_TRIAL))
        if "Verified" not in roles_str or "Admin" in roles_str or "Moderator" in roles_str:
            if "Verified" not in roles_str:
                await ctx.channel.send(f'Please verify yourself to Crabalert before registering (go to https://discord.gg/PxyXk4TT).')
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
                    await ctx.channel.send(f'Your trial has just started for a duration of {humanize.naturaldelta(DURATION_TRIAL)}')
            except Exception as e:
                await ctx.channel.send(f'Something went wrong. Please open a ticket and send the following error message : {str(e)}.')
        else:
            await ctx.channel.send(f'You already had a trial subscription. Please follow the #instructions to suscribe to the alerts.')
        close_database(connection)