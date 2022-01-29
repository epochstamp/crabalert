import discord
from config import ID_SERVER, WALLET_PATTERN
import re
import sqlite3 as sl
from utils import (
    open_database,
    execute_query,
    close_database
)

class Register(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="rw")
    async def register(self, ctx, wallet: str = ""):
        guild = self.bot.get_guild(ID_SERVER)
        member = await guild.fetch_member(ctx.author.id)
        roles_str = [str(role) for role in member.roles]
        if "Verified" not in roles_str or "Admin" in roles_str or "Moderator" in roles_str:
            if "Verified" not in roles_str:
                await ctx.channel.send(f'Please verify yourself to Crabalert before registering (go to https://discord.gg/PxyXk4TT).')
            return
        if re.match(WALLET_PATTERN, wallet):
            discord_id = ctx.author.id
            connection = open_database()
            data = execute_query(
                connection, f"SELECT discord_id, from_wallet FROM last_received_payment WHERE discord_id = '{discord_id}'",
            )
            # gets the number of rows affected by the command executed
            rowcount = len(data)
            if rowcount == 0:
                try:
                    insert_wallet = f"INSERT INTO last_received_payment (discord_id, from_wallet, received_timestamp, txn_hash, reminded) VALUES('{discord_id}', '{wallet}', 0, '', 'FALSE')"
                    status = execute_query(connection, insert_wallet)
                    if status == 1:
                        await ctx.channel.send(f'Your wallet {wallet} has been added in the database. Please follow #instructions to proceed to payment.')
                except Exception as e:
                    await ctx.channel.send(f'Something went wrong. Please open a ticket and send the following error message : {str(e)}.')
            else:
                if wallet.lower() == data[0][1]:
                    await ctx.channel.send(f'Your wallet has already been associated with your Discord ID in our database.')
                else:
                    await ctx.channel.send(f'A wallet ({data[0][1]}) has already been added to our database. If you wish to change it, please open a ticket and provide your new wallet address.')
            close_database(connection)