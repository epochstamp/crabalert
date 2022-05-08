import asyncio
import discord
from config import ID_SERVER, ID_COMMAND_CENTER
from utils import in_channel

class Reboot(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="reboot")
    @discord.ext.commands.has_any_role('Admin', 'Moderator')
    @in_channel(ID_COMMAND_CENTER)
    async def reboot(self, ctx):
        await ctx.channel.send(f'Good bye.')
        exit(1)
