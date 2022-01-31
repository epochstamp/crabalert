import asyncio
import discord
from config import ID_SERVER

class Reboot(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="reboot")
    @discord.ext.commands.has_any_role('Admin', 'Moderator')
    async def reboot(self, ctx):
        asyncio.create_task(ctx.channel.send(f'Good bye.'))
        exit(1)
