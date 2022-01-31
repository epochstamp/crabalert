import asyncio
import discord
from config import ID_SERVER

class Reboot(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="reboot")
    @discord.ext.commands.has_any_role('Admin', 'Moderator')
    async def reboot(self, ctx):
        task = asyncio.create_task(ctx.channel.send(f'Good bye.'))
        task.add_done_callback(lambda t: exit(1))
        exit(1)
