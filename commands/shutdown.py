import discord
from config import ID_SERVER
import asyncio

class Shutdown(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="shutdown")
    @discord.ext.commands.has_any_role('Admin')
    async def shutdown(self, ctx):
        task = asyncio.create_task(ctx.channel.send(f'Farewell.'))
        task.add_done_callback(lambda t: exit(0))
