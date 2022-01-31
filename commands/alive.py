import discord
from config import ID_SERVER
import asyncio

class Alive(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="alive")
    @discord.ext.commands.has_any_role('Admin', 'Moderator')
    async def alive(self, ctx):
        asyncio.create_task(ctx.channel.send(f'I am alive.'))
