import discord
from config import ID_SERVER, ID_COMMAND_CENTER
from utils import in_channel

class Shutdown(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="shutdown")
    @discord.ext.commands.has_any_role('Admin')
    @in_channel(ID_COMMAND_CENTER)
    async def shutdown(self, ctx):
        await ctx.channel.send(f'Farewell.')
        exit(0)
