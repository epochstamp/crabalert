import discord
import sys
import traceback
from discord.ext import commands
from asyncio import create_task

from config import ID_COMMAND_CENTER
from . import in_channel


class CommandErrHandler(commands.Cog):

    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    @in_channel(ID_COMMAND_CENTER)
    async def on_command_error(self, ctx, error):
        """The event triggered when an error is raised while invoking a command.
        Parameters
        ------------
        ctx: commands.Context
            The context used for command invocation.
        error: commands.CommandError
            The Exception raised.
        """
        if isinstance(error, discord.ext.commands.CommandNotFound):
            if ctx.channel.id == ID_COMMAND_CENTER:
                create_task(ctx.send('I do not know that command?!'))
        elif isinstance(error, discord.ext.commands.CheckFailure):
            pass
        else:
            print('Ignoring exception in command {}:'.format(ctx.command), file=sys.stderr)
            traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
