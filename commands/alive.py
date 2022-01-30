import discord
from config import ID_SERVER

class Alive(discord.ext.commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.ext.commands.command(name="alive")
    async def alive(self, ctx):
        guild = self.bot.get_guild(ID_SERVER)
        member = await guild.fetch_member(ctx.author.id)
        roles_str = [str(role) for role in member.roles]
        if "Admin" not in roles_str and "Moderator" not in roles_str:
            print("You cannot execute that command.")
            return
        await ctx.channel.send(f'I am alive.')