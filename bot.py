# bot.py
import discord
from player_lookup import generate_player_response

intents = discord.Intents.default()
intents.message_content = True  # Required to read messages

client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user}")

@client.event
async def on_message(message):
    if message.author.bot:
        return

    if message.content.startswith("!player"):
        query = message.content[len("!player"):].strip()
        if not query:
            await message.channel.send("Please enter a player name, e.g., `!player LeBron James`")
            return

        await message.channel.send("Searching...")
        result = generate_player_response(query)
        await message.channel.send(result)

client.run("DISCORD_API_KEY")
