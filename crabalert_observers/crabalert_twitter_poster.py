from abc import ABC, abstractmethod
import asyncio
from .crabalert_observer import CrabalertObserver
import tweepy
from requests_oauthlib import OAuth1Session
from utils import bold
from config import cool_classes, subclass_map, cool_subclasses
from asyncio import Semaphore

SLEEP_TIME = 10

class CrabalertTwitterPoster(CrabalertObserver):

    def __init__(self):
        CONSUMER_KEY = 'jnUYZMLw1mylES1QdbDHYFlJn'
        CONSUMER_SECRET = 'YAbJGAfeXlQxAOoUleKTGkj446hm6NyABg2nx3lxTBxXhUzQeW'

        # Create a new Access Token
        ACCESS_TOKEN = '1489533611447570433-w00bPp9e7lgDUeiT0CsZLJGWAQ6PxK'
        ACCESS_SECRET = 'lT3qHcGpe1XUAYAHRNHUzPbMZ0lCkEr4U4tDwojPvT5d8'
        BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAPSBYwEAAAAAeg9uky5PLdsSwJW9FWyxIChhYGQ%3Dy9Slr9skgP7Q95d7NfTki3nmgqnvUXss5VvkjjodFM1hZFHCPc'
        self._client = tweepy.Client(
            bearer_token=BEARER_TOKEN,
            consumer_key=CONSUMER_KEY,
            consumer_secret=CONSUMER_SECRET,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_SECRET
        )
        self._semaphore = Semaphore(value=1)

    async def notify_crab_item(self, infos_nft, token_id, price, timestamp_transaction):
        async with self._semaphore:
            tus_text = f"{price} #TUS"
            first_column = tus_text
            subclass_display = subclass_map.get(infos_nft['crabada_subclass'], 'unknown')
            subclass_display = subclass_display if subclass_display.lower() not in cool_subclasses else bold(subclass_display)
            class_display = infos_nft['class_name']
            class_display = class_display if class_display.lower() not in cool_classes else bold(class_display)
            message = (
                f"🦀 {class_display}({subclass_display}) (No.{token_id}) for sale at {first_column} on #PlayCrabada Marketplace\n" +
                f"Join Discord server https://discord.gg/NvkNAax2\n" +
                f"https://marketplace.crabada.com/crabada/{token_id}"
            )
            self._client.create_tweet(text=message)
            await asyncio.sleep(SLEEP_TIME)

    async def notify_egg_item(self, infos_family_nft, infos_nft, token_id, price, timestamp_transaction):
        async with self._semaphore:
            tus_text = f"{price} #TUS"
            first_column = tus_text
            crabada_parent_1 = infos_family_nft[0]
            crabada_parent_2 = infos_family_nft[1]
            class_parent_1 = crabada_parent_1["class_name"]
            class_parent_2 = crabada_parent_2["class_name"]
            if class_parent_1 == class_parent_2:
                class_display = class_parent_1
                class_display = class_display if class_display.lower() not in cool_classes else bold(class_display)
                emoji = "🦀"
            else:
                egg_class_1 = class_parent_1
                egg_class_2 = class_parent_2
                class_display_1 = egg_class_1 if egg_class_1.lower() not in cool_classes else bold(egg_class_1)
                class_display_2 = egg_class_2 if egg_class_2.lower() not in cool_classes else bold(egg_class_2)
                class_display = f"{class_display_1}┃{class_display_2}"
                emoji = "🥚"
            message = (
                f"{emoji} {class_display} (No.{token_id}) for sale at {first_column} on #PlayCrabada Marketplace\n" +
                f"Join Discord server https://discord.gg/NvkNAax2\n" +
                f"https://marketplace.crabada.com/crabada/{token_id}"
            )
            self._client.create_tweet(text=message)
            await asyncio.sleep(SLEEP_TIME)

    @property
    def id(self):
        return "CrabalertTwitterPoster"

if __name__ == "__main__":
    crabalert_twitter_poster = CrabalertTwitterPoster()
    crabalert_twitter_poster._client.create_tweet(text="machin")
