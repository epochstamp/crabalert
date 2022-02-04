from abc import ABC, abstractmethod
import asyncio
from .crabalert_observer import CrabalertObserver
import tweepy
from requests_oauthlib import OAuth1Session
from utils import bold
from config import cool_classes, subclass_map, cool_subclasses
from asyncio import Semaphore

SLEEP_TIME = 5

class CrabalertTwitterPoster(CrabalertObserver):

    def __init__(self):
        CONSUMER_KEY = 'IsNKAf3aZsVpg0v2QUOLNKTiG'
        CONSUMER_SECRET = 'w0K3Ox2YOSac1sTlULOXB0RIPUdz4ssYqhjKjfehpQo6MeTjig'

        # Create a new Access Token
        ACCESS_TOKEN = '1489533611447570433-EhTr0pWH150N9Ize2WfkkiwJviUsvV'
        ACCESS_SECRET = 'xIrPxiJhgGyoTNHNJDqp2UVL8ukplMIlp0aI6k5gHdNSY'
        BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAABJ3YwEAAAAAwEMqYQISXJRoOtoFqje63i8bRt8%3D74CsvhXWWhguBk5C7TygLff21XvAN5yOSrklKI03u6Wf49gD9Z'
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
                f"🦀 {class_display}({subclass_display}) (No.{token_id}) sold for {first_column} on @PlayCrabada Marketplace\n" +
                f"Join Discord server https://discord.gg/NvkNAax2\n" +
                f"Listing: https://marketplace.crabada.com/crabada/{token_id}"
            )
            self._client.create_tweet(text=message)
            asyncio.sleep(SLEEP_TIME)

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
                f"{emoji} {class_display} (No.{token_id}) sold for {first_column} on @PlayCrabada Marketplace\n" +
                f"Join Discord server https://discord.gg/NvkNAax2\n" +
                f"Listing: https://marketplace.crabada.com/crabada/{token_id}"
            )
            self._client.create_tweet(text=message)
            asyncio.sleep(SLEEP_TIME)

    @property
    def id(self):
        return "CrabalertTwitterPoster"

if __name__ == "__main__":
    crabalert_twitter_poster = CrabalertTwitterPoster()
    crabalert_twitter_poster._client.create_tweet(text="machin")
