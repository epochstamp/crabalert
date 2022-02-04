from abc import ABC, abstractmethod
from .crabalert_observer import CrabalertObserver
import tweepy
from requests_oauthlib import OAuth1Session
from utils import bold
from config import cool_classes, subclass_map, cool_subclasses

class CrabalertTwitterPoster(CrabalertObserver):

    def __init__(self):
        CONSUMER_KEY = '1w0WlPuwcENEBtLgqd7kSp5GD'
        CONSUMER_SECRET = 'pZIjzF9y8hAjggB29XavkgDjQVq1CvMz7MFnoEH9mDIY2C9B5V'

        # Create a new Access Token
        ACCESS_TOKEN = '1489533611447570433-7aCX71oDh4Ma79IWoUOA1ZoMXgAjQM' 
        ACCESS_SECRET = '1nxqBN3UL3VXG8H2YJ1Sm3Ue05zYIqFFe2KIQ0iGqsdh0'
        BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAOlvYwEAAAAAJWxrfKg6kCZRqnsC5OX83v%2F1hPI%3Dt1wu6Vs5Q9YB4U9nVR3ujBqukdyPwMK3EocmfDHqoto1QWoFGJ'
        self._client = tweepy.Client(
            bearer_token=BEARER_TOKEN,
            consumer_key=CONSUMER_KEY,
            consumer_secret=CONSUMER_SECRET,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_SECRET
        )

    async def notify_crab_item(self, infos_nft, token_id, price, timestamp_transaction):
        tus_text = f"$TUS {price}"
        first_column = tus_text
        subclass_display = subclass_map.get(infos_nft['crabada_subclass'], 'unknown')
        subclass_display = subclass_display if subclass_display.lower() not in cool_subclasses else bold(subclass_display)
        class_display = infos_nft['class_name']
        class_display = class_display if class_display.lower() not in cool_classes else bold(class_display)
        message = (
            f"🦀 {class_display}({subclass_display})\n" +
            f"{first_column}\n" +
            f"https://marketplace.crabada.com/crabada/{token_id}"
        )
        self._client.create_tweet(text=message)

    async def notify_egg_item(self, infos_family_nft, infos_nft, token_id, price, timestamp_transaction):
        tus_text = f"$TUS {price}"
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
            f"{emoji} {class_display}\n" +
            f"{first_column}\n" +
            f"https://marketplace.crabada.com/crabada/{token_id}"
        )
        self._client.create_tweet(text=message)

    @property
    def id(self):
        return "CrabalertTwitterPoster"

if __name__ == "__main__":
    crabalert_twitter_poster = CrabalertTwitterPoster()
