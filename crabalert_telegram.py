import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
import wget
import memcache
from config import (
    SELLING_ITEM_EXPIRATION,
    LISTING_ITEM_EXPIRATION,
    cool_classes,
    cool_subclasses,
    subclass_map
)
from subclasses import calc_subclass_info, subclass_type_map
from utils import (
    bold,
    seconds_to_pretty_print
)
from web3 import Web3
import json
import urllib
import re
from telegram.ext import Updater, CallbackContext, CommandHandler
from asyncio import Semaphore

SLEEP_TIME = 11

class CrabalertTelegram:

    def __init__(self, variables=None, context=None):
        self._updater = Updater(token="5282653712:AAGZHLl38ic5vWuO6hp6200p3BnFN_u-yVk", use_context=True)
        """
        self._client = tweepy.Client(
            bearer_token=BEARER_TOKEN,
            consumer_key=CONSUMER_KEY,
            consumer_secret=CONSUMER_SECRET,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_SECRET
        )
        """
        self._semaphore = Semaphore(value=1)
        self._variables = dict() if variables is None else variables
        self._context = context
        self._shared = memcache.Client(["127.0.0.1:11211"], debug=0)
        Path("images/").mkdir(parents=True, exist_ok=True)

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, context):
        self._context = context

    @property
    def variables(self):
        return self._variables

    def _get_variable(self, name: str, f_value_if_not_exists=lambda: None):
        if name not in self._variables:
            self._variables[name] = f_value_if_not_exists()
        return self._variables[name]

    async def _set_variable(self, name: str, value):
        self._variables[name] = value

    def _set_sync_variable(self, name: str, value):
        self._variables[name] = value

    

    async def _notify_crab_item(self, infos_nft, token_id, price, timestamp_transaction, is_selling=False):
        already_seen = self._get_variable("already_seen", lambda: set())
        if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
            self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
            async with self._semaphore:
                price = int(round(price * 10**-18, 0))
                price_formatted = "{:,}".format(price)
                price_in_usd_formatted = "${:,.2f}".format(infos_nft["price_usd"])
                tus_text = f"{price_formatted} $TUS ({price_in_usd_formatted})"
                first_column = tus_text
                subclass = subclass_map.get(infos_nft['crabada_subclass'], 'unknown')
                subclass_display = subclass
                subclass_display = subclass_display if subclass_display.lower() not in cool_subclasses else bold(subclass_display)
                dna = infos_nft["dna"]
                n_comp_subclass = sum([(1 if sbc.lower() == subclass.lower() else 0) for sbc in calc_subclass_info(dna)])
                subclass_type = subclass_type_map.get(infos_nft['crabada_subclass'], 'unknown')
                if subclass_type == "Tank":
                    emoji_subclass_type = "🛡️"
                elif subclass_type == "Damage":
                    emoji_subclass_type = "🗡️"
                elif subclass_type == "Buff":
                    emoji_subclass_type = "✨"
                else:
                    emoji_subclass_type = "❓"
                class_display = infos_nft['class_name']
                class_display = class_display if class_display.lower() not in cool_classes else bold(class_display)
                type_entry = bold("LISTING") if not is_selling else bold("SOLD") + "<aftertime>"
                order_id_pool = self._shared.get("order_id_pool")
                seller_wallet, buyer_wallet, _, _, timestamp_listing, timestamp_selling = order_id_pool.get(infos_nft["order_id"], (None, None, None, None, None, None))
                if is_selling:
                    if timestamp_listing is not None:
                        if timestamp_selling is None:
                            duration_min = abs(timestamp_transaction - float(timestamp_listing))
                        else:
                            duration_min = abs(float(timestamp_selling) - float(timestamp_listing))
                    else:
                        duration_min = None
                    if duration_min is None:
                        type_entry = type_entry.replace("<aftertime>", "")
                    else:
                        human_deltatime = seconds_to_pretty_print(duration_min)
                        type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                    if buyer_wallet is not None and buyer_wallet.lower() == infos_nft['owner'].lower():
                        buyer_seller = infos_nft['owner']
                        buyer_seller_full_name = infos_nft['owner_full_name']
                    else:
                        buyer_wallet = infos_nft["seller_wallet"]
                        buyer_seller = buyer_wallet
                        buyer_seller_full_name = buyer_seller
                else:
                    if seller_wallet is not None and seller_wallet.lower() == infos_nft['owner'].lower():
                        buyer_seller = infos_nft['owner']
                        buyer_seller_full_name = infos_nft['owner_full_name']
                    else:
                        seller_wallet = infos_nft["seller_wallet"]
                        buyer_seller = seller_wallet
                        buyer_seller_full_name = buyer_seller
                        

                    
                buyer_seller_type = "Listed by" if not is_selling else "Bought by"
                url_buyer_seller = infos_nft["url_wallet"]
                message = (
                    f"[{type_entry}] 🦀 {class_display}({emoji_subclass_type} {subclass_display} {n_comp_subclass}/18) No.{token_id} at {first_column} on Crabada Marketplace\n" +
                    f"{infos_nft['photos_link']}\n" +
                    f"Per-category and speed-enhanced alerts in https://discord.gg/KYwprbzpFd\n" +
                    f"#snibsnib\n" +
                    f"{infos_nft['marketplace_link']}\n" +
                    f"{buyer_seller_type} {buyer_seller_full_name}({url_buyer_seller})"
                )
                async with self._get_variable(f"sem_{token_id}_{timestamp_transaction}_{price}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
                    if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                        while True:
                            try:
                                if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                                    """
                                    if not os.path.isfile(f"{token_id}.png"):
                                        wget.download(f"https://photos.crabada.com/{token_id}.png", out=f"{token_id}.png", bar=None)
                                    self._client.update_status_with_media(status=message, filename=f"{token_id}.png")
                                    self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                                    print(f"posted crab {token_id} {is_selling}")
                                    if os.path.isfile(f"{token_id}.png"):
                                        os.remove(f"{token_id}.png")
                                    break
                                    """
                                    
                                    self.context.bot.send_message(chat_id=f'@crabada_{"sales" if is_selling else "listings"}',
                                                                  text=message)
                                    self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                                    break
                            except Exception as e:
                                print("crab", e)
                                await asyncio.sleep(3)

    async def _notify_egg_item(self, infos_family_nft, infos_nft, token_id, price, timestamp_transaction, is_selling=False):
        already_seen = self._get_variable("already_seen", lambda: set())
        if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
            async with self._semaphore:
                infos_family_nft = infos_family_nft["crabada_parents"]
                price = int(round(price * 10**-18, 0))
                price_formatted = "{:,}".format(price)
                price_in_usd_formatted = "${:,.2f}".format(infos_nft["price_usd"])
                tus_text = f"{price_formatted} $TUS ({price_in_usd_formatted})"
                first_column = tus_text
                crabada_parent_1 = infos_family_nft[0]
                crabada_parent_2 = infos_family_nft[1]
                class_parent_1 = crabada_parent_1["class_name"]
                class_parent_2 = crabada_parent_2["class_name"]
                if class_parent_1 == class_parent_2:
                    class_display = class_parent_1
                    class_display = class_display if class_display.lower() not in cool_classes else bold(class_display)
                else:
                    egg_class_1 = class_parent_1
                    egg_class_2 = class_parent_2
                    class_display_1 = egg_class_1 if egg_class_1.lower() not in cool_classes else bold(egg_class_1)
                    class_display_2 = egg_class_2 if egg_class_2.lower() not in cool_classes else bold(egg_class_2)
                    class_display = f"{class_display_1}┃{class_display_2}"
                type_entry = bold("LISTING") if not is_selling else bold("SOLD") + "<aftertime>"
                order_id_pool = self._shared.get("order_id_pool")
                seller_wallet, buyer_wallet, _, _, timestamp_listing, timestamp_selling = order_id_pool.get(infos_nft["order_id"], (None, None, None, None, None, None))
                if is_selling:
                    if timestamp_listing is not None:
                        if timestamp_selling is None:
                            duration_min = abs(timestamp_transaction - float(timestamp_listing))
                        else:
                            duration_min = abs(float(timestamp_selling) - float(timestamp_listing))
                    else:
                        duration_min = None
                    if duration_min is None:
                        type_entry = type_entry.replace("<aftertime>", "")
                    else:
                        human_deltatime = seconds_to_pretty_print(duration_min)
                        type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                    if buyer_wallet is not None and buyer_wallet.lower() == infos_nft['owner'].lower():
                        buyer_seller = infos_nft['owner']
                        buyer_seller_full_name = infos_nft['owner_full_name']
                    else:
                        buyer_wallet = infos_nft["seller_wallet"]
                        buyer_seller = buyer_wallet
                        buyer_seller_full_name = buyer_seller
                else:
                    if seller_wallet is not None and seller_wallet.lower() == infos_nft['owner'].lower():
                        buyer_seller = infos_nft['owner']
                        buyer_seller_full_name = infos_nft['owner_full_name']
                    else:
                        seller_wallet = infos_nft["seller_wallet"]
                        buyer_seller = seller_wallet
                        buyer_seller_full_name = buyer_seller
                        

                    
                buyer_seller_type = "Listed by" if not is_selling else "Bought by"
                url_buyer_seller = infos_nft["url_wallet"]
                message = (
                    f"[{type_entry}] 🥚 {class_display} No.{token_id} at {first_column} on Crabada Marketplace\n" +
                    f"{infos_nft['marketplace_link']}\n" +
                    f"Per-category and speed-enhanced alerts in https://discord.gg/KYwprbzpFd\n" +
                    f"#snibsnib\n" +
                    f"{infos_nft['photos_link']}\n" +
                    f"{buyer_seller_type} {buyer_seller_full_name}({url_buyer_seller})"
                )
                async with self._get_variable(f"sem_{token_id}_{timestamp_transaction}_{price}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
                    if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                        while True:
                            try:
                                if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                                    """
                                    if not os.path.isfile("egg.png"):
                                        wget.download(f"https://i.ibb.co/hXcP49w/egg.png", out=f"egg.png", bar=None)
                                    self._client.update_status_with_media(status=message, filename=f"egg.png")
                                    self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                                    print(f"posted egg {token_id} {is_selling}")
                                    if os.path.isfile("egg.png"):
                                        os.remove(f"egg.png")
                                    break
                                    """
                                    
                                    self.context.bot.send_message(chat_id=f'@crabada_{"sales" if is_selling else "listings"}',
                                                                  text=message)
                                    self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                                    break
                            except Exception as e:
                                print("egg", e)
                                await asyncio.sleep(3)
                #self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, is_selling)}))



    async def crabada_alert_loop(self, seconds=1):
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        current_timestamp = utc_time.timestamp()
        tasks = []
        async with self._get_variable(f"sem_database", lambda: asyncio.Semaphore(value=1)):
            nft_pool = self._shared.get("nft_pool")
            for keys_info, infos_nft in nft_pool.items():
                token_id, timestamp, selling_price, is_selling = keys_info
                is_selling_integer = 1 if is_selling else 0
                if current_timestamp - timestamp <= is_selling_integer*SELLING_ITEM_EXPIRATION + (1-is_selling_integer)*LISTING_ITEM_EXPIRATION:
                    if infos_nft["is_crab"]:
                        tasks.append(asyncio.create_task(self._notify_crab_item(infos_nft, token_id, selling_price, timestamp, is_selling=is_selling)))
                    else:
                        tasks.append(asyncio.create_task(self._notify_egg_item(self, infos_nft, infos_nft, token_id, selling_price, timestamp, is_selling=is_selling)))
                    
            asyncio.gather(*tasks)
            

if __name__ == "__main__":
    
    from telegram.ext import Updater

    u = Updater('5282653712:AAGZHLl38ic5vWuO6hp6200p3BnFN_u-yVk', use_context=True)
    j = u.job_queue
    bot = CrabalertTelegram()
    def callback_alert(context: CallbackContext):
        bot.context = context
        asyncio.run(
            bot.crabada_alert_loop()
        )

    j.run_repeating(callback_alert, interval=2, first=1)
    # Start the Bot
    u.start_polling()

    # Block until you press Ctrl-C or the process receives SIGINT, SIGTERM or
    # SIGABRT. This should be used most of the time, since start_polling() is
    # non-blocking and will stop the bot gracefully.
    u.idle()
