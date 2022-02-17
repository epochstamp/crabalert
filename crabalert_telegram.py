import asyncio
from datetime import datetime, timezone
import os
import wget
from config import (
    APICRABADA_SEM_ID,
    COINS,
    NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS,
    PAYMENT_SEM_ID,
    SELLING_ITEM_EXPIRATION,
    SNOWTRACE_SEM_ID,
    SPAN_TIMESTAMP,
    HEADERS,
    SNOWTRACE_API_KEY,
    TIMEOUT,
    LISTING_ITEM_EXPIRATION,
    cool_classes,
    cool_subclasses,
    subclass_map
)
from utils import (
    blockchain_urls,
    close_database,
    download_image,
    execute_query,
    get_price_tus_in_usd,
    get_transactions_between_blocks,
    get_transactions_between_blocks_async,
    is_crab,
    is_valid_marketplace_listing_transaction,
    iblock_near,
    is_valid_marketplace_selling_transaction,
    async_http_get_request_with_callback_on_result_v2,
    get_current_block,
    nothing,
    open_database,
    bold,
    safe_json,
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
                price_formatted = "{:,}".format(price)
                price_in_usd_formatted = "${:,.2f}".format(price*get_price_tus_in_usd())
                tus_text = f"{price_formatted} $TUS ({price_in_usd_formatted})"
                first_column = tus_text
                subclass_display = subclass_map.get(infos_nft['crabada_subclass'], 'unknown')
                subclass_display = subclass_display if subclass_display.lower() not in cool_subclasses else bold(subclass_display)
                class_display = infos_nft['class_name']
                class_display = class_display if class_display.lower() not in cool_classes else bold(class_display)
                type_entry = bold("LISTING") if not is_selling else bold("SOLD") + "<aftertime>"
                if is_selling:
                    db = open_database()
                    query = f"""
                        SELECT timestamp from crabada_listings where token_id={token_id}
                    """
                    data = execute_query(db, query)
                    duration_min = float("+inf")
                    duration_argmin = None
                    for ts, in data:
                        if timestamp_transaction - ts <= duration_min:
                            duration_min = timestamp_transaction - ts
                            duration_argmin = ts
                    if duration_argmin is None:
                        type_entry = type_entry.replace("<aftertime>", "")
                    else:
                        
                        human_deltatime = seconds_to_pretty_print(duration_min)
                        type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                        

                    close_database(db)
                buyer_seller_type = "Listed by:" if not is_selling else "Bought by:"
                buyer_seller = f"https://snowtrace.io/address/{infos_nft['owner']}"
                buyer_seller_full_name = infos_nft['owner_full_name']
                link = f"https://photos.crabada.com/{token_id}.png"
                message = (
                    f"[{type_entry}] 🦀 {class_display}({subclass_display}) No.{token_id} at {first_column} on Crabada Marketplace\n" +
                    f"{link}\n"
                    f"Per-category and speed-enhanced alerts in https://discord.gg/KYwprbzpFd\n" +
                    f"#snibsnib\n" +
                    f"https://marketplace.crabada.com/crabada/{token_id}\n" +
                    f"{buyer_seller_type} {buyer_seller_full_name}({buyer_seller})"
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
                                    self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                                    self.context.bot.send_message(chat_id=f'@crabada_{"sales" if is_selling else "listings"}',
                                                                  text=message)
                                    break
                            except Exception as e:
                                print("crab", e)
                                await asyncio.sleep(3)

    async def _notify_egg_item(self, infos_family_nft, infos_nft, token_id, price, timestamp_transaction, is_selling=False):
        already_seen = self._get_variable("already_seen", lambda: set())
        if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
            async with self._semaphore:
                infos_family_nft = infos_family_nft["crabada_parents"]
                price_formatted = "{:,}".format(price)
                price_in_usd_formatted = "${:,.2f}".format(price*get_price_tus_in_usd())
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
                if is_selling:
                    db = open_database()
                    query = f"""
                        SELECT timestamp from crabada_listings where token_id={token_id}
                    """
                    data = execute_query(db, query)
                    duration_min = float("+inf")
                    duration_argmin = None
                    for ts, in data:
                        if timestamp_transaction - ts <= duration_min:
                            duration_min = timestamp_transaction - ts
                            duration_argmin = ts
                    if duration_argmin is None:
                        type_entry = type_entry.replace("<aftertime>", "")
                    else:
                        human_deltatime = seconds_to_pretty_print(duration_min)
                        type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                        

                    close_database(db)
                buyer_seller_type = "Listed by:" if not is_selling else "Bought by:"
                buyer_seller = f"https://snowtrace.io/address/{infos_nft['owner']}"
                buyer_seller_full_name = infos_nft['owner_full_name']
                link = "https://i.ibb.co/hXcP49w/egg.png"
                message = (
                    f"[{type_entry}] 🥚 {class_display} No.{token_id} at {first_column} on Crabada Marketplace\n" +
                    f"{link}\n" +
                    f"Per-category and speed-enhanced alerts in https://discord.gg/KYwprbzpFd\n" +
                    f"#snibsnib\n" +
                    f"https://marketplace.crabada.com/crabada/{token_id}\n" +
                    f"{buyer_seller_type} {buyer_seller_full_name}({buyer_seller})"
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
                                    self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                                    self.context.bot.send_message(chat_id=f'@crabada_{"sales" if is_selling else "listings"}',
                                                                  text=message)
                                    break
                            except Exception as e:
                                print("egg", e)
                                await asyncio.sleep(3)
                #self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, is_selling)}))



    """
    SUBSCRIPTION MANAGEMENT
    """
    async def crabada_listing_alert_loop(self):
        try:
            connection = open_database()
        except Exception as e:
            #TODO : logging
            return
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        current_timestamp = utc_time.timestamp()
        query = f"""
            SELECT * FROM crabada_listings WHERE {current_timestamp} - timestamp <= {LISTING_ITEM_EXPIRATION}
        """
        tasks = []
        data = execute_query(connection, query)
        for token_id, selling_price, timestamp, is_crab, infos_nft, infos_family in data:
            is_crab = is_crab.lower() == "true"
            infos_nft = json.loads(infos_nft)
            if infos_family != "":
                infos_family = json.loads(infos_family)
            if is_crab:
                tasks.append(asyncio.create_task(self._notify_crab_item(infos_nft, token_id, selling_price, timestamp, is_selling=False)))
            else:
                tasks.append(asyncio.create_task(self._notify_egg_item(infos_family, infos_nft, token_id, selling_price, timestamp, is_selling=False)))
        if tasks != []:
            asyncio.gather(*tasks)

    async def crabada_selling_alert_loop(self):
        try:
            connection = open_database()
        except Exception as e:
            #TODO : logging
            return
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        current_timestamp = utc_time.timestamp()
        query = f"""
            SELECT * FROM crabada_sellings WHERE {current_timestamp} - timestamp <= {SELLING_ITEM_EXPIRATION}
        """
        tasks = []
        data = execute_query(connection, query)
        for token_id, selling_price, timestamp, is_crab, infos_nft, infos_family in data:
            infos_nft = json.loads(infos_nft)
            is_crab = is_crab.lower() == "true"
            if infos_family != "":
                infos_family = json.loads(infos_family)
            if is_crab:
                tasks.append(asyncio.create_task(self._notify_crab_item(infos_nft, token_id, selling_price, timestamp, is_selling=True)))
            else:
                tasks.append(asyncio.create_task(self._notify_egg_item(infos_family, infos_nft, token_id, selling_price, timestamp, is_selling=True)))
        if tasks != []:
            asyncio.gather(*tasks)

        #asyncio.create_task(self._run_fetch_and_store_crabada_selling_transactions_loop())
        #asyncio.create_task(self._run_fetch_and_store_crabada_listing_transactions_loop())
            

if __name__ == "__main__":
    """
    variables = None
    bot = CrabalertTelegram(variables=variables)
    #bot.run()
    my_token = '5282653712:AAGZHLl38ic5vWuO6hp6200p3BnFN_u-yVk'
    updater = Updater(my_token, use_context=True)
    job_queue = updater.job_queue

    def send_message_job(context):
        tasks = []
        tasks.append(bot.crabada_selling_alert_loop(context))
        tasks.append(bot.crabada_listing_alert_loop(context))
        asyncio.run(asyncio.gather(*tasks))
        context.bot.send_message(chat_id='1001650187597',text='job executed')

    job_queue.run_repeating(send_message_job,interval=10.0,first=0.0)
    updater.start_polling()
    updater.idle()
    """
    
    from telegram.ext import Updater

    u = Updater('5282653712:AAGZHLl38ic5vWuO6hp6200p3BnFN_u-yVk', use_context=True)
    j = u.job_queue
    bot = CrabalertTelegram()
    def callback_listing(context: CallbackContext):
        bot.context = context
        asyncio.run(
            bot.crabada_listing_alert_loop()
        )
    def callback_selling(context: CallbackContext):
        bot.context = context
        asyncio.run(
            bot.crabada_selling_alert_loop()
        )

    j.run_repeating(callback_listing, interval=2, first=1)
    j.run_repeating(callback_selling, interval=2, first=1)
    # Start the Bot
    u.start_polling()

    # Block until you press Ctrl-C or the process receives SIGINT, SIGTERM or
    # SIGABRT. This should be used most of the time, since start_polling() is
    # non-blocking and will stop the bot gracefully.
    u.idle()
