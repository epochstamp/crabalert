import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
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
from subclasses import calc_subclass_info, subclass_type_map
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
import tweepy
from asyncio import Semaphore

SLEEP_TIME = 11

class CrabalertTwitter:

    def __init__(self, variables=None):
        CONSUMER_KEY = 'eScR54r8TnTeinfZd4WzeC3Ez'
        CONSUMER_SECRET = 'RRNwamMerbzyyKqLjw5SkDKwvTLk9z1uCZNeAI8oVEsnhoFshW'

        # Create a new Access Token
        ACCESS_TOKEN = '1489533611447570433-6IttKleRLP0Wa1tUPqklVymh94VJli'
        ACCESS_SECRET = '0kB66Fh52QyQS1CHIakSx5EVqjo0FWpsfh1l377WFKqOR'
        BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAANGcYwEAAAAA28SAMaN6j4W2pSQ61w98An1mCZU%3D6ljcSJBel31UmA7tSm4AuJrRaAnOCq4lwk0n4SAvLaBlrIQE4m'
        """
        self._client = tweepy.Client(
            bearer_token=BEARER_TOKEN,
            consumer_key=CONSUMER_KEY,
            consumer_secret=CONSUMER_SECRET,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_SECRET
        )
        """
        auth=tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN,ACCESS_SECRET)
        self._client=tweepy.API(auth)
        self._semaphore = Semaphore(value=1)
        self._variables = dict() if variables is None else variables
        Path("images/").mkdir(parents=True, exist_ok=True)

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
        async with self._semaphore:
            if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                price_formatted = "{:,}".format(price)
                price_in_usd_formatted = "${:,.2f}".format(price*get_price_tus_in_usd())
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
                if is_selling:
                    db = open_database()
                    query = f"""
                        SELECT timestamp from crabada_listings where token_id={token_id} ORDER BY timestamp DESC LIMIT 1
                    """
                    data = execute_query(db, query)
                    close_database(db)
                    if len(data) > 0:
                        duration_min = abs(timestamp_transaction - float(data[0][0]))
                    else:
                        duration_min = None
                    if duration_min is None:
                        type_entry = type_entry.replace("<aftertime>", "")
                    else:
                        human_deltatime = seconds_to_pretty_print(duration_min)
                        type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                        

                    
                buyer_seller_type = "Listed by" if not is_selling else "Bought by"
                buyer_seller = f"https://snowtrace.io/address/{infos_nft['owner']}"
                buyer_seller_full_name = infos_nft['owner_full_name']
                message = (
                    f"[{type_entry}] 🦀 {class_display}({emoji_subclass_type} {subclass_display} {n_comp_subclass}/18) No.{token_id} at {first_column}\n" +
                    f"Per-category and speed-enhanced alerts in https://discord.gg/KYwprbzpFd\n" +
                    f"https://marketplace.crabada.com/crabada/{token_id}\n" +
                    f"{buyer_seller_type} {buyer_seller_full_name}({buyer_seller})"
                )
                async with self._get_variable(f"sem_{token_id}_{timestamp_transaction}_{price}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
                    if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                        try:
                            if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                                if not os.path.isfile(f"images/{token_id}.png"):
                                    wget.download(f"https://photos.crabada.com/{token_id}.png", out=f"images/{token_id}.png", bar=None)
                                self._client.update_status_with_media(status=message, filename=f"images/{token_id}.png")
                                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                        except Exception as e:
                            print(f"crab {token_id}", type(e), e)
                            reposts = self._get_variable(f"{'selling' if is_selling else 'listing'}_reposts", lambda : set())
                            self._set_sync_variable(f"{'selling' if is_selling else 'listing'}_reposts", reposts.union({(json.dumps(infos_nft), None, token_id, price, timestamp_transaction)}))

    async def _notify_egg_item(self, infos_family_nft_init, infos_nft, token_id, price, timestamp_transaction, is_selling=False):
        already_seen = self._get_variable("already_seen", lambda: set())
        async with self._semaphore:
            if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                infos_family_nft = infos_family_nft_init["crabada_parents"]
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
                        SELECT timestamp from crabada_listings where token_id={token_id} ORDER BY timestamp DESC LIMIT 1
                    """
                    data = execute_query(db, query)
                    close_database(db)
                    if len(data) > 0:
                        duration_min = abs(timestamp_transaction - float(data[0][0]))
                    else:
                        duration_min = None
                    if duration_min is None:
                        type_entry = type_entry.replace("<aftertime>", "")
                    else:
                        human_deltatime = seconds_to_pretty_print(duration_min)
                        type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                        

                    
                buyer_seller_type = "Listed by" if not is_selling else "Bought by"
                buyer_seller = f"https://snowtrace.io/address/{infos_nft['owner']}"
                buyer_seller_full_name = infos_nft['owner_full_name']
                message = (
                    f"[{type_entry}] 🥚 {class_display} No.{token_id} {first_column}\n" +
                    f"Per-category and speed-enhanced alerts in https://discord.gg/KYwprbzpFd\n" +
                    f"https://marketplace.crabada.com/crabada/{token_id}\n" +
                    f"{buyer_seller_type} {buyer_seller_full_name}({buyer_seller})"
                )
                async with self._get_variable(f"sem_{token_id}_{timestamp_transaction}_{price}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
                    if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                        try:
                            if (token_id, timestamp_transaction, price, is_selling) not in already_seen:
                                if not os.path.isfile("images/egg.png"):
                                    wget.download(f"https://i.ibb.co/hXcP49w/egg.png", out=f"images/egg.png", bar=None)
                                self._client.update_status_with_media(status=message, filename=f"images/egg.png")
                                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, price, is_selling)}))
                                print(f"posted egg {token_id} {is_selling}")
                        except Exception as e:
                            print(f"egg {token_id}", type(e), e)
                            reposts = self._get_variable(f"{'selling' if is_selling else 'listing'}_reposts", lambda : set())
                            self._set_sync_variable(f"{'selling' if is_selling else 'listing'}_reposts", reposts.union({(json.dumps(infos_nft), json.dumps(infos_family_nft_init), token_id, price, timestamp_transaction)}))
                #self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, is_selling)}))



    """
    SUBSCRIPTION MANAGEMENT
    """
    async def _crabada_listing_alert_loop(self, seconds=2):
        while True:
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
            close_database(connection)
            for token_id, selling_price, timestamp, is_crab, infos_nft, infos_family in data:
                already_seen = self._get_variable("already_seen", lambda: set())
                if (token_id, timestamp, selling_price, False) not in already_seen:
                    is_crab = is_crab.lower() == "true"
                    infos_nft = json.loads(infos_nft)
                    if infos_family != "":
                        infos_family = json.loads(infos_family)
                    if is_crab:
                        tasks.append(asyncio.create_task(self._notify_crab_item(infos_nft, token_id, selling_price, timestamp, is_selling=False)))
                    else:
                        tasks.append(asyncio.create_task(self._notify_egg_item(infos_family, infos_nft, token_id, selling_price, timestamp, is_selling=False)))
            listing_reposts = self._get_variable(f"listing_reposts", lambda : set())
            for infos_nft, infos_family, token_id, selling_price, timestamp_transaction in listing_reposts:
                if current_timestamp - timestamp_transaction <= LISTING_ITEM_EXPIRATION and (token_id, timestamp, selling_price, False) not in already_seen:
                    if infos_family is None:
                        tasks.append(asyncio.create_task(self._notify_crab_item(json.loads(infos_nft), token_id, selling_price, timestamp, is_selling=False)))
                    else:
                        tasks.append(asyncio.create_task(self._notify_egg_item(json.loads(infos_family), json.loads(infos_nft), token_id, selling_price, timestamp, is_selling=False)))
                else:
                    self._set_sync_variable(f"listing_reposts", listing_reposts.difference({(infos_nft, infos_family, token_id, selling_price, timestamp_transaction)}))
            if tasks != []:
                asyncio.gather(*tasks)
            await asyncio.sleep(seconds)

    async def _crabada_selling_alert_loop(self, seconds=2):
        while True:
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
            close_database(connection)
            for token_id, selling_price, timestamp, is_crab, infos_nft, infos_family in data:
                already_seen = self._get_variable("already_seen", lambda: set())
                if (token_id, timestamp, selling_price, True) not in already_seen:
                    infos_nft = json.loads(infos_nft)
                    is_crab = is_crab.lower() == "true"
                    if infos_family != "":
                        infos_family = json.loads(infos_family)
                    if is_crab:
                        tasks.append(asyncio.create_task(self._notify_crab_item(infos_nft, token_id, selling_price, timestamp, is_selling=True)))
                    else:
                        tasks.append(asyncio.create_task(self._notify_egg_item(infos_family, infos_nft, token_id, selling_price, timestamp, is_selling=True)))
            selling_reposts = self._get_variable(f"selling_reposts", lambda : set())
            for infos_nft, infos_family, token_id, selling_price, timestamp_transaction in selling_reposts:
                if current_timestamp - timestamp_transaction <= SELLING_ITEM_EXPIRATION and (token_id, timestamp, selling_price, True) not in already_seen:
                    if infos_family is None:
                        tasks.append(asyncio.create_task(self._notify_crab_item(json.loads(infos_nft), token_id, selling_price, timestamp, is_selling=True)))
                    else:
                        tasks.append(asyncio.create_task(self._notify_egg_item(json.loads(infos_family), json.loads(infos_nft), token_id, selling_price, timestamp, is_selling=True)))
                else:
                    self._set_sync_variable(f"selling_reposts", selling_reposts.difference({(infos_nft, infos_family, token_id, selling_price, timestamp_transaction)}))
            if tasks != []:
                asyncio.gather(*tasks)
            await asyncio.sleep(seconds)
        

    async def run(self):
        crabada_listing_alert_loop_task = asyncio.create_task(self._crabada_listing_alert_loop())
        crabada_selling_alert_loop_task = asyncio.create_task(self._crabada_selling_alert_loop())
        await asyncio.gather(
            crabada_listing_alert_loop_task,
            crabada_selling_alert_loop_task
        )
        print("lu")

        #asyncio.create_task(self._run_fetch_and_store_crabada_selling_transactions_loop())
        #asyncio.create_task(self._run_fetch_and_store_crabada_listing_transactions_loop())
            

if __name__ == "__main__":
    variables = None
    bot = CrabalertTwitter(variables=variables)
    asyncio.run(bot.run())
