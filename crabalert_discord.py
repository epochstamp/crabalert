from datetime import datetime, timezone
import os
from pathlib import Path
import wget
from discord.ext import tasks
from discord import Embed, File
import memcache
from config import (
    LISTING_ITEM_EXPIRATION,
    SELLING_ITEM_EXPIRATION,
    channel_to_post_listings_with_filters,
    channel_to_post_sellings_with_filters,
    listing_channels_to_display_shortdescrs,
    selling_channels_to_display_shortdescrs,
    cool_classes,
    cool_subclasses,
    THRESOLD_PURE_PROBA,
    channels_emojis,
    CRABMESSAGE_SEM_ID,
    EGGMESSAGE_SEM_ID,
    subclass_map)
from subclasses import calc_subclass_info, subclass_type_map
from utils import (
    download_image,
    seconds_to_pretty_print
)
from discord.utils import get
from discord.ext import commands
import asyncio
from eggs_utils import calc_pure_probability
from classes import classes_to_spacebarsize_map


class CrabalertDiscord(commands.Bot):
    def __init__(self, command_prefix="!", intents=None, variables=None):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self._launched = False
        self._tasks = []
        self._variables = variables if variables is not None else dict()
        Path("images/").mkdir(parents=True, exist_ok=True)
        self._shared = memcache.Client(["127.0.0.1:11211"], debug=0)
        
        
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

    async def _close_all_tasks(self):
        for task in self._tasks:
            try:
                await task.close()
            except:
                pass
        
    async def on_ready(self):
        if not self._launched:
            self._tasks.append(self._crabada_alert_loop.start())
            self._launched = True


    @tasks.loop(seconds=1)
    async def _crabada_alert_loop(self):
        
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
                        infos_family = None
                    else:
                        infos_family = infos_nft
                    tasks.append(asyncio.create_task(self._notify_marketplace_item(infos_nft, infos_family, token_id, selling_price, timestamp, infos_nft["is_crab"], is_selling=is_selling)))
            asyncio.gather(*tasks)

    async def _notify_marketplace_item(self, infos_nft, infos_family, token_id, selling_price, timestamp, is_crab_bool, is_selling=False):
        tasks = []
        already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
        channels_to_post = (
            channel_to_post_listings_with_filters
            if not is_selling else
            channel_to_post_sellings_with_filters
        )
        for channel_id, filter_function in channels_to_post.items():
            channel = self._get_variable(f"channel_{channel_id}", f_value_if_not_exists=lambda: self.get_channel(channel_id))
            infos_family_test = infos_family if not is_crab_bool else None
            if infos_nft["price"] is None:
                infos_nft["price"] = selling_price * 10**18
            if filter_function((infos_nft, infos_family_test)):
                if (token_id, timestamp, channel.id, is_selling) not in already_seen:
                    if is_crab_bool:
                        tasks.append(asyncio.create_task(
                            self.notify_crab_item(
                                infos_nft,
                                token_id,
                                selling_price,
                                timestamp,
                                channel,
                                is_selling=is_selling
                            )
                        ))
                    else:
                        tasks.append(asyncio.create_task(
                            self.notify_egg_item(
                                infos_nft,
                                infos_family,
                                token_id,
                                selling_price,
                                timestamp,
                                channel,
                                is_selling=is_selling
                            )
                        ))
        if tasks != []:
            asyncio.gather(*tasks)
            

    async def notify_crab_item(self, infos_nft, token_id, price, timestamp_transaction, channel, is_selling=False):
        async with self._get_variable(f"sem_{CRABMESSAGE_SEM_ID}_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            # print"crab from timestamp", timestamp_transaction,"will be posted", token_id, "at channel", channel.id)
            marketplace_link = infos_nft["marketplace_link"]
            photos_link = infos_nft["photos_link"]
            price_usd = infos_nft["price_usd"]
            channel_id = channel.id
            tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
            crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", "crabadegg")
            price = int(round(price * 10**-18, 0))
            price_formatted = "{:,}".format(price)
            price_in_usd_formatted = "{:,.2f}".format(price_usd)
            tus_text = f"{tus_emoji} **{price_formatted}**"
            tus_text_len_in_space_bars = sum([1 if c == "1" or c == "." or c == "," else 2 for c in str(price)]) + 6 + 1
            usd_text = f":moneybag: **{price_in_usd_formatted}**"
            purity_text = (':gem: **PURE**' if int(infos_nft['pure_number']) == 6 else ':diamond_shape_with_a_dot_inside: ' + str(infos_nft['pure_number']))
            purity_text_len_in_space_bars = 3 + 1 + (12 if int(infos_nft['pure_number']) == 6 else (1 if int(infos_nft['pure_number']) == 1 else 2))
            breed_text = f"{crabadegg_emoji} {infos_nft['breed_count'] if int(infos_nft['breed_count']) > 0 else '**NO-BREED**'}"
            mining_text = f":pick: {infos_nft['speed'] + infos_nft['critical']}"
            mining_text_len_in_space_bars = 4 + 1 + sum([1 if c == "1" else 2 for c in str(infos_nft['speed'] + infos_nft['critical'])])

            battle_text = f":crossed_swords: {infos_nft['hp'] + infos_nft['armor'] + infos_nft['damage']}"

            max_len_space_text_first_column = max([tus_text_len_in_space_bars, purity_text_len_in_space_bars, mining_text_len_in_space_bars])

            first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
            second_column=purity_text + " "*((max_len_space_text_first_column*2 - purity_text_len_in_space_bars)) + breed_text
            third_column=mining_text + " "*((max_len_space_text_first_column*2 - mining_text_len_in_space_bars)) + battle_text
            subclass = subclass_map.get(infos_nft['crabada_subclass'], 'unknown')
            subclass_display = subclass
            subclass_display = subclass_display if subclass_display.lower() not in cool_subclasses else f"**{subclass_display}**"
            class_display = infos_nft['class_name']
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

            class_display = class_display if class_display.lower() not in cool_classes else f"**{class_display}**"
            order_id_pool = self._shared.get("order_id_pool")
            seller_wallet, buyer_wallet, _, _, timestamp_listing, timestamp_selling = order_id_pool.get(infos_nft["order_id"], (None, None, None, None, None, None))
            type_entry = "**[SOLD<aftertime>]**" if is_selling else "**[LISTING]**"
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
                    buyer_seller_full_name = infos_nft['owner_full_name']
                else:
                    buyer_seller_full_name = buyer_wallet
            else:
                if seller_wallet is not None and seller_wallet.lower() == infos_nft['owner'].lower():
                    buyer_seller_full_name = infos_nft['owner_full_name']
                else:
                    buyer_seller_full_name = seller_wallet
                
            url_buyer_seller = infos_nft["url_wallet"]
            message = (
                f"{type_entry} :crab: {'**PURE**' if int(infos_nft['pure_number']) == 6 else ''}{' **ORIGIN**' if infos_nft['is_origin'] == 1 else ''}{' **GENESIS**' if infos_nft['is_genesis'] == 1 else ''}{' **NO-BREED**' if int(infos_nft['breed_count']) == 0 else ''} {class_display}({emoji_subclass_type} {subclass_display} {n_comp_subclass}/18)\n" +
                f"{first_column}\n" +
                f"{second_column}\n" +
                f"{third_column}"
            )
            asyncio.gather(asyncio.create_task(
                self._send_crab_item_message(token_id, timestamp_transaction, channel, message, marketplace_link, photos_link, url_buyer_seller, buyer_seller_full_name, is_selling=is_selling)
            ))

    async def notify_egg_item(self, infos_nft, infos_family_nft, token_id, price, timestamp_transaction, channel, is_selling=False):
        async with self._get_variable(f"sem_{EGGMESSAGE_SEM_ID}_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            type_entry = "**[SOLD<aftertime>]**" if is_selling else "**[LISTING]**"
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
                if buyer_wallet.lower() == infos_nft['owner'].lower():
                    buyer_seller = infos_nft['owner']
                    buyer_seller_full_name = infos_nft['owner_full_name']
                else:
                    buyer_seller = buyer_wallet
                    buyer_seller_full_name = buyer_wallet
            else:
                if seller_wallet.lower() == infos_nft['owner'].lower():
                    buyer_seller = infos_nft['owner']
                    buyer_seller_full_name = infos_nft['owner_full_name']
                else:
                    buyer_seller = seller_wallet
                    buyer_seller_full_name = seller_wallet
            url_buyer_seller = infos_nft["url_wallet"]
            channel_id = channel.id
            price_usd = infos_nft["price_usd"]
            infos_family_nft = infos_family_nft["crabada_parents"]
            crabada_parent_1 = infos_family_nft[0]
            crabada_parent_2 = infos_family_nft[1]
            class_parent_1 = crabada_parent_1["class_name"]
            class_parent_2 = crabada_parent_2["class_name"]
            dna_parent_1 = crabada_parent_1["dna"]
            dna_parent_2 = crabada_parent_2["dna"]
            egg_purity_probability = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1), 4)
            if egg_purity_probability.is_integer():
                egg_purity_probability = int(egg_purity_probability)
            price = int(round(price * 10**-18, 0))
            price_formatted = "{:,}".format(price)
            price_in_usd_formatted = "{:,.2f}".format(price_usd)
            tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
            tus_text = f"{tus_emoji} **{price_formatted}**"
            tus_text_len_in_space_bars = sum([1 if c == "1" or c == "." or c == "," else 2 for c in str(price)]) + 6 + 1
            usd_text = f":moneybag: **{price_in_usd_formatted}**"
            marketplace_link = infos_nft["marketplace_link"]
            egg_purity_probability_1 = egg_purity_probability
            egg_purity_probability_2 = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_2), 4)
            if egg_purity_probability_2.is_integer():
                egg_purity_probability_2 = int(egg_purity_probability_2)
            egg_class_1 = class_parent_1
            egg_class_2 = class_parent_2
            egg_class_display_1 = egg_class_1 if egg_class_1.lower() not in cool_classes else f"**{egg_class_1}**"
            egg_class_display_2 = egg_class_2 if egg_class_2.lower() not in cool_classes else f"**{egg_class_2}**"
            egg_class_display = f"({egg_class_display_1}┃{egg_class_display_2})" if egg_class_1 != egg_class_2 else f"{egg_class_display_1}"
            egg_class_text_1 = f"<crab1> {egg_class_display_1}"
            egg_class_text_2 = f"<crab2> {egg_class_display_2}"
            egg_class_1_text_len_in_space_bars = 4 + 1 + classes_to_spacebarsize_map.get(class_parent_1.upper(), 1)
            emoji_pure_1 = ":gem:" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
            emoji_pure_2 = ":gem:" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
            probability_display_1 = f"**{round(egg_purity_probability_1*100, 2)}%**" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else f"{round(egg_purity_probability_1*100, 2)}%"
            probability_display_2 = f"**{round(egg_purity_probability_2*100, 2)}%**" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else f"{round(egg_purity_probability_2*100, 2)}%"
            purity_probability_text_1 = f"{emoji_pure_1} {probability_display_1}"
            purity_probability_text_1_len_in_space_bars = 4 + 1 + sum([1 if c == "1" or c == "." else 2 for c in str(int(round(egg_purity_probability_1*100, 2)))])
            purity_probability_text_2 = f"{emoji_pure_2} {probability_display_2}"

            max_len_space_text_first_column = max([tus_text_len_in_space_bars, egg_class_1_text_len_in_space_bars, purity_probability_text_1_len_in_space_bars])
            first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
            second_column = f"{egg_class_text_1}" + " "*((max_len_space_text_first_column*2 - egg_class_1_text_len_in_space_bars)) + f"{egg_class_text_2}"
            third_column = purity_probability_text_1 + " "*((max_len_space_text_first_column*2 - purity_probability_text_1_len_in_space_bars)) + purity_probability_text_2
            message = (
                f"{first_column}\n" +
                f"{second_column}\n" +
                f"{third_column}\n"
            )
            probability_pure = round(0.5*egg_purity_probability_1 + 0.5*egg_purity_probability_2, 2)
            if probability_pure.is_integer():
                probability_pure = int(probability_pure)
            infos_egg = {
                "class_name_1": egg_class_1,
                "class_name_2": egg_class_2,
                "probability_pure": probability_pure
            }
            header_message = f"{type_entry} <crabadegg> {'**PURE** ' if infos_egg['probability_pure'] == 1 else ''}{egg_class_display} \n"
            footer_message = (
                ""
            )
            crab_1_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab1", ":crab1:")#"<:crab1:934087822254694441>" if channel_id == 932591668597776414 else "<:crab_1:934075767602700288>"
            crab_2_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab2", ":crab2:")#"<:crab2:934087853732921384>" if channel_id == 932591668597776414 else "<:crab_2:934076410132332624>"
            
            crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", ":crabadegg:")
            channels_to_display_shortdescrs = (
                listing_channels_to_display_shortdescrs if not is_selling else
                selling_channels_to_display_shortdescrs
            )
            if channel_id in channels_to_display_shortdescrs:
                message_egg = (
                    f"{first_column}"
                )
                footer_message_egg = (
                    ""
                )
                header_message_egg = (
                    f"{type_entry} <crabadegg> {egg_class_display} \n"
                )
                    
            else:
                header_message_egg = header_message
                footer_message_egg = footer_message
                message_egg = message
            asyncio.gather(asyncio.create_task(
                self._send_egg_item_message(message_egg, header_message_egg, footer_message_egg, crab_2_emoji, tus_emoji, crab_1_emoji, crabadegg_emoji, token_id, timestamp_transaction, channel, marketplace_link, url_buyer_seller, buyer_seller_full_name, is_selling=is_selling)
            ))

    async def _send_crab_item_message(self, token_id, timestamp_transaction, channel, message, marketplace_link, photos_link, url_buyer_seller, buyer_seller_full_name, is_selling=False):
        already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
        async with self._get_variable(f"semaphore_crab_message_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            if (token_id, timestamp_transaction, channel.id, is_selling) not in already_seen:
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, channel.id, is_selling)}))
                embed = Embed(
                    title=marketplace_link
                )
                embed.add_field(
                    name=f"{'Bought by' if is_selling else 'Listed by'}", value=f"[{buyer_seller_full_name}]({url_buyer_seller})", inline=False
                )
                if not os.path.isfile("images/{token_id}.png"):
                    download_image(photos_link, out=f"images/{token_id}.png")
                try:
                    await channel.send(message, embed=embed, file=File(f"images/{token_id}.png"))
                except BaseException as e:
                    print(e)
                    already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
                    self._set_sync_variable("already_seen", already_seen.difference({(token_id, timestamp_transaction, channel.id, is_selling)}))
                    

    async def _send_egg_item_message(self, message_egg_in, header_message_egg, footer_message_egg, crab_2_emoji, tus_emoji, crab_1_emoji, crabadegg_emoji, token_id, timestamp_transaction, channel, marketplace_link, url_buyer_seller, buyer_seller_full_name, is_selling=False):
        message_egg = header_message_egg + message_egg_in + footer_message_egg
        message_egg = message_egg.replace("<crab1>", crab_1_emoji).replace("<crab2>", crab_2_emoji).replace("<tus>", tus_emoji).replace("<crabadegg>" ,crabadegg_emoji)
        already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
        
        async with self._get_variable(f"semaphore_egg_message_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            if (token_id, timestamp_transaction, channel.id, is_selling) not in already_seen:
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, channel.id, is_selling)}))
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, channel.id, is_selling)}))
                embed = Embed(
                    title=marketplace_link
                )
                embed.add_field(
                    name=f"{'Bought by' if is_selling else 'Listed by'}", value=f"[{buyer_seller_full_name}]({url_buyer_seller})", inline=False
                )
                if not os.path.isfile("images/egg.png"):
                    download_image(f"https://i.ibb.co/hXcP49w/egg.png", out=f"images/egg.png")
                try:
                    await channel.send(message_egg, embed=embed, file=File("images/egg.png"))
                except BaseException as e:
                    print(e)
                    already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
                    self._set_sync_variable("already_seen", already_seen.difference({(token_id, timestamp_transaction, channel.id, is_selling)}))
                
