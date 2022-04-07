import json
import os
from adfly import AdflyApi

from subclasses import calc_subclass_info

WAITING_BEFORE_RECONNECT = 5
SPAN = 100
SPAN_PAYMENTS=10000
SPAN_TIMESTAMP = 20
SNOWTRACE_SEM_ID = 0
APICRABADA_SEM_ID = 1
ADFLY_SEM_ID = 2
CRABALERT_SEM_ID = 3
CRABREFRESH_SEM_ID = 4
CRABMESSAGE_SEM_ID = 5
EGGMESSAGE_SEM_ID = 6
PAYMENT_SEM_ID = 7
MEM_MAX = 15
WALLET_PATTERN = "^0x[a-fA-F0-9]{40}$"
RATE = 1.3
TUS_CONTRACT_ADDRESS = "0xf693248F96Fe03422FEa95aC0aFbBBc4a8FdD172"
USDT_CONTRACT_ADDRESS = "0xc7198437980c041c805a1edcba50c1ce5db95118"
THRESOLD_PURE_PROBA = 0.8
ID_TUS_BOT = 910250184271867924
ID_SERVER = 932475138434277377
ID_COMMAND_CENTER = 933453311967887370
TIMEOUT = 4
SNOWTRACE_API_KEY = "KNDGDGKUAJ3UT1F8BHZDT61P2X453KGVAA"
NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS = 7
MINIMUM_PAYMENT = 0.5
ADFLY_USER_ID = 26237719
ADFLY_PUBLIC_KEY = "3584d4107dc520de199b2a023b8c9a2b"
ADFLY_PRIVATE_KEY = "0b66ae48-4171-46c6-8c2a-2228358521bd"
HEADERS = {'User-Agent': 'Mozilla/5.0'}
MONTHLY_RATE = 9
DURATION_TRIAL = 3600*24*7
LISTING_ITEM_EXPIRATION = 30
SELLING_ITEM_EXPIRATION = 30
MAX_TIMEOUT_BEFORE_KILL_SNOWTRACE = 2
MAX_TIMEOUT_BEFORE_KILL_CRABADAPI = 4


stablecoins = {
    "0xc7198437980c041c805a1edcba50c1ce5db95118".lower()
}



COINS = {
    "0xc7198437980c041c805a1edcba50c1ce5db95118".lower(): 6,
    TUS_CONTRACT_ADDRESS.lower(): 18
}

COINS_SYMBOL = {
    "0xc7198437980c041c805a1edcba50c1ce5db95118".lower(): "USDT.e",
    TUS_CONTRACT_ADDRESS.lower(): "TUS"
}

channels_emojis = {
    932591668597776414: {
        "tus": "<:tus:932645938944688148>",
        "crabadegg": "<:crabadegg:932909072653615144>",
        "crab1": "<:crab1:934087822254694441>",
        "crab2": "<:crab2:934087853732921384>"
    },
    935237809697095723: {
        "tus": "<:tus:935299315906256978>",
        "crabadegg": "<:crabadegg:935299133785387058>",
        "crab1": "<:crab_1:935299456658726933>",
        "crab2": "<:crab_2:935299577861509121>"
    },
    "default": {
        "tus": "<:tus:932797205767659521>",
        "crabadegg": "<:crabadegg:932809750624743454>",
        "crab1": "<:crab_1:934075767602700288>",
        "crab2": "<:crab_2:934076410132332624>"
    }
}

subclass_map = {
            1: "Emeraldo",
            2: "Crazor",
            3: "Vocrano",
            4: "Spikey",
            5: "Frozo",
            6: "Cratos",
            7: "Rubie",
            8: "Amida",
            16: "Staro",
            17: "Craken",
            18: "Crobster",
            19: "Cralmon",
            20: "Creasure",
            21: "Crucket",
            22: "Crele",
            23: "Crotopus",
            31: "Bitcoin",
            32: "Cardano",
            33: "Near",
            34: "Ether",
            35: "Cz",
            36: "Fantom",
            37: "Avalanche",
            38: "Solana",
            46: "Cragma",
            47: "C-Rex",
            48: "Charoite",
            49: "Rocco",
            50: "Chief",
            51: "Cropion",
            52: "Crazurite",
            53: "Crava",
            61: "Twinner",
            62: "Onepunch",
            63: "Cropter",
            64: "Plasma",
            65: "Lasery",
            66: "Redeye",
            67: "Crocket",
            68: "Gear",
            76: "Skul",
            77: "Cragon",
            78: "Crombie",
            79: "Deadeye",
            80: "Cranosis",
            81: "Crailer",
            82: "Camun-Ra",
            83: "Crauldron",
            91: "Pearlio",
            92: "Lapidari",
            93: "Paraiba",
            94: "Cramethyst",
            95: "Cranet",
            96: "Croyo",
            97: "Earl Cray",
            98: "Magnifiso",
            106: "Natura",
            107: "Freshie",
            108: "Adam",
            109: "Eva",
            110: "Bulbie",
            111: "Celon",
            112: "Cranana",
            113: "Crawberry"
        }

subclass_type_map = {
            1: "Tank",
            2: "Damage",
            3: "Damage",
            4: "Tank",
            5: "Buff",
            6: "Tank",
            7: "Buff",
            8: "Damage",
            16: "Tank",
            17: "Tank",
            18: "Damage",
            19: "Buff",
            20: "Tank",
            21: "Damage",
            22: "Buff",
            23: "Damage",
            31: "Damage",
            32: "Tank",
            33: "Damage",
            34: "Tank",
            35: "Damage",
            36: "Buff",
            37: "Tank",
            38: "Damage",
            46: "Tank",
            47: "Tank",
            48: "Damage",
            49: "Damage",
            50: "Buff",
            51: "Tank",
            52: "Buff",
            53: "Damage",
            61: "Damage",
            62: "Damage",
            63: "Buff",
            64: "Tank",
            65: "Damage",
            66: "Tank",
            67: "Buff",
            68: "Tank",
            76: "Tank",
            77: "Damage",
            78: "Buff",
            79: "Damage",
            80: "Damage",
            81: "Tank",
            82: "Tank",
            83: "Buff",
            91: "Damage",
            92: "Tank",
            93: "Tank",
            94: "Damage",
            95: "Tank",
            96: "Damage",
            97: "Buff",
            98: "Buff",
            106: "Tank",
            107: "Buff",
            108: "Tank",
            109: "Damage",
            110: "Buff",
            111: "Tank",
            112: "Damage",
            113: "Damage"
        }

def get_parent_class(infos_family: dict, parent: int):
    lst_parents = infos_family.get("crabada_parents", [])
    if len(lst_parents) != 2:
        return ""
    return lst_parents[parent].get("class_name", "")

def get_probability_pure(infos_family: dict):
    from eggs_utils import calc_pure_probability
    lst_parents = infos_family.get("crabada_parents", [])
    if len(lst_parents) != 2:
        return 0
    crabada_parent_1 = lst_parents[0]
    crabada_parent_2 = lst_parents[1]
    class_parent_1 = crabada_parent_1["class_name"]
    class_parent_2 = crabada_parent_2["class_name"]
    dna_parent_1 = crabada_parent_1["dna"]
    dna_parent_2 = crabada_parent_2["dna"]
    egg_purity_probability = calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1)
    egg_purity_probability_2 = calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_2)
    probability_pure = 0.5*egg_purity_probability + 0.5*egg_purity_probability_2
    return probability_pure

def is_below_floor_price(price):
    if not os.path.isfile("floor_prices.json"):
        return False
    floor_prices = json.load(open("floor_prices.json"))
    return price <= floor_prices[-1][1]

channel_to_post_listings_with_filters = {
    #Special
    958728365861396550: lambda x: round(float(x[0].get("price", float("+inf")))*10**-18, 0) <= 5000,
    959174623247868026: lambda x: round(float(x[0].get("price", float("+inf")))*10**-18, 0) <= 5000,
    961248732945469511: lambda x: x[1] is None and round(float(x[0].get("price", float("+inf")))*10**-18, 0) <= 15200 and x[0].get("class_name", "").lower() == "bulk" and x[0].get("pure_number", -1) == 6,
    961701024089903104: lambda x: sum([(1 if sbc.lower() == subclass_map.get(int(x[0].get("crabada_subclass", "")), 'unknown').lower() else 0) for sbc in calc_subclass_info(x[0].get("dna", ""))]) >= 15,
    #Crabs and all
    932591668597776414: lambda x: True,
    933456755395006495: lambda x: True,
    935237809697095723: lambda x: True,
    951797923086213140: lambda x: round(float(x[0].get("price", float("+inf")))*10**-18, 0) <= 10000,
    951798251139522610: lambda x: round(float(x[0].get("price", float("+inf")))*10**-18, 0) <= 12000,
    951798278691881000: lambda x: round(float(x[0].get("price", float("+inf")))*10**-18, 0) <= 14000,
    951798307989114900: lambda x: round(float(x[0].get("price", float("+inf")))*10**-18, 0) <= 16000,
    951797761601318912: lambda x: is_below_floor_price(float(x[0].get("price", float("+inf")))*10**-18),
    938865303167836230: lambda x: x[1] is None and x[0].get("class_name", None) is not None,
    933456911913848912: lambda x: x[1] is None and x[0].get("pure_number", -1) is not None and x[0].get("pure_number", -1) == 6,
    933457087369978016: lambda x: x[1] is None and x[0].get("class_name", "") is not None and x[0].get("class_name", "").lower() == "prime",
    933399063330701414: lambda x: x[1] is None and x[0].get("breed_count", -1) is not None and x[0].get("breed_count", -1) == 0,
    933411792925913129: lambda x: x[1] is None and x[0].get("class_name", "") is not None and x[0].get("class_name", "").lower() == "craboid",
    944009796917530674: lambda x: x[1] is None and x[0].get("class_name", "") is not None and x[0].get("class_name", "").lower() == "organic",
    951797051329507328: lambda x: x[1] is None and subclass_type_map.get(x[0].get("crabada_subclass", -1), "unknown").lower() == "tank",
    951797103863136306: lambda x: x[1] is None and subclass_type_map.get(x[0].get("crabada_subclass", -1), "unknown").lower() == "damage",
    951797172419047424: lambda x: x[1] is None and subclass_type_map.get(x[0].get("crabada_subclass", -1), "unknown").lower() == "buff",
    933506031261188116: lambda x: (
        x[1] is None and
        x[0].get("breed_count", -1) is not None and
        x[0].get("pure_number", -1) is not None and
        x[0].get("breed_count", -1) == 0 and
        x[0].get("pure_number", -1) == 6
    ),
    933860819920355359: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Avalanche",
    933860865831223318: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Ethereum",
    933860950942044180: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Near",
    933861077312217129: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Bitcoin",
    938919076447813672: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Fantom",
    933862594278740048: lambda x: x[1] is None and x[0].get("is_origin", -1) == 1,
    #Eggs
    938865346125889646: lambda x: x[1] is not None,
    933861312809799691: lambda x: x[1] is not None and get_probability_pure(x[1]) >= THRESOLD_PURE_PROBA,
    933861463414669422: lambda x: x[1] is not None and get_parent_class(x[1], 0) == "PRIME" and get_parent_class(x[1], 1) == "PRIME",
    934101749013291068: lambda x: x[1] is not None and (get_parent_class(x[1], 0) == "PRIME" or get_parent_class(x[1], 1) == "PRIME") and (get_parent_class(x[1], 0) != get_parent_class(x[1], 1)),
    933861589738737664: lambda x: x[1] is not None and get_parent_class(x[1], 0) == "CRABOID" and get_parent_class(x[1], 1) == "CRABOID",
    934101847420055552: lambda x: x[1] is not None and (get_parent_class(x[1], 0) == "CRABOID" or get_parent_class(x[1], 1) == "CRABOID") and (get_parent_class(x[1], 0) != get_parent_class(x[1], 1)),
    944009583305834496: lambda x: x[1] is not None and get_parent_class(x[1], 0) == "ORGANIC" and get_parent_class(x[1], 1) == "ORGANIC",
    944009617803980820: lambda x: x[1] is not None and (get_parent_class(x[1], 0) == "ORGANIC" or get_parent_class(x[1], 1) == "ORGANIC") and (get_parent_class(x[1], 0) != get_parent_class(x[1], 1)),
    938864199394820177: lambda x: x[1] is not None and get_probability_pure(x[1]) >= 0.98
}

listing_channels_to_display_shortdescrs = {
    
}

channel_to_post_sellings_with_filters = {
    #special
    932591668597776414: lambda x: True,
    935237809697095723: lambda x: True,
    #Crabs and all
    943230174466547712: lambda x: True,
    943964760876138548: lambda x: x[1] is None and x[0].get("class_name", None) is not None,
    943966530889199616: lambda x: (
        x[1] is None and
        x[0].get("breed_count", -1) is not None and
        x[0].get("pure_number", -1) is not None and
        x[0].get("breed_count", -1) == 0 and
        x[0].get("pure_number", -1) == 6
    ),
    943966566071009290: lambda x: (
        x[1] is None and
        x[0].get("breed_count", -1) is not None and
        x[0].get("breed_count", -1) == 0
    ),
    #Eggs
    943964843063521310: lambda x: x[1] is not None,
    943966387498532905: lambda x: (x[1] is None and x[0].get("pure_number", -1) is not None and x[0].get("pure_number", -1) == 6) or (x[1] is not None and get_probability_pure(x[1]) >= 0.98)


}

selling_channels_to_display_shortdescrs = set()


cool_subclasses = {"near", "avalanche", "ethereum", "fantom", "bitcoin"}

cool_classes = {"prime", "craboid", "organic"}
