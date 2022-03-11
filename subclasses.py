from utils import Aliasstr, dec2hex

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

def lookup_subclass(n):
    return subclass_map.get(n, "unknown")

def calc_subclass_info(dna):
    hexString = dec2hex(dna)
    dnafixed = Aliasstr(f"0{hexString}")

    shellr0 = lookup_subclass(int(dnafixed.substring(28, 30), 16))
    shellr1 = lookup_subclass(int(dnafixed.substring(30, 32), 16))
    shellr2 = lookup_subclass(int(dnafixed.substring(32, 34), 16))

    hornr0 = lookup_subclass(int(dnafixed.substring(34, 36), 16))
    hornr1 = lookup_subclass(int(dnafixed.substring(36, 38), 16))
    hornr2 = lookup_subclass(int(dnafixed.substring(38, 40), 16))

    bodyr0 = lookup_subclass(int(dnafixed.substring(40, 42), 16))
    bodyr1 = lookup_subclass(int(dnafixed.substring(42, 44), 16))
    bodyr2 = lookup_subclass(int(dnafixed.substring(44, 46), 16))

    mouthr0 = lookup_subclass(int(dnafixed.substring(46, 48), 16))
    mouthr1 = lookup_subclass(int(dnafixed.substring(48, 50), 16))
    mouthr2 = lookup_subclass(int(dnafixed.substring(50, 52), 16))

    eyer0 = lookup_subclass(int(dnafixed.substring(52, 54), 16))
    eyer1 = lookup_subclass(int(dnafixed.substring(54, 56), 16))
    eyer2 = lookup_subclass(int(dnafixed.substring(56, 58), 16))

    pincerr0 = lookup_subclass(int(dnafixed.substring(58, 60), 16))
    pincerr1 = lookup_subclass(int(dnafixed.substring(60, 62), 16))
    pincerr2 = lookup_subclass(int(dnafixed.substring(62, 64), 16))

    return [shellr0, shellr1, shellr2, hornr0, hornr1, hornr2, bodyr0, bodyr1, bodyr2, mouthr0, mouthr1, mouthr2, eyer0, eyer1, eyer2, pincerr0, pincerr1, pincerr2]
