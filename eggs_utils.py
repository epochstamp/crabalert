def dec2hex(dna):
    dec = int(dna)
    return hex(dec).split('x')[-1]


def lookup(value: int) -> str:
        if (value <= 8):
            return 'SURGE'
        elif (value < 24):
            return 'SUNKEN'
        elif (value < 39):
            return 'PRIME'
        elif (value < 54):
            return 'BULK'
        elif (value < 69):
            return 'CRABOID'
        elif (value < 84):
            return 'RUINED'
        elif (value < 99):
            return 'GEM'
        elif (value < 114):
            return 'ORGANIC'

class Aliasstr:

    def __init__(self, s):
        self._s = s
    
    def substring(self, a, b):
        return self._s[a: b]

def calc_pure_probability(dna1, dna2, class_name):
    hex_string1 = dec2hex(dna1)
    dnafixed1 = Aliasstr(f"0{hex_string1}")
    hex_string2 = dec2hex(dna2)
    dnafixed2 = Aliasstr(f"0{hex_string2}")

    shellr0 = 0.3333 * (lookup(int(dnafixed1.substring(28, 30), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed1.substring(30, 32), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed1.substring(32, 34), 16)) == class_name) + 0.3333 * (lookup(int(dnafixed2.substring(28, 30), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed2.substring(30, 32), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed2.substring(32, 34), 16)) == class_name)
    hornr0 = 0.3333 * (lookup(int(dnafixed1.substring(34, 36), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed1.substring(36, 38), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed1.substring(38, 40), 16)) == class_name) + 0.3333 * (lookup(int(dnafixed2.substring(34, 36), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed2.substring(36, 38), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed2.substring(38, 40), 16)) == class_name)
    bodyr0 = 0.3333 * (lookup(int(dnafixed1.substring(40, 42), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed1.substring(42, 44), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed1.substring(44, 46), 16)) == class_name) + 0.3333 * (lookup(int(dnafixed2.substring(40, 42), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed2.substring(42, 44), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed2.substring(44, 46), 16)) == class_name)
    mouthr0 = 0.3333 * (lookup(int(dnafixed1.substring(46, 48), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed1.substring(48, 50), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed1.substring(50, 52), 16)) == class_name) + 0.3333 * (lookup(int(dnafixed2.substring(46, 48), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed2.substring(48, 50), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed2.substring(50, 52), 16)) == class_name)
    eyer0 = 0.3333 * (lookup(int(dnafixed1.substring(52, 54), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed1.substring(54, 56), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed1.substring(56, 58), 16)) == class_name) + 0.3333 * (lookup(int(dnafixed2.substring(52, 54), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed2.substring(54, 56), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed2.substring(56, 58), 16)) == class_name)
    pincerr0 = 0.3333 * (lookup(int(dnafixed1.substring(58, 60), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed1.substring(60, 62), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed1.substring(62, 64), 16)) == class_name) + 0.3333 * (lookup(int(dnafixed2.substring(58, 60), 16)) == class_name) + 0.1111 * (lookup(int(dnafixed2.substring(60, 62), 16)) == class_name) + 0.0556 * (lookup(int(dnafixed2.substring(62, 64), 16)) == class_name)

    pure_probability = shellr0 * hornr0 * bodyr0 * mouthr0 * eyer0 * pincerr0


    return pure_probability
