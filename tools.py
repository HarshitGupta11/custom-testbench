import random

import numpy as np
import string


def get_rand_num(n):
    ls = range(1, n + 1)
    base = random.randint(200000, 6767467)
    return base + np.array(ls)


def get_rand_decimal(n):
    ls = list(get_rand_num(n))
    return np.array(ls) / 100


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def get_rand_string(n):
    ls = []
    for i in range(n):
        ls.append(id_generator(10))
    return ls

def get_choice_from_list(ls, n):
    ret = []
    for i in range(n):
        ret.append(random.choice(ls))
    return ret
