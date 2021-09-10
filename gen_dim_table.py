# this will genersate the dimension table
# the tables generated here will have names dim_table_{}
# the table will have the first field as primary key
import numpy as np

from tools import get_rand_num, get_rand_string
from tools import get_rand_decimal

width = 7


def gen(rows, fname):
    prim = get_rand_num(rows)
    temp = [prim]
    for i in range(3):
        temp.append(get_rand_num(rows)[np.random.permutation(rows)])
    for i in range(2):
        temp.append(get_rand_decimal(rows)[np.random.permutation(rows)])
    temp.append(get_rand_string(rows))
    with open(fname, "w") as file:
        for val in list(zip(*temp)):
            file.write(",".join(map(str, val)))
            file.write("\n")
    return prim
