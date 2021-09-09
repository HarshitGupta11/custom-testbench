import gc
import random

import numpy as np

from tools import get_choice_from_list
from tools import get_rand_num
from tools import get_rand_decimal


def gen_data_for_part(part_id, num_rows, prim_keys, prim_key_width, random_width, file):
    # generate the partitioned columns
    lparts = [part_id for i in range(num_rows)]
    # generate the primary key values
    prim_values = []
    for i in range(prim_key_width):
        prim_values.append(get_choice_from_list(prim_keys[i],num_rows))
    prim_values = zip(*prim_values)
    prim_values = [list(x) for x in prim_values]
    # half the columns in the random space would be decimal and half would be integer
    num_int = random_width // 2
    if num_int % 2 == 0:
        num_int += 1
    num_dec = random_width - num_int
    rand_cols = []
    for i in range(num_int):
        rand_cols.append(get_rand_num(num_rows)[np.random.permutation(num_rows)])
    for i in range(num_dec):
        rand_cols.append(get_rand_decimal(num_rows)[np.random.permutation(num_rows)])
    rand_cols = zip(*rand_cols)
    rand_cols = [list(x) for x in rand_cols]
    fin_cols = [lparts[i] + prim_values[i] + rand_cols[i] for i in range(num_rows)]
    for x in fin_cols:
        file.write(",".join(map(str, x)))
        file.write("\n")
    return


def gen_parts_rec(part_id, parts, parts_depth, num_rows, prim_keys, prim_key_width, random_width, file):
    if parts_depth == 0:
        gen_data_for_part(part_id,num_rows,prim_keys,prim_key_width,random_width,file)
        gc.collect()
        return
    else:
        part_id_new = part_id.copy()
        part_base = random.randint(32532, 868363)
        for i in range(parts):
            part_id_new.append(i + 1 + part_base)
            gen_parts_rec(part_id_new, parts, parts_depth - 1, num_rows, prim_keys, prim_key_width, random_width, file)
            part_id_new = part_id_new[:-1]
    return


def gen_fact_table(parts, parts_depth, num_rows_part, prim_keys, width_prim, width,fname):
    # generate the partitioned columns first(these tables will also be dim tables)
    with open(fname, "w") as file:
        gen_parts_rec([],parts,parts_depth,num_rows_part,prim_keys,width_prim, width, file)
    return
