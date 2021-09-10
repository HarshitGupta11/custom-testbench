from gen_fact_table import gen_fact_table
from gen_dim_table import gen
from gen_sql import gen_sql_dim_table
from gen_sql import gen_sql_fact_table
from gen_sql import gen_sql_dim_table_orc
from gen_sql import gen_sql_fact_table_orc
from gen_sql import gen_sql_dim_table_overwrite
from gen_sql import gen_sql_fact_table_partitioned_load

part = 10
part_depth = 1
prim_width = 2
rand_width = 8
num_rows_part = 10
num_rows_dim = 25
dir = "/tmp/"

dim_table_name = "dim_table_txt_{}"
dim_table_orc_name = "dim_table_{}"
dim_table_part_name = "dim_table_part_{}"
fact_table_name = "fact_table_txt_{}"
fact_table_orc_name = "fact_table_{}"

sql_file = "/tmp/sql_file"
file = open(sql_file, "w")

prim_keys = []
for i in range(prim_width):
    prim_keys.append(gen(num_rows_dim, dir + dim_table_name.format(i + 1)))
    gen_sql_dim_table(dim_table_name.format(i + 1), file)

for i in range(2):
    gen_fact_table(part, part_depth, num_rows_part, prim_keys, prim_width, rand_width,
                   dir + fact_table_name.format(i + 1))
    gen_sql_fact_table(fact_table_name.format(i + 1), part_depth, prim_width, rand_width, file)

for i in range(prim_width):
    gen_sql_dim_table_orc(dim_table_orc_name.format(i + 1), file)
    gen_sql_dim_table_overwrite(dim_table_name.format(i + 1), dim_table_orc_name.format(i + 1), file)

for i in range(2):
    gen_sql_fact_table_orc(fact_table_orc_name.format(i + 1), part_depth, prim_width, rand_width, file)
    gen_sql_fact_table_partitioned_load(fact_table_name.format(i + 1), fact_table_orc_name.format(i + 1), part_depth,
                                        file)

file.close()
print("Done")
