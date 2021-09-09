from gen_fact_table import gen_fact_table
from gen_dim_table import gen
from gen_sql import gen_sql_dim_table
part = 5
part_depth = 2
prim_width = 5
rand_width = 8
num_rows_part = 10
num_rows_dim = 25
dir = "/tmp/"

dim_table_name = "dim_table_{}"
dim_table_part_name = "dim_table_part_{}"
fact_table_name = "fact_table_{}"

sql_file = "/tmp/sql_file"
file = open(sql_file,"w")

prim_keys =[]
for i in range(prim_width):
    prim_keys.append(gen(num_rows_dim, dir + dim_table_name.format(i+1)))
    gen_sql_dim_table(dim_table_name.format(i+1),file)

for i in range(2):
    gen_fact_table(part,part_depth,num_rows_part,prim_keys,prim_width,rand_width,dir + fact_table_name.format(i+1))

file.close()
print("Done")