drop_dim_table_template = "DROP TABLE IF EXISTS {};"

create_table_template = """CREATE TABELE {}({})
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE;"""

table_col_name = """{}_col_{} {}"""
part_table_col_name = """{}_part_col_{} {}"""

load_data_template = """LOAD DATA LOCAL INPATH "/tmp/{} INTO TABLE {};"""

create_table_template_dim_orc = """CREATE TABLE {}({})
STORED AS ORC;"""

insert_dim_stmt_template = """INSERT OVERWRITE TABLE {}
SELECT * FROM {};"""

create_table_template_fact_orc = """CREATE TABLE {}(
{})
PARTITIONED BY ({})
STORED AS ORC;"""

insert_fact_stmt_template = """INSERT OVERWRITE TABLE {} PARTITION ({})
SELECT * FROM {};"""


def gen_prep_dim(tblname):
    ls_cols = []
    for i in range(4):
        ls_cols.append(table_col_name.format(tblname, i + 1, "BIGINT"))
    for i in range(4, 7):
        ls_cols.append(table_col_name.format(tblname, i + 1, "DECIMAL"))
    return ",".join(ls_cols)


def gen_prep_fact(tblname, num_parts, num_prim_key, rand_width):
    ls_cols = []
    for i in range(num_parts):
        ls_cols.append(part_table_col_name.format(tblname, i + 1, "BIGINT"))
    for i in range(num_prim_key):
        ls_cols.append(table_col_name.format(tblname, i + 1, "BIGINT"))
    num_int = rand_width // 2
    if rand_width % 2 != 0:
        num_int += 1
    num_decimal = rand_width - num_int
    for i in range(num_int):
        ls_cols.append(table_col_name.format(tblname, i + 1 + num_prim_key, "BIGINT"))
    for i in range(num_decimal):
        ls_cols.append(table_col_name.format(tblname, i + 1 + num_prim_key + num_int, "DECIMAL"))
    return ",".join(ls_cols)


def gen_sql_dim_table(tablename, file):
    file.write(drop_dim_table_template.format(tablename))
    file.write("\n")
    cols = gen_prep_dim(tablename)
    file.write(create_table_template.format(tablename, cols))
    file.write("\n")
    file.write(load_data_template.format(tablename, tablename))
    file.write("\n")
    return


def gen_sql_dim_table_orc(tblname, file):
    col_names = gen_prep_dim(tblname)
    file.write(create_table_template_dim_orc.format(tblname, col_names))
    file.write("\n")
    return


def gen_sql_dim_table_overwrite(tblname, orctblname, file):
    file.write(insert_dim_stmt_template.format(orctblname, tblname))
    file.write("\n")
    return


def gen_sql_fact_table(tablename, num_parts, num_prim, rand_width,file):
    file.write(drop_dim_table_template.format(tablename))
    file.write("\n")
    # generate the create table stmt
    # generate the column names
    file.write(create_table_template.format(tablename, gen_prep_fact(tablename, num_parts, num_prim, rand_width)))
    file.write("\n")
    file.write(load_data_template.format(tablename, tablename))
    file.write("\n")
    return


def gen_sql_fact_table_orc(tblname, num_parts, num_prim, rand_width, file):
    ls_parts = []
    for i in range(num_parts):
        ls_parts.append(part_table_col_name.format(tblname, i + 1, "BIGINT"))
    ls_cols = []
    num_int = rand_width // 2
    if rand_width % 2 != 0:
        num_int += 1
    num_decimal = rand_width - num_int
    num_int += num_prim
    for i in range(num_int):
        ls_cols.append(table_col_name.format(tblname, i + 1, "BIGINT"))
    for i in range(num_decimal):
        ls_cols.append(table_col_name.format(tblname, i + 1 + num_int, "DECIMAL"))
    fin_col = ",".join(ls_cols)
    fin_part_col = ",".join(ls_parts)
    file.write(create_table_template_fact_orc.format(tblname, fin_col, fin_part_col))
    file.write("\n")
    return


def gen_sql_fact_table_partitioned_load(tblname, orctblname, num_parts,file):
    # generate the insert overwrite stmt
    part_temp = "{}_part_col_{}"
    ls_parts = []
    for i in range(num_parts):
        ls_parts.append(part_temp.format(tblname, i + 1))
    fin_part = ",".join(ls_parts)
    file.write(insert_fact_stmt_template.format(orctblname, fin_part, tblname))
    file.write("\n")
    return
