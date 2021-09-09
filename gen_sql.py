
drop_dim_table_template = "DROP TABLE IF EXISTS {};"
create_table_template = """CREATE TABELE {}({})
FIELDS SEPERATED BY ",";"""
dim_table_col_name = """{}_col_{} {}"""
load_data_template = """LOAD DATA LOCAL INPATH "/tmp/{} INTO TABLE {};"""

def gen_prep_dim(tblname):
    ls_cols = []
    for i in range(4):
        ls_cols.append(dim_table_col_name.format(tblname,i+1, "bigint"))
    for i in range(4,7):
        ls_cols.append(dim_table_col_name.format(tblname, i + 1, "DECIMAL"))
    return ",".join(ls_cols)

def gen_sql_dim_table(tablename, file):
    print(drop_dim_table_template.format(tablename))
    cols = gen_prep_dim(tablename)
    print(create_table_template.format(tablename,cols))
    print(load_data_template.format(tablename,tablename))
    return