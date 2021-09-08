import pandas as pd  # to work with data
from openpyxl import load_workbook  # to work with excel xml
from datetime import datetime  # to timestamp stuff


# After saving in xlsx format, we can now work on the xml files with openpyxl
def read_worksheet(workbook, worksheet):
    '''
    Generic function that returns a worksheet object

    :param workbook: str containing the workbook name and path
    :param worksheet: str, worksheet name
    :return: worksheet object from openpyxl
    '''
    wb = load_workbook(workbook)
    return wb[worksheet]


# let's iterate all pivot tables and find out which ones we'll be working with!
def pivot_names(worksheet):
    '''
    Function that queries pivot tables' names in case there's specific pivot tables the user want to use

    :param worksheet: worksheet object from openpyxl
    :return: list of pivot names
    '''
    names = []
    for pivot in worksheet._pivots:
        names.append(pivot.name)
    return names


# so now we have table definitions, let's get cacheDefinitions (instead of reading each pivotCacheDefinition.xml)
def df_pivot(pivot_cache):
    '''
    Generic Function that returns both a DataFrame with the pivot table records and its dimensions in a dictionary

    :param pivot_cache: cacheDefinitions object from openpyxl
    :returns    DataFrame object: a pandas dataframe containing record's values and cacheFields names in columns
                dict_dim: dictionary containing all dimensions names and items from sharedItems fields
    '''
    record = []
    dims = {}
    dict_dim = {}
    for cf in pivot_cache.cacheFields:
        dims[cf.name] = cf.name
        dim_items = {}
        i = 0
        # let's get other items under this dimension
        for si in cf.sharedItems._fields:
            try:  # sometimes the list is empty
                dim_items[i] = si.v
                i += 1
            except:
                dim_items[i] = None
        dict_dim[cf.name] = dim_items

    # so now working with actual records
    for rows in pivot_cache.records.r:
        row = []
        for cols in rows._fields:
            try:  # we have to handle missing objects
                row.append(cols.v)
            except :
                row.append(None)
        record.append(row)
    # Let's turn it into a dataframe to work properly
    return pd.DataFrame(columns=dims, data=record), dict_dim


# Let's remap the fact table with dimensions from cacheDefinitions
def remap(df, dict_dim):
    '''
    Generic function to remap fact table with dimensions from cacheDefinitions

    :param df: DataFrame object containing the fact table from record's values
    :param dict_dim: dictionary containing all dimensions names and items from sharedItems fields
    :return: DataFrame object containing actual (human readable) data
    '''
    remapped_columns = {}
    for c in df:
        def change_value(x):
            # Function that returns a given dictionary changing only values
            # that are not nan (avoids use of lambda)
            return dict_dim[c].get(x, x)
        c2 = df[c].apply(change_value)
        remapped_columns[c] = c2
    return df.assign(**remapped_columns)


# Let's build the correct schema
def build_schema(nice_table):
    '''
    Specific Function that builds a given schema

    :param nice_table: a DataFrame object containing specific pivot table schema
    :return: DataFrame object with following schema:
    Column	    Type
    year_month	date
    uf	        string
    product	    string
    unit	    string
    volume	    double
    created_at	timestamp
    '''
    nice_table_schema = pd.melt(nice_table,
                                id_vars=['COMBUSTÍVEL', 'REGIÃO', 'ANO', 'ESTADO', 'UNIDADE', 'TOTAL'],
                                var_name='month',
                                value_name='volume')
    nice_table_schema.drop(['REGIÃO', 'TOTAL'], axis=1, inplace=True)
    nice_table_schema = nice_table_schema.rename(columns={"COMBUSTÍVEL": "product",
                                                          "ANO": "year_month",
                                                          "ESTADO": "uf",
                                                          "UNIDADE": "unit"})

    # Changing month literals for numbers in MM format
    months = {
        'Jan': '01',
        'Fev': '02',
        'Mar': '03',
        'Abr': '04',
        'Mai': '05',
        'Jun': '06',
        'Jul': '07',
        'Ago': '08',
        'Set': '09',
        'Out': '10',
        'Nov': '11',
        'Dez': '12',
    }
    nice_table_schema['month'].replace(months, inplace=True)
    data_types_dict = {'year_month': int}  # take this floating point off of my sight!
    nice_table_schema = nice_table_schema.astype(data_types_dict)
    data_types_dict = {'year_month': str}  # now we can concatenate in a easier way
    nice_table_schema = nice_table_schema.astype(data_types_dict)
    nice_table_schema['year_month'] = nice_table_schema['year_month'] + nice_table_schema['month']
    nice_table_schema['year_month'] = pd.to_datetime(nice_table_schema['year_month'], format="%Y%m")  # date format!
    nice_table_schema['created_at'] = pd.Timestamp(datetime.now())
    nice_table_schema.drop('month', axis=1, inplace=True)  # drop mic... thug life!
    return nice_table_schema


if __name__ == '__main__':

    ###
    # Let's start with the generic stuff
    ###
    ws1 = read_worksheet(workbook="/opt/bitnami/airflow/dags/vendas-combustiveis-m3.xlsx", worksheet="Plan1")
    # nms = pivot_names(ws)  # only used to check which ones we're working with
    # print(nms)

    ###
    # Sales of oil derivative fuels by UF and product - Tabela dinâmica1 -> index 3
    ###
    pvt_cache1 = ws1._pivots[3].cache
    df1_aux = df_pivot(pvt_cache1)
    df1 = df1_aux[0]
    dict_d1 = df1_aux[1]
    Table1 = remap(df1, dict_d1)
    ex1 = build_schema(Table1)

    ###
    # Sales of diesel by UF and type - Tabela dinâmica3 -> index 1
    ###
    pvt_cache2 = ws1._pivots[1].cache
    df2_aux = df_pivot(pvt_cache2)
    df2 = df2_aux[0]
    dict_d2 = df2_aux[1]
    Table2 = remap(df2, dict_d2)
    ex2 = build_schema(Table2)
