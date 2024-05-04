from datafusion import SessionContext, DataFrame, __version__

from queries.common_utils import (
    check_query_result_pd,
    check_query_result_pl,
    get_table_path,
    run_query_generic,
)

from settings import Settings
settings = Settings()

session_ctx = None
def get_or_create_session() -> SessionContext:
    global session_ctx
    if session_ctx is None:
        session_ctx = SessionContext()
    return session_ctx

def _scan_table(table_name: str) -> str:
    path = get_table_path(table_name)

    #DOCFIX: read_ functions require strings, not PosixPath
    path = str(path)
    if settings.run.io_type == "skip":
        session = get_or_create_session()
        df = session.read_parquet(path)
        df = df.cache()
        assert df is not None
        session.register_dataset(table_name, df)
    elif settings.run.io_type == "parquet":
        get_or_create_session().register_parquet(table_name, path)
    elif settings.run.io_type == "csv":
        get_or_create_session().register_csv(table_name, path)
    else:
        msg = f"unsupported file type: {settings.run.io_type}"
        raise ValueError(msg)
    return table_name 

def get_line_item_table() -> str:
    return _scan_table("lineitem")

#def get_line_item_df() -> DataFrame:
#    return get_or_create_session().table(get_line_item_table)

def get_orders_table() -> str:
    return _scan_table("orders")

#def get_orders_df() -> DataFrame:
#    return get_or_create_session().table(get_orders_table())

def get_customer_table() -> str:
    return _scan_table("customer")

#def get_customer_df() -> DataFrame:
#    return get_or_create_session().table(get_customer_table())

def get_region_table() -> str:
    return _scan_table("region")

#def get_region_df() -> DataFrame:
#    return get_or_create_session().table(get_region_table())

def get_nation_table() -> str:
    return _scan_table("nation")

#def get_nation_df() -> DataFrame:
#    return get_or_create_session().table(get_nation_table())

def get_supplier_table() -> str:
    return _scan_table("supplier")

#def get_supplier_df() -> DataFrame:
#    return get_or_create_session().table(get_supplier_table())

def get_part_table() -> str:
    return _scan_table("part")

#def get_part_df() -> DataFrame:
#    return get_or_create_session().table(get_part_table())

def get_part_supp_table() -> str:
    return _scan_table("partsupp")

#def get_part_supp_df() -> DataFrame:
#    return get_or_create_session().table(get_part_supp_table())


def run_query(query_number: int, df: DataFrame) -> None:
    #query = df.to_pandas
    query = df.to_polars
    run_query_generic(
        query,
        query_number, 
        f"datafusion_py_{settings.run.datafusion_mode}",
        library_version=__version__,
        #query_checker=check_query_result_pd
        query_checker=check_query_result_pl
    )