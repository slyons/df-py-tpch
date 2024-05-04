from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 6

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                sum(l_extendedprice * l_discount) as revenue
            from
                lineitem
            where
                l_shipdate >= make_date(1994, 1, 1)
                and l_shipdate < make_date(1994, 1, 1) + interval '1' year
                and l_discount between .06 - 0.01 and .06 + 0.01
                and l_quantity < 24
        """
        #utils.get_region_table()
        #utils.get_nation_table()
        #utils.get_supplier_table()
        #utils.get_part_table()
        #utils.get_part_supp_table()
        #utils.get_customer_table()
        #utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()