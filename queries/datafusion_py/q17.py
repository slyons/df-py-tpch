from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 17

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                round(sum(l_extendedprice) / 7.0, 2) as avg_yearly
            from
                lineitem,
                part
            where
                p_partkey = l_partkey
                and p_brand = 'Brand#23'
                and p_container = 'MED BOX'
                and l_quantity < (
                    select
                        0.2 * avg(l_quantity)
                    from
                        lineitem
                    where
                        l_partkey = p_partkey
                )
        """
        #utils.get_region_table()
        #utils.get_nation_table()
        #utils.get_supplier_table()
        utils.get_part_table()
        #utils.get_part_supp_table()
        #utils.get_customer_table()
        #utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()