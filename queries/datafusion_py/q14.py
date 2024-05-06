from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 14

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                round(100.00 * sum(case
                    when p_type like 'PROMO%'
                        then l_extendedprice * (1 - l_discount)
                    else 0
                end) / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
            from
                lineitem,
                part
            where
                l_partkey = p_partkey
                and l_shipdate >= date '1995-09-01'
                and l_shipdate < date '1995-09-01' + interval '1' month
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