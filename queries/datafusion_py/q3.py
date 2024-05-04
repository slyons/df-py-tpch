from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 3

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                l_orderkey,
                sum(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate,
                o_shippriority
            from
                customer,
                orders,
                lineitem
            where
                c_mktsegment = 'BUILDING'
                and c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate < make_date(1995, 3, 15)
                and l_shipdate > make_date(1995, 3, 15)
            group by
                l_orderkey,
                o_orderdate,
                o_shippriority
            order by
                revenue desc,
                o_orderdate
            limit 10
        """

        #utils.get_region_table()
        #utils.get_nation_table()
        #utils.get_supplier_table()
        #utils.get_part_table()
        #utils.get_part_supp_table()
        utils.get_customer_table()
        utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()