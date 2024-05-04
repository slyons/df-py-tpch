from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 5

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                n_name,
                sum(l_extendedprice * (1 - l_discount)) as revenue
            from
                customer,
                orders,
                lineitem,
                supplier,
                nation,
                region
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and l_suppkey = s_suppkey
                and c_nationkey = s_nationkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'ASIA'
                and o_orderdate >= make_date(1994, 1, 1)
                and o_orderdate < make_date(1994, 1, 1) + interval '1' year
            group by
                n_name
            order by
                revenue desc
        """
        utils.get_region_table()
        utils.get_nation_table()
        utils.get_supplier_table()
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