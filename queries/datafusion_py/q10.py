from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 10

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                c_custkey,
                c_name,
                round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            from
                customer,
                orders,
                lineitem,
                nation
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate >= date '1993-10-01'
                and o_orderdate < date '1993-10-01' + interval '3' month
                and l_returnflag = 'R'
                and c_nationkey = n_nationkey
            group by
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment
            order by
                revenue desc
            limit 20
        """
        #utils.get_region_table()
        utils.get_nation_table()
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