from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 7

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                supp_nation,
                cust_nation,
                l_year,
                sum(volume) as revenue
            from
                (
                    select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        date_part('year', l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                    from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                    where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l_shipdate between make_date(1995, 1, 1) and make_date(1996, 12, 31)
                ) as shipping
            group by
                supp_nation,
                cust_nation,
                l_year
            order by
                supp_nation,
                cust_nation,
                l_year
        """
        #utils.get_region_table()
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