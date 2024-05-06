from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 9

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                nation,
                o_year,
                round(sum(amount), 2) as sum_profit
            from
                (
                    select
                        n_name as nation,
                        date_part('year', o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                    from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                    where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%green%'
                ) as profit
            group by
                nation,
                o_year
            order by
                nation,
                o_year desc
        """
        #utils.get_region_table()
        utils.get_nation_table()
        utils.get_supplier_table()
        utils.get_part_table()
        utils.get_part_supp_table()
        #utils.get_customer_table()
        utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()