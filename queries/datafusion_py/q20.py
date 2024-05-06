from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 20

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                s_name,
                s_address
            from
                supplier,
                nation
            where
                s_suppkey in (
                    select
                        ps_suppkey
                    from
                        partsupp
                    where
                        ps_partkey in (
                            select
                                p_partkey
                            from
                                part
                            where
                                p_name like 'forest%'
                        )
                        and ps_availqty > (
                            select
                                0.5 * sum(l_quantity)
                            from
                                lineitem
                            where
                                l_partkey = ps_partkey
                                and l_suppkey = ps_suppkey
                                and l_shipdate >= date '1994-01-01'
                                and l_shipdate < date '1994-01-01' + interval '1' year
                        )
                )
                and s_nationkey = n_nationkey
                and n_name = 'CANADA'
            order by
                s_name
        """
        #utils.get_region_table()
        utils.get_nation_table()
        utils.get_supplier_table()
        utils.get_part_table()
        utils.get_part_supp_table()
        #utils.get_customer_table()
        #utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()