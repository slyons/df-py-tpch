from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 11

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                ps_partkey,
                round(sum(ps_supplycost * ps_availqty), 2) as value
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
            group by
                ps_partkey having
                        sum(ps_supplycost * ps_availqty) > (
                    select
                        sum(ps_supplycost * ps_availqty) * 0.0001
                    from
                        partsupp,
                        supplier,
                        nation
                    where
                        ps_suppkey = s_suppkey
                        and s_nationkey = n_nationkey
                        and n_name = 'GERMANY'
                    )
                order by
                    value desc
        """
        #utils.get_region_table()
        utils.get_nation_table()
        utils.get_supplier_table()
        #utils.get_part_table()
        utils.get_part_supp_table()
        #utils.get_customer_table()
        #utils.get_orders_table()
        #utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()