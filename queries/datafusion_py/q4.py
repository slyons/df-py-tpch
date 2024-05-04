from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 4

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                o_orderpriority,
                count(*) as order_count
            from
                orders
            where
                o_orderdate >= date '1993-07-01'
                and o_orderdate < date '1993-07-01' + interval '3' month
                and exists (
                    select
                        *
                    from
                        lineitem
                    where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
                )
            group by
                o_orderpriority
            order by
                o_orderpriority
        """

        #utils.get_region_table()
        #utils.get_nation_table()
        #utils.get_supplier_table()
        #utils.get_part_table()
        #utils.get_part_supp_table()
        #utils.get_customer_table()
        utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()