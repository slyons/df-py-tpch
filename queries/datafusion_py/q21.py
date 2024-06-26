from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 21

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                s_name,
                count(*) as numwait
            from
                supplier,
                lineitem l1,
                orders,
                nation
            where
                s_suppkey = l1.l_suppkey
                and o_orderkey = l1.l_orderkey
                and o_orderstatus = 'F'
                and l1.l_receiptdate > l1.l_commitdate
                and exists (
                    select
                        *
                    from
                        lineitem l2
                    where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
                )
                and not exists (
                    select
                        *
                    from
                        lineitem l3
                    where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
                )
                and s_nationkey = n_nationkey
                and n_name = 'SAUDI ARABIA'
            group by
                s_name
            order by
                numwait desc,
                s_name
            limit 100
        """
        #utils.get_region_table()
        utils.get_nation_table()
        utils.get_supplier_table()
        #utils.get_part_table()
        utils.get_part_supp_table()
        #utils.get_customer_table()
        utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()