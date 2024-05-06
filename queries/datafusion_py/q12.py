from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 12

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                l_shipmode,
                sum(case
                    when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                    else 0
                end) as high_line_count,
                sum(case
                    when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                    else 0
                end) as low_line_count
            from
                orders,
                lineitem
            where
                o_orderkey = l_orderkey
                and l_shipmode in ('MAIL', 'SHIP')
                and l_commitdate < l_receiptdate
                and l_shipdate < l_commitdate
                and l_receiptdate >= date '1994-01-01'
                and l_receiptdate < date '1994-01-01' + interval '1' year
            group by
                l_shipmode
            order by
                l_shipmode
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