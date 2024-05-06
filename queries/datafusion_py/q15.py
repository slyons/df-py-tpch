from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 15

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        ddl_str = """
            create temp view revenue (supplier_no, total_revenue) as
                select
                    l_suppkey,
                    sum(l_extendedprice * (1 - l_discount))
                from
                    lineitem
                where
                    l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-01-01' + interval '3' month
                group by
                    l_suppkey
        """
        query_str = """
            select
                s_suppkey,
                s_name,
                s_address,
                s_phone,
                total_revenue
            from
                supplier,
                revenue
            where
                s_suppkey = supplier_no
                and total_revenue = (
                    select
                        max(total_revenue)
                    from
                        revenue
                )
            order by
                s_suppkey
        """
        #utils.get_region_table()
        #utils.get_nation_table()
        utils.get_supplier_table()
        #utils.get_part_table()
        #utils.get_part_supp_table()
        #utils.get_customer_table()
        #utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        session.sql(ddl_str)
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()