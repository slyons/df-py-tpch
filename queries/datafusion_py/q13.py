from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 13

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                c_count, count(*) as custdist
            from (
                select
                    c_custkey,
                    count(o_orderkey)
                from
                    customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%special%requests%'
                group by
                    c_custkey
                )as c_orders (c_custkey, c_count)
            group by
                c_count
            order by
                custdist desc,
                c_count desc
        """
        #utils.get_region_table()
        #utils.get_nation_table()
        #utils.get_supplier_table()
        #utils.get_part_table()
        #utils.get_part_supp_table()
        utils.get_customer_table()
        utils.get_orders_table()
        #utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()