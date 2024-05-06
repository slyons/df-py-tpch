from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 16

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                p_brand,
                p_type,
                p_size,
                count(distinct ps_suppkey) as supplier_cnt
            from
                partsupp,
                part
            where
                p_partkey = ps_partkey
                and p_brand <> 'Brand#45'
                and p_type not like 'MEDIUM POLISHED%'
                and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
                and ps_suppkey not in (
                    select
                        s_suppkey
                    from
                        supplier
                    where
                        s_comment like '%Customer%Complaints%'
                )
            group by
                p_brand,
                p_type,
                p_size
            order by
                supplier_cnt desc,
                p_brand,
                p_type,
                p_size
        """
        #utils.get_region_table()
        #utils.get_nation_table()
        utils.get_supplier_table()
        utils.get_part_table()
        utils.get_part_supp_table()
        #utils.get_customer_table()
        #utils.get_orders_table()
        #utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()