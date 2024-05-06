from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 19

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                round(sum(l_extendedprice* (1 - l_discount)), 2) as revenue
            from
                lineitem,
                part
            where
                (
                    p_partkey = l_partkey
                    and p_brand = 'Brand#12'
                    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    and l_quantity >= 1 and l_quantity <= 1 + 10
                    and p_size between 1 and 5
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
                or
                (
                    p_partkey = l_partkey
                    and p_brand = 'Brand#23'
                    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    and l_quantity >= 10 and l_quantity <= 20
                    and p_size between 1 and 10
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
                or
                (
                    p_partkey = l_partkey
                    and p_brand = 'Brand#34'
                    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    and l_quantity >= 20 and l_quantity <= 30
                    and p_size between 1 and 15
                    and l_shipmode in ('AIR', 'AIR REG')
                    and l_shipinstruct = 'DELIVER IN PERSON'
                )
        """
        #utils.get_region_table()
        #utils.get_nation_table()
        #utils.get_supplier_table()
        utils.get_part_table()
        #utils.get_part_supp_table()
        #utils.get_customer_table()
        #utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()