from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 8

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                o_year,
                round(
                    sum(case
                        when nation = 'BRAZIL' then volume
                        else 0
                    end) / sum(volume)
                , 2) as mkt_share
            from
                (
                    select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                    from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                    where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'AMERICA'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
                ) as all_nations
            group by
                o_year
            order by
                o_year
        """
        utils.get_region_table()
        utils.get_nation_table()
        utils.get_supplier_table()
        utils.get_part_table()
        #utils.get_part_supp_table()
        utils.get_customer_table()
        utils.get_orders_table()
        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()