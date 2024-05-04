from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 2

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            from
                part,
                supplier,
                partsupp,
                nation,
                region
            where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and p_size = 15
                and p_type like '%BRASS'
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'EUROPE'
                and ps_supplycost = (
                    select
                        min(ps_supplycost)
                    from
                        partsupp,
                        supplier,
                        nation,
                        region
                    where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
                )
            order by
                s_acctbal desc,
                n_name,
                s_name,
                p_partkey
            limit 100
        """

        utils.get_region_table()
        utils.get_nation_table()
        utils.get_supplier_table()
        utils.get_part_table()
        utils.get_part_supp_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()