from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 22

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                cntrycode,
                count(*) as numcust,
                sum(c_acctbal) as totacctbal
            from (
                select
                    substring(c_phone from 1 for 2) as cntrycode,
                    c_acctbal
                from
                    customer
                where
                    substring(c_phone from 1 for 2) in
                        ("13","31","23", "29", "30", "18", "17")
                    and c_acctbal > (
                        select
                            avg(c_acctbal)
                        from
                            customer
                        where
                            c_acctbal > 0.00
                            and substring (c_phone from 1 for 2) in
                                ("13","31","23", "29", "30", "18", "17")
                    )
                    and not exists (
                        select
                            *
                        from
                            orders
                        where
                            o_custkey = c_custkey
                    )
                ) as custsale
            group by
                cntrycode
            order by
                cntrycode
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