from queries.datafusion_py import utils
from settings import Settings

settings = Settings()

Q_NUM = 1

def q() -> None:
    if settings.run.datafusion_mode == "sql":
        query_str = """
            select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
            from
                lineitem
            where
                l_shipdate <= make_date(1998, 9, 2)
            group by
                l_returnflag,
                l_linestatus
            order by
                l_returnflag,
                l_linestatus
        """

        utils.get_line_item_table()
        session = utils.get_or_create_session()
        df = session.sql(query_str)

        utils.run_query(Q_NUM, df)

if __name__ == "__main__":
    q()