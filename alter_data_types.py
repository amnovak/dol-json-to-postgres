" Sandbox to update data types in the JSON data tables "

import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text
import os
from test_helpers import get_database_engine
from dotenv import load_dotenv


load_dotenv()
engine = get_database_engine(local_dev=False)


def update_data_types():

    query = text(
        """ALTER TABLE h2a
           ALTER COLUMN "clearance_order_job_minedu" TYPE character varying(255);"""
            )

    engine.execute(query)



def add_new_column():

    # query = text(
    #     """UPDATE jo SET "eta_case_number" = REPLACE("case_number", 'JO-A', 'H')""")

    # query = text(
    # """UPDATE jo SET "date_acceptance_ltr_issued_ymd" = DATE(to_timestamp("date_acceptance_ltr_issued", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'))""")


    # query = text("""ALTER TABLE jo ADD date_submitted_ymd DATE;
    # UPDATE jo SET "date_submitted_ymd" = DATE(to_timestamp("date_submitted", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'))
    # """)

    # query = text("""ALTER TABLE jo ADD form790_as_of_date_ymd DATE;
    # UPDATE jo SET "form790_as_of_date_ymd" = DATE(to_timestamp("form790_as_of_date", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'));
    # """)

    # query = text("""ALTER TABLE h2b ADD date_acceptance_ltr_issued_ymd DATE;
    # UPDATE h2b SET "date_acceptance_ltr_issued_ymd" = DATE(to_timestamp("date_acceptance_ltr_issued", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'));
    # ALTER TABLE h2b ADD date_application_submitted_ymd DATE;
    # UPDATE h2b SET "date_application_submitted_ymd" = DATE(to_timestamp("date_application_submitted", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'))
    # """)


    # query = text("""ALTER TABLE h2a ADD date_acceptance_ltr_issued_ymd DATE;
    # UPDATE h2a SET "date_acceptance_ltr_issued_ymd" = DATE(to_timestamp("date_acceptance_ltr_issued", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'));
    # ALTER TABLE h2a ADD date_submitted_ymd DATE;
    # UPDATE h2a SET "date_submitted_ymd" = DATE(to_timestamp("date_submitted", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'));

    # ALTER TABLE h2a ADD clearance_order_date_submitted_ymd DATE;
    # UPDATE h2a SET clearance_order_date_submitted_ymd = DATE(to_timestamp("clearance_order_date_submitted", 'YYYY-MM-DD"T"HH24:MI:SS.MSZ'));
    # """)

    # effectively a changelog until I get a better system here - 9/6/23 added these columns: 
    # query = text("""ALTER TABLE h2a ADD atty_fein VARCHAR(25)""")
    # query = text("""ALTER TABLE h2a ADD emp_fein VARCHAR(25)""")
    # query = text("""ALTER TABLE h2a ADD prep_fein VARCHAR(25)""")

    # query = text("""ALTER TABLE h2b ADD emp_fein VARCHAR(25);
    #              ALTER TABLE h2b ADD atty_fein VARCHAR(25);
    #              ALTER TABLE h2b ADD prep_fein VARCHAR(25);""")

    # query = text("""ALTER TABLE jo ADD emp_fein VARCHAR(25)""")

    # query = text("""ALTER TABLE h2b ADD cap_exempt VARCHAR(25);
    #              ALTER TABLE h2b ADD cap_subject VARCHAR(25);""")
    
    # query = text("""ALTER TABLE h2b ADD registration_number VARCHAR(25);""")


    # 1/3/24 added these columns: 

    # query = text(
    # """
    # ALTER TABLE h2a ADD clearance_order_job_end_date_ymd DATE;
    # UPDATE h2a SET "clearance_order_job_end_date_ymd" = TO_DATE("clearance_order_job_end_date", 'DD Mon YYYY');
    
    # """)


    # query = text(
    # """
    # ALTER TABLE h2a ADD clearance_order_job_begin_date_ymd DATE;
    # UPDATE h2a SET "clearance_order_job_begin_date_ymd" = TO_DATE("clearance_order_job_begin_date", 'DD Mon YYYY');
    
    # """)

    
    query = text(
    """
    ALTER TABLE h2b ADD tempneed_start_ymd DATE;
    UPDATE h2b SET "tempneed_start_ymd" = TO_DATE("tempneed_start", 'DD Mon YYYY');
    
    """)


    engine.execute(query)


if __name__ == '__main__':

    add_new_column()
    # update_data_types()