"""
Module to upsert JSON files downloaded from DOL data feeds to PostgreSQL database tables

"""

# from imghdr import tests
# from msilib.schema import File, tables
import os
import logging
import traceback
import json
import pandas as pd
import helpers
import inflection
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()
logger = logging.getLogger('')
engine = helpers.get_database_engine(local_dev = False)



def load_json(item, filename):
    """Loads a json file with unicode_escape or, if that fails, utf-8 encoding"""

    try:
        infile = open(item + "/" + filename, encoding = "unicode_escape")
        data = json.load(infile, strict = False)

    except Exception as e:

        try:
            infile = open(item + "/" + filename, encoding = "utf-8")
            data = json.load(infile, strict = False)
        
        except Exception as e:
            # print("Exception opening with utf-8 encoding: ", e)
            print(f"Exception opening {filename} with utf-8 encoding: {e}")

    return data




def upsert_df(df: pd.DataFrame, table):
    """ Adapting eviction scraper geocode_and_merge.py + https://stackoverflow.com/questions/54831495/pandas-leaving-idle-postgres-connections-open-after-to-sql """

    conn = engine.connect()

    try: 
        df.to_sql('temp_table', conn, if_exists='replace', index=False)

    except Exception as ee: 
        logger.error('temp_table df.to_sql failed', ee)
        traceback.print_exc()

    finally:
        conn.close()


    columns = list(df)

    # # On conflict with case number, set all other column values to those from the temp table
    conflict_clause = ', '.join([f"{x} = EXCLUDED.{x}" for x in columns])

    # For the ON CONFLICT clause, postgres requires column to have unique constraint
    query_unique_constraint = f"""ALTER TABLE "{table}" ADD CONSTRAINT unique_constraint_for_upsert_{table} UNIQUE (case_number);"""

    with engine.begin() as connection: 
        try:
            connection.execute(text(query_unique_constraint))
        except Exception as e:
            # relation "unique_constraint_for_upsert" already exists
            if not 'already exists' in e.args[0]:
                raise e


    upsert_query = f"""
        INSERT INTO {table} ({', '.join(columns)})
        SELECT {', '.join(columns)} FROM temp_table
        ON CONFLICT (case_number) DO UPDATE
        SET {conflict_clause}; 
        """



    with engine.begin() as connection:
        connection.execute(text(upsert_query))





def clean_df(df, table):

    df.columns = map(inflection.underscore, df.columns)

    df = df.rename(columns=lambda s: s.replace(".", "_"))

    df.to_csv(f'{table}.csv')

    upsert_df(df, table)


def load_and_clean_data(directory, cols_to_drop, date_cols, additional_transforms=None):
    """ Load json files to a dataframe. """

    df = pd.DataFrame()

    for filename in sorted(os.listdir(directory)):
        data = load_json(directory, filename)
        normalized_data = pd.json_normalize(data)
        df = pd.concat([df, normalized_data])

    df = df.drop(cols_to_drop, axis = 1, errors='ignore')

    for col, new_col in date_cols.items():
        df[new_col] = pd.to_datetime(df[col], errors='coerce').dt.date

    if additional_transforms:
        df = additional_transforms(df)

    clean_df(df, directory)



def additional_numeric_transform(df): 
    """ Convert 'app_is_cap_exempt' column to numeric if present"""
    df['appIsCapExempt'] = pd.to_numeric(df['appIsCapExempt'])
    return df


def h2b_to_postgres():
    """ Load json files and add each as row to the appropriate dataframe. Clean df, then add or upsert to Postgres table"""

    cols_to_drop = ['employmentLocations', 'recruiters', 'employerClient']
    date_cols = {
        'dateAcceptanceLtrIssued': 'date_acceptance_ltr_issued_ymd',
        'dateApplicationSubmitted': 'date_application_submitted_ymd',
        'tempneedStart': 'tempneed_start_ymd'
    }

    additional_transforms = additional_numeric_transform

    load_and_clean_data("h2b", cols_to_drop, date_cols, additional_transforms)



def h2a_to_postgres():
    cols_to_drop = ['clearanceOrder.cropsAndActivities', 'clearanceOrder.employmentLocations',
                       'clearanceOrder.housingLocations', 'clearanceOrder.termsAndConditions',
                       'foreignLaborRecInfo', 'clearanceOrder.agBusinesses']
    

    date_cols = {'dateAcceptanceLtrIssued': 'date_acceptance_ltr_issued_ymd',
        'dateSubmitted': 'date_submitted_ymd',
        'clearanceOrder.dateSubmitted': 'clearance_order_date_submitted_ymd',
        'clearanceOrder.jobEndDate': 'clearance_order_job_end_date_ymd',
        'clearanceOrder.jobBeginDate': 'clearance_order_job_begin_date_ymd'
    }

    load_and_clean_data("h2a", cols_to_drop, date_cols)




def jo_transforms(df): 
    df = df.drop_duplicates()
    df['eta_case_number'] = df['caseNumber'].str.replace("JO-A", "H")
    return df



def jo_to_postgres():
    # 1/31/25  - Removing 'agbBusinesses' because of ; removing 'addSpecialPayInfo', 'appIsItinerant', 'isDailyTransport', 'isEmploymentTransport' (new columns)
    cols_to_drop = ['cropsAndActivities', 'employmentLocations', 'housingLocations', 'termsAndConditions', 'agBusinesses', 
                    'addSpecialPayInfo', 'appIsItinerant', 'isDailyTransport', 'isEmploymentTransport', 'isMealProvision', 
                    'isOvertimeAvailable', 'isPayDeductions', 'minProductivity']
    date_cols = {
        'dateAcceptanceLtrIssued': 'date_acceptance_ltr_issued_ymd',
        'dateSubmitted': 'date_submitted_ymd',
        'form790AsOfDate': 'form790_as_of_date_ymd'
    }

    load_and_clean_data("jo", cols_to_drop, date_cols, jo_transforms)





def count_records():
    counts = []

    for item in ["h2a", "h2b", "jo"]: 

        record_count = pd.read_sql_query(f"SELECT COUNT(*) FROM {item};", engine).iloc[0, 0]
        engine.dispose()
        counts.append(record_count)
        print(f"item: {item}, count: {record_count}")

    return counts



def upsert_json():

    start_counts = count_records()

    h2a_to_postgres()
    h2b_to_postgres()
    jo_to_postgres()

    end_counts = count_records()

    result = [a - b for a, b in zip(end_counts, start_counts)]

    # print(f"Added {result[0]} to h2a, {result[1]} to h2b, and {result[2]} to jo. There are now {end_counts[0]} total h2a records, {end_counts[1]} total h2b records, and {end_counts[2]} total jo records.")
    try: 
        print(f"Added {result[0]} to h2a, {result[1]} to h2b. There are now {end_counts[0]} total h2a records, {end_counts[1]} total h2b records.")
    except IndexError:
        print("Error: result or end_counts has fewer than 2 elements:", result, end_counts)


if __name__ == "__main__":
    upsert_json()
