import re
import psycopg2
from psycopg2 import extras
import json
from exceptions import Exception, AssertionError


TABLE_NAME = "beneficiary_sample_2010"

def cursor_connect(db_dsn, cursor_factory=None):
    """
    Connects to the DB and returns the connection and cursor, ready to use.

    Parameters
    ----------
    db_dsn : str, unicode
        DSN of the database to connect to.
    cursor_factory : psycopg2.extras
        An optional psycopg2 cursor type, e.g. DictCursor.

    Returns
    -------
    (psycopg2.extensions.connection, psycopg2.extensions.cursor)
        A tuple of (psycopg2 connection, psycopg2 cursor).
    """
    con = psycopg2.connect(dsn=db_dsn)
    con = psycopg2.connect(dsn=db_dsn)
    if not cursor_factory:
        cur = con.cursor()
    else:
        cur = con.cursor(cursor_factory=cursor_factory)
    return con, cur


def disease_frequency(col):
    """
    Get the states in descending order of the percentage of disease claims,
    where disease corresponds to the column name.

    Parameters
    ----------
    col : str, unicode
        A column name.

    Returns
    -------
    json
        A labeled JSON object with the state and percent disease claims out
        of all of that state's claims.

    Examples
    --------
    /api/v1/freq/depression
    /api/v1/freq/diabetes
    """
    disease = []
    accepted_cols = (
        "end_stage_renal_disease",
        "alzheimers_related_senile",
        "heart_failure",
        "chronic_kidney",
        "cancer",
        "chronic_obstructive_pulmonary",
        "depression",
        "diabetes",
        "ischemic_heart",
        "osteoporosis",
        "rheumatoid_osteo_arthritis",
        "stroke_ischemic_attack",
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            raise AssertionError("Column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(db_dsn, psycopg2.extras.DictCursor)
        query = """
        SELECT state, {1}/claims::float AS frequency FROM (SELECT
        LHS.state AS state, {1}, claims FROM (SELECT state, count(*) AS
        claims FROM {0} GROUP BY state order by claims desc)
        AS LHS LEFT JOIN (SELECT state, count(*) AS {1} FROM
        {0} WHERE {1}='true' GROUP BY state) AS RHS
        ON LHS.state=RHS.state) AS outer_q
        ORDER by frequency DESC;""".format(TABLE_NAME, cleaned_col)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            freq = {row['state']: row['frequency']}
            disease.append(freq)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return disease


if __name__ == "__main__":
    host = "cmsdata.chtdutbma0ig.us-west-2.rds.amazonaws.com"
    dbname = "BENEFICIARYDATA"
    user = "cpnguyen5"
    password = "hs611"
    db_dsn = "host={0} dbname={1} user={2} password={3}".format(
        host, dbname, user, password)
    cursor_connect(db_dsn)
    print disease_frequency('diabetes')
