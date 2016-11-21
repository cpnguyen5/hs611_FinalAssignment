import re
import psycopg2
from psycopg2 import extras
import json
from exceptions import Exception, AssertionError


TABLE_NAME = "beneficiary_sample_2010"

def cursor_connect(cursor_factory=None):
    """
    Connects to the DB and returns the connection and cursor, ready to use.

    Parameters
    ----------
    cursor_factory : psycopg2.extras
        An optional psycopg2 cursor type, e.g. DictCursor.

    Returns
    -------
    (psycopg2.extensions.connection, psycopg2.extensions.cursor)
        A tuple of (psycopg2 connection, psycopg2 cursor).
    """
    #DB DSN
    host = "cmsdata.chtdutbma0ig.us-west-2.rds.amazonaws.com"
    dbname = "BENEFICIARYDATA"
    user = "cpnguyen5"
    password = "hs611"
    db_dsn = "host={0} dbname={1} user={2} password={3}".format(
        host, dbname, user, password)
    #DB connection
    con = psycopg2.connect(dsn=db_dsn)
    con = psycopg2.connect(dsn=db_dsn)
    if not cursor_factory:
        cur = con.cursor()
    else:
        cur = con.cursor(cursor_factory=cursor_factory)
    return con, cur


def disease_bene_resp(disease_col):
    """
    Get the average outpatient beneficiary responsibility and deviation from the overall average of patient beneficiary
    responsibility, grouped by race, in descending order of the average outpatient beneficiary responsibility by race.
    The disease corresponds to the column name.

    Parameters
    ----------
    disease_col : str, unicode
        A column name specifying a disease/condition.

    Returns
    -------
    json
        A labeled JSON object with the race and their respective average outpatient beneficiary responsibility and
        deviation from the overall average of outpatient beneficiary responsibility.
    """
    outpt_bene_resp = []
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
    cleaned_col = re.sub('\W+', '', disease_col)
    try:
        if cleaned_col not in accepted_cols:
            raise AssertionError("Column '{0}' is not allowed".format(cleaned_col)) # error inapplicable disease
        con, cur = cursor_connect(psycopg2.extras.DictCursor) #access retrieved records as Python dict
        query = """
        SELECT race, avg_outpt::float,
        ROUND((avg_outpt-overall_avg)::numeric,2)::float AS deviation
        FROM
            (SELECT race, AVG(outpatient_beneficiary_responsibility) AS avg_outpt,
                (SELECT AVG(outpatient_beneficiary_responsibility)
                FROM beneficiary_sample_2010
                WHERE {0}='t') AS overall_avg
            FROM beneficiary_sample_2010
            WHERE {0}='t'
            GROUP BY race) AS sub_query
        ORDER BY avg_outpt DESC""".format(cleaned_col)
        cur.execute(query) #execute query
        result = cur.fetchall() # fetch all results
        for row in result:
            disease_avg_dict = {'avg_outpatient_bene_resp': row['avg_outpt'], 'deviation':row['deviation']}
            race = {row['race']:disease_avg_dict}
            outpt_bene_resp.append(race)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return outpt_bene_resp


def percent_comorbidities(lower_bound, upper_bound):
    """
    Get the percent of comorbidities of heart failure with ischemic heart disease, diabetes, & stroke/transient ischemic
    attack for diseased individuals, given specific age intervals [lower_bound, upper_bound).

    Parameters
    ----------
    lower_bound : int, str, unicode
        Lower bound of age group, including endpoint (start interval).

    upper_bound: int, str, unicode
        Upper bound of age group, excluding endpoint (end interval).

    Returns
    -------
    json
        A labeled JSON object with the percent of diseased individuals with the following comorbidities: heart failure
        +  ischemic heart disease, heart failure + diabetes, heart failure + stroke/transient ischemic attack.
    """
    accepted_ages = range(102)
    try:
        if int(lower_bound) not in accepted_ages:
            raise AssertionError("Lower Bound '{0}' is not allowed".format(lower_bound))
        if int(upper_bound) not in accepted_ages:
            raise AssertionError("Upper Bound '{0}' is not allowed".format(upper_bound))
        if int(lower_bound) > int(upper_bound):
            raise AssertionError("Lower Bound '{0}' greater than upper bound '{1}' is not allowed".format(lower_bound, upper_bound))
        if type(lower_bound) == float or type(upper_bound) == float:
            raise AssertionError("Float values for age value is not allowed")
        con, cur = cursor_connect(psycopg2.extras.DictCursor)  # access retrieved records as Python dict
        query = """SELECT ROUND((SUM(CASE
	      WHEN (heart_failure='t' AND ischemic_heart='t') THEN 1 ELSE 0
        END)*100.0)/COUNT(*),2) AS perc_hf_ih,
        ROUND((SUM(CASE
	      WHEN (heart_failure='t' AND diabetes='t') THEN 1 ELSE 0
        END)*100.0)/COUNT(*),2) AS perc_hf_db,
        ROUND((SUM(CASE
	      WHEN (heart_failure='t' AND stroke_ischemic_attack='t') THEN 1 ELSE 0
        END)*100.0)/COUNT(*),2) AS perc_hf_stroke
        FROM
	      (SELECT *, FLOOR((dod-dob)/365::float) AS age
	      FROM beneficiary_sample_2010
	      WHERE dod IS NOT NULL) AS sub_query
        WHERE AGE >={0} AND AGE <{1}; """.format(str(lower_bound), str(upper_bound))
        cur.execute(query) # execute query
        result = cur.fetchall() #fetch all results
        perc_comorbidities = {'percent_heart_fail_ischemic_heart': float(result[0]['perc_hf_ih']), # comorbidity: heart failure + ischemic heart
                              'percent_heart_fail_diabetes': float(result[0]['perc_hf_db']), # comorbidity: heart failture + diabetes
                              'percent_heart_fail_stroke': float(result[0]['perc_hf_stroke'])} # comorbidity: heart failure + stroke
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return perc_comorbidities


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
        con, cur = cursor_connect(psycopg2.extras.DictCursor)
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
    cursor_connect()
    # print disease_frequency('diabetes')
    # print disease_bene_resp('diabetes')
    print percent_comorbidities('60',70)
