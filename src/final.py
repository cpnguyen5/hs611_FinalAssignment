import re
import psycopg2
from psycopg2 import extras
import json
from exceptions import Exception, AssertionError


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

    Examples
    --------
    /api/v1/bene_resp/diabetes -- disease_bene_resp('diabetes')
    /api/v1/bene_resp/osteoporosis -- disease_bene_resp('osteoporosis')
    """
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
        SELECT race, ROUND(avg_outpt::numeric,2)::float AS avg_outpt,
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
        avg_bene_resp = dict() # average beneficiary responsibility
        dev = dict() # deviation from average
        for row in result:
            avg_bene_resp[row['race']]=row['avg_outpt']
            dev[row['race']]=row['deviation']
        outpt_bene_resp = {'average_outpatient_bene_resp': avg_bene_resp,
                           'deviation': dev}
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return outpt_bene_resp


def hmo_mo_max_reimb():
    """
    Get the max value of annual primary payer reimbursement for each bin of Part A coverage hmo months, given
    the sex of beneficiaries that have the most count of comorbidities of rheumatoid and osteo- arthritis and diabetes.
    The query is ordered by the max values of primary payer reimbursement in descending order.

    Parameters
    ----------
    None

    Returns
    -------
    json
        A labeled JSON object with the bins of Part A coverage hmo months and their respective max value of annual
        primary payer reimbursement, filtered for the sex that has the most prevalence for comorbidities of rheumatoid
        & osteo- arthritis.
    Examples
    --------
    /api/v1/max_pp_reimb -- hmo_mo_max_reimb()
    """
    con, cur = cursor_connect(psycopg2.extras.DictCursor)
    query="""
    SELECT part_a_coverage_months, MAX(primary_payer_reimbursement) AS max_primary_reimb
    FROM beneficiary_sample_2010
    WHERE sex IN
        (SELECT sex
        FROM
            (SELECT sex, SUM(CASE
                WHEN rheumatoid_osteo_arthritis=True AND diabetes=True THEN 1 ELSE 0
                END) AS n_comorbid
            FROM beneficiary_sample_2010
            GROUP BY sex
            ORDER BY n_comorbid DESC LIMIT 1) AS sub_q)
    GROUP BY part_a_coverage_months
    ORDER BY max_primary_reimb DESC"""
    cur.execute(query)  # execute query
    result = cur.fetchall()  # fetch results
    hmo_reimb = dict()
    for row in result:
        hmo_reimb["Part A hmo mo - " + str(row['part_a_coverage_months'])] = row['max_primary_reimb']
    max_reimb_dict = {"max_primary_payer_reimbursements_per_hmo_mo": hmo_reimb}
    return max_reimb_dict


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

    Examples
    --------
    /api/v1/perc_comorbidities/50_70 -- percent_comorbidities(50, 70)
    /api/v1/perc_comorbidities/80_90 -- percent_comorbidities('80', 90)
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
        perc_comorbidities = {"heart fail & ischemic heart": float(result[0]['perc_hf_ih']), # comorbidity: heart failure + ischemic heart
                              "heartfail & diabetes": float(result[0]['perc_hf_db']), # comorbidity: heart failture + diabetes
                              "heart_fail & stroke": float(result[0]['perc_hf_stroke'])} # comorbidity: heart failure + stroke
        comorbid_dict = {"percent_comorbidities": perc_comorbidities}
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return comorbid_dict


def osteo_proportion_reimb():
    """
    Get the states and their respective proportion of osteoporosis-related annual Medicare inpatient reimbursement to
    the overall osteoporosis-relate annual Medicare inpatient reimbursement, where the state's proportion is above the
    national average (of osteoporosis-related annual Medicare reimbursement). The query is ordered by the proportion
    in ascending order.

    Parameters
    ----------
    None

    Returns
    -------
    json
        A labeled JSON object with the states and their respective proportion of osteoporosis-related annual Medicare
        inpatient reimbursement, where the state's proportion is above the national average.

    Examples
    --------
    /api/v1/state/prop_inpatient_reimb/osteo -- osteo_proportion_reimb()
    """
    proportion = {}
    con, cur = cursor_connect(psycopg2.extras.DictCursor)
    query = """
    SELECT state, proportion_osteo_inpt_reimb
    FROM
        (SELECT LHS.state AS state, LHS.osteo_inpt_reimb, RHS.total_inpt_reimb,
        (LHS.osteo_inpt_reimb::float/RHS.total_inpt_reimb) AS proportion_osteo_inpt_reimb
        FROM
            (SELECT state, SUM(inpatient_reimbursement) AS osteo_inpt_reimb
            FROM beneficiary_sample_2010
            WHERE osteoporosis=True
            GROUP BY state) AS LHS
            INNER JOIN
                (SELECT state, SUM(inpatient_reimbursement) AS total_inpt_reimb
                FROM beneficiary_sample_2010
                GROUP BY state) AS RHS
            ON LHS.state=RHS.state) AS LLHS
    CROSS JOIN
        (SELECT AVG(proportion_osteo_inpt_reimb) AS avg_osteo_proportion
        FROM
            (SELECT LHS.state AS state, LHS.osteo_inpt_reimb, RHS.total_inpt_reimb,
            (LHS.osteo_inpt_reimb::float/RHS.total_inpt_reimb) AS proportion_osteo_inpt_reimb
            FROM
                (SELECT state, SUM(inpatient_reimbursement) AS osteo_inpt_reimb
                FROM beneficiary_sample_2010
                WHERE osteoporosis=True
                GROUP BY state) AS LHS
                INNER JOIN
                    (SELECT state, SUM(inpatient_reimbursement) AS total_inpt_reimb
                    FROM beneficiary_sample_2010
                    GROUP BY state) AS RHS
                ON LHS.state=RHS.state) AS sub_q) AS RRHS
    WHERE proportion_osteo_inpt_reimb > avg_osteo_proportion
    ORDER BY proportion_osteo_inpt_reimb ASC"""
    cur.execute(query) # execute query
    result = cur.fetchall() # fetch results
    for row in result:
        proportion[row['state']]=row['proportion_osteo_inpt_reimb']
    result_dict = {"osteoporosis_inpatient_reimb_proportion":proportion}
    return result_dict


def median_age():
    """
    Get the median of ages of all individuals that were diagnosed with depression and have the total of hmo coverage
    months that were below the national average.

    Parameters
    ----------
    None

    Returns
    -------
    json
        A labeled JSON object with the median of age of individuals with depression and have the total hmo coverage
        months below the national average.

    Examples
    --------
    /api/v1/median/age -- median_age()
    """
    con, cur = cursor_connect(psycopg2.extras.DictCursor)
    query="""
    SELECT MAX(age)::int as median
    FROM
      (SELECT age, ntile(2) OVER (ORDER BY age ASC) AS bucket
        FROM
	      (SELECT FLOOR(CASE
		    WHEN dod IS NULL THEN ((('2010-01-01'::Date)-dob)/365::float)
			ELSE ((dod-dob)::float/365)
		  END) AS age
	      FROM beneficiary_sample_2010
	      WHERE depression=True AND
		  hmo_coverage_months < (SELECT ROUND(AVG(hmo_coverage_months)) FROM beneficiary_sample_2010)
	      ) AS sub_q
      ) as tile
    WHERE bucket = 1
    GROUP BY bucket;
    """
    cur.execute(query)  # execute query
    result = cur.fetchall()  # fetch results
    age = {"age": result[0]['median']}
    median_dict = {"median": age}
    return median_dict


if __name__ == "__main__":
    cursor_connect()
    # print disease_bene_resp('diabetes')
    # print percent_comorbidities('60',70)
    # print osteo_proportion_reimb()
    # print hmo_mo_max_reimb()
    # print median_age()