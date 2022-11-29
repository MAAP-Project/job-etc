import os
import json
import argparse
import requests
import numpy as np
from astropy.time import Time

from sql_database import SQLDatabase

from hysds.celery import app

# new metrics: http://localhost:9200
def return_jobs(jobtype="*", instance="*", start_idx=0, start_timestamp="2020-01-01T00:00:00",
                es_index="_search", es_endpoint="http://localhost:9200", status="job-completed", 
                size=1000, verbose=False, return_total=False):

    # set up elastic search query
    query = {"query":{"bool":{ 
        "must":[
            {"wildcard":{"type":jobtype}},
            {"match":{"status":status}},
            {"wildcard":{"job.job_info.execute_node":instance}},
            {"range":{"@timestamp":{"gt":start_timestamp}}}
        ],
        "must_not":[],
        "should":[]}},
        "from":start_idx,
        "size":size,
        "sort":[{"@timestamp":{"order":"asc"}}], # oldest first
        "aggs":{}
    }

    # query end point
    endpoint = os.path.join(es_endpoint, es_index)

    if "localhost" in endpoint:
        res = requests.post(endpoint, data=json.dumps(query), headers={"Content-Type":"application/json"})
    else:
        res = requests.post(endpoint, data=json.dumps(query), verify=False)

    # parse response
    jobs = []
    if res.status_code == 200:
        search_result = res.json()
        jobs.extend(search_result['hits']['hits'])
        if verbose:
            print("search results for completed jobs:", search_result['hits']['total'])
            print("   values returned:", len(search_result['hits']['hits']))
    else:
        print("Error:", res.status_code)

    if return_total:
        return jobs, search_result['hits']['total']
    else:
        return jobs

def create_backup_table(table_name):
    """ Create a SQL database to store job information

    Parameters
    ----------
    table_name : str
        Name of SQL database on disk
    
    Returns
    -------

    """

    if not os.path.exists(table_name):
        db = SQLDatabase()

        db.create_db(table_name)

        columns = ("uid integer primary key autoincrement, job_type text, "
                   "instance text, run_time real, timestamp datetime, data text")

        db.create_table('job_times', columns=columns)
        db.close()
    else:
        print(f"Database already exists: {table_name}")


def populate_backup_table(table_name):
    """ Populate SQL database with job information
    
    Parameters
    ----------
    table_name : str
        Name of SQL database on disk
    
    Returns
    -------

    """
    # if table does not exist, create it
    if not os.path.exists(table_name):
        create_backup_table(table_name)

    # query for most recent timestamp
    db = SQLDatabase()
    db.open(table_name)
    rows = db.table_query("job_times", "MAX(timestamp)", "", [])
    if len(rows) > 0:
        recent_timestamp = rows[0][0]
    else:
        recent_timestamp = "2020-01-01T00:00:00"
    db.close()

    # quick elastic search to get total number of jobs
    jobs, total = return_jobs(jobtype="*", instance="*", size=1, verbose=True,
                              start_idx=0, start_timestamp=recent_timestamp,
                              return_total=True)

    # loop over all the jobs in es 
    for i in range(0,total['value'],1000):
        
        # query to get jobs
        jobs = return_jobs(jobtype="*", instance="*", verbose=False, 
                           size=1000, start_idx=i, return_total=False)

        # compute queued, started and completed time for each
        for job in jobs:
            try:
                tq = Time(job['_source']['job']['job_info']['time_queued'])
                ts = Time(job['_source']['job']['job_info']['time_start'])
                te = Time(job['_source']['job']['job_info']['time_end'])

                job['queue_time'] = ts.jd - tq.jd # queue time
                job['run_time'] = te.jd - ts.jd # run time
            except:
                job['queue_time'] = 0
                job['run_time'] = 0

        print(i, jobs[0]['_source']['@timestamp'], jobs[-1]['_source']['@timestamp'])

        # extract job information
        run_times = np.array([job['run_time'] for job in jobs])
        instances = np.array([job['_source']['job']['job_info'].get('facts',{}).get('ec2_instance_type','') for job in jobs])
        job_types = np.array([job['_source']['type'] for job in jobs])
        timestamp = np.array([job['_source']['@timestamp'] for job in jobs])

        params = []
        for j, job in enumerate(jobs):
            if zmask[j]:
                continue
            try:
                params.append(json.dumps(job['_source']['job']['params']['job_specification']['params']))
            except:
                params.append("")

        # insert jobs into database
        db.open(table_name)
        for j in range(len(timestamp)):

            # check for duplicate before inserting
            count = db.count_rows("job_times", "*", 
                            "timestamp = ? AND job_type = ? AND instance = ?", 
                            [timestamp[j], job_types[j], instances[j]])

            if count == 0:
                db.insert_records("job_times",
                                  {"job_type":job_types[j], "instance":instances[j], 
                                  "run_time":run_times[j], "timestamp":timestamp[j]}) 
                                  #"params":params[j]} )

        db.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sqldb', default='job.db', type=str, help='SQLite database file')
    parser.add_argument('--cadence', default=0, type=float, help='Cadence in days')
    return  parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    status = 0
    try:
        populate_backup_table(args.sqldb)
    except Exception as e:
        status = 1
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    
    sys.exit(status)
