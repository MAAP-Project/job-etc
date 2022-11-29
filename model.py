import os
import sys
import json
import logging
import warnings
import argparse
import requests
import numpy as np
from astropy.time import Time
warnings.filterwarnings('ignore')

from sql_database import SQLDatabase

es_endpoint = "http://localhost:9200"

def runtime_prediction(jobtype, instance="c5.9xlarge", size=100):
    """ Returns the average and standard deviation of the runtime for a job type.

    Parameters
    ----------
    jobtype : str
        Name of the job type to search for
    
    instance : str
        Name of the instance running the job
    
    size : int
        Number of hits to use to compute the average

    url : str
        Elastic search endpoint

    Returns
    -------
    run_avg : float
        Average runtime of the job type in days
    
    run_std : float
        Standard deviation of the runtime of the job type in days

    run_low : float
        Lower percentile of runtime
    
    run_high : float
        Upper percentile of runtime
    """

    jobs = return_jobs_sql(jobtype, instance, size=size)
    
    if len(jobs) == 0:
        print(f"No {jobtype} found in db...")
        return 0,0

    # simple model
    run_times = np.array([job['run_time'] for job in jobs])

    # mask outliers
    mask = run_times < np.percentile(run_times, 90)

    if np.isnan(run_times.mean()):
        return 0,0,0,0 # not enough historical data
    else:
        if len(run_times) == 1:
            return run_times[0], run_times[0], run_times[0], run_times[0]
        elif len(run_times) < 10:
            return np.median(run_times), np.std(run_times), np.min(run_times), np.max(run_times)
        else:
            return np.median(run_times[mask]), np.std(run_times[mask]), np.percentile(run_times[mask],1), np.percentile(run_times[mask],99)

def return_jobs_sql(jobtype, instance, size=100, sqldb='job.db'):
    # query for N jobs of type job_type
    db = SQLDatabase()
    db.open(sqldb)

    if jobtype == "*" and instance == "*":
        jerbs = db.table_query("job_times", "*", "", [] )
    elif jobtype == "*" and instance != "*":
        jerbs = db.table_query("job_times", "*", "instance=?", [instance] )
    elif instance == "*":
        jerbs = db.table_query("job_times", "*", "job_type=?", [jobtype] )
    else:
        jerbs = db.table_query("job_times", "*", "job_type=? AND instance=?", [jobtype,instance] )

    db.close()

    jdata = []
    for job in jerbs:
        jdata.append({
            'job_id': job[0],
            'job_type': job[1],
            'instance': job[2],
            'run_time': job[3],
            'time_start': job[4]
        })

        try:
            jdata[-1]['params'] = job[5]
        except:
            pass

    del jerbs
    return jdata

def get_queue(size=1000):
    """ Returns the jobs in the queue

    Parameters
    ----------
    size : int
        Number of hits to use to compute the average

    url : str
        Elastic search endpoint

    Returns
    -------
    jobs : list of dicts
        List of jobs currently queued
    """
    # build query
    query = {"query":{"bool":{"must":[{"wildcard":{"type":"*"}},{"match":{"status":"job-queued"}}],
                "must_not":[],"should":[]}},"from":0,"size":size,"sort":[{"@timestamp":{"order":"asc"}}],"aggs":{}}

    # query for jobs queued
    #endpoint = os.path.join(es_endpoint, "job_status-current/_search")
    endpoint = os.path.join(es_endpoint, "_search")

    res = requests.post(endpoint, data=json.dumps(query), headers={"Content-Type":"application/json"})#, verify=False, auth=(os.environ['JUSERNAME'], os.environ['JPASSWORD']))

    if res.status_code == 200:
        search_result = res.json()
        print("Number of jobs in queue:", search_result['hits']['total'])
        print("  jobs returned:", len(search_result['hits']['hits']))
        return search_result['hits']['hits']
    else:
        print("Error:", res.status_code)
        print(res.text)
        return None

def get_jobs_started(size=100):
    """
    Returns the jobs that have been started

    Parameters
    ----------
    size : int
        Number of hits to use to compute the average

    url : str
        Elastic search endpoint
    """
    query = {"query":{"bool":{"must":[{"wildcard":{"type":"*"}},{"match":{"status":"job-started"}}],
                "must_not":[],"should":[]}},"from":0,"size":size,"sort":[{"@timestamp":{"order":"asc"}}],"aggs":{}}

    # query for jobs queued
    endpoint = os.path.join(es_endpoint, "job_status-current/_search")
    res = requests.post(endpoint, data=json.dumps(query), verify=False, auth=(os.environ['JUSERNAME'], os.environ['JPASSWORD']))

    if res.status_code == 200:
        search_result = res.json()
        print("Number of jobs currently-started:", search_result['hits']['total'])
        print("  jobs returned:", len(search_result['hits']['hits']))
        return search_result['hits']['hits']
    else:
        print("Error:", res.status_code)
        print(res.text)
        return None

def estimate_time_to_complete():
    """
    Estimate time for jobs in the queue to complete

    Parameters
    ----------
    size : int
        Number of hits to use to compute the average
    """

    sjobs = get_jobs_started()

    # extract unique jobs
    job_types = [job['_source']['type'] for job in sjobs]
    ujobs = set(job_types)
    jdata = {}

    if verbose:
        print(f"Number of unique jobs in queue: {len(ujobs)}")

    # for each job type query for the runtime
    for job in ujobs:
        jdata[job] = {}
        run_avg, run_std, _, _ = runtime_prediction(job, instance="*") # TODO replace with actual instance
        jdata[job]['run_avg'] = run_avg
        jdata[job]['run_std'] = run_std
        jdata[job]['count'] = job_types.count(job)

    # loop over current jobs and compute the time to completion
    times = []
    for job in sjobs:
        ts = Time(job['_source']['job']['job_info']['time_start'])
        dt = Time.now().jd - ts.jd # how long the job has been running, TODO check timezones, UTC?
        ert = jdata[job['_source']['type']]['run_avg'] #+ jdata[job['_source']['type']]['run_std'] # est. run time
        time_left = max(0, ert - dt) # sometimes is negative
        times.append(time_left)

    return times # days

def queuetime_prediction(nodes=1, size=4000):
    """ Returns a dictionary of the result for the given target

    Parameters
    ----------
    nodes : int
        Number of nodes / parallel instances running jobs

    size : int
        Number of jobs returned in queue for calculation
    
    Returns
    -------
    qmin : float
        Minimum time to complete all jobs in the queue (-1 stdev)

    qmax : float
        Maximum time to complete all jobs in the queue (+1 stdev)

    njobs : int
        Number of jobs in the queue
    """
    qjobs = get_queue(size=size)

    # extract unique jobs
    job_types = [job['_source']['type'] for job in qjobs]
    ujobs = set(job_types)
    jdata = {}

    # for each unique job type query for the runtime
    for job in ujobs:
        jdata[job] = {}
        run_avg, run_std, _, _ = runtime_prediction(job, instance="*")
        jdata[job]['run_avg'] = run_avg
        jdata[job]['run_std'] = run_std
        jdata[job]['count'] = job_types.count(job)
        print(job, run_avg)

    # compte avg, min, max, std for queue time
    qtime = []
    qmax = []
    qmin = []
    for qjob in job_types:
        qtime.append(jdata[qjob]['run_avg'])
        qmax.append(jdata[qjob]['run_avg'] + jdata[qjob]['run_std'])
        qmin.append( max(0, jdata[qjob]['run_avg'] - jdata[qjob]['run_std']))

    # time in days
    return np.sum(qmin)/nodes, np.sum(qmax)/nodes, len(job_types)

def plot_stats():

    import matplotlib.pyplot as plt
    
    # get unique jobs
    jobs = return_jobs_sql('*', 'c5.9xlarge')
    run_times = np.array([job['run_time'] for job in jobs])
    job_types = [job['job_type'] for job in jobs]

    ujobs = list(set(job_types))

    # compute stdev for each job type
    jdata = {}
    newjobs = []
    for job in ujobs:
        run_avg, run_std, run_min, run_max = runtime_prediction(job, instance="c5.9xlarge")
        if run_avg < 1e-6 or run_std < 1e-6:
            continue
        jdata[job] = {}
        jdata[job]['run_avg'] = run_avg
        jdata[job]['run_std'] = run_std
        jdata[job]['run_min'] = run_min
        jdata[job]['run_max'] = run_max
        jdata[job]['count'] = job_types.count(job)
        # percent error
        jdata[job]['perr'] = (jdata[job]['run_std']/ jdata[job]['run_avg']) * 100
        newjobs.append(job)

    perr = np.array([jdata[job]['perr'] for job in newjobs])
    avgs = np.array([jdata[job]['run_avg'] for job in newjobs])
    stds = np.array([jdata[job]['run_std'] for job in newjobs])
    counts = np.array([jdata[job]['count'] for job in newjobs])
    mins = np.array([jdata[job]['run_min'] for job in newjobs])
    maxs = np.array([jdata[job]['run_max'] for job in newjobs])
    si = np.argsort(counts)[::-1]
    mask = si[:25]

    # mask to 25 most popular jobs
    perr = perr[mask]
    avgs = avgs[mask]
    stds = stds[mask]
    counts = counts[mask]
    mins = mins[mask]
    maxs = maxs[mask]
    newjobs = np.array(newjobs)[mask]

    # sort data in run time
    si = np.argsort(avgs)
    perr = perr[si]
    avgs = avgs[si]
    stds = stds[si]
    counts = counts[si]
    mins = mins[si]
    maxs = maxs[si]
    newjobs = np.array(newjobs)[si]

    # plot jobs with highest percent error
    plt.figure(figsize=(10,10))
    plt.bar(newjobs, avgs*24*60, alpha=0.5, label='Model Estimate')

    for x in np.arange(25):
        if x == 0:
            plt.plot([x,x], [mins[x]*24*60, maxs[x]*24*60], 'k--', alpha=0.75, label='Historical Data [min-max]')
        else:
            plt.plot([x,x], [mins[x]*24*60, maxs[x]*24*60], 'k--', alpha=0.75)

    plt.errorbar(np.arange(25)+0.1, avgs*24*60, yerr=stds*24*60, fmt='.', ls='none', color='blue',alpha=0.5, label='Model Uncertainty')
    plt.ylabel("Run Time [min]")
    plt.legend(loc='best')
    plt.title("Performance estimate for 25 of the most popular jobs")
    plt.ylim([0,1])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()
