#!/usr/bin/env python
"""
Cron script to submit a job that updates job etc model
"""

from datetime import datetime, timedelta
import argparse
from hysds.celery import app
from hysds_commons.job_utils import submit_mozart_job


def get_job_params(job_type, job_name, model_url, version, tag):

    rule = {
        "rule_name": job_type.lstrip('job-'),
        "queue": "factotum-job_worker-small",
        "priority": 5,
        "kwargs": '{}'
    }

    params = [
      {
          "name": "model_url_opt",
          "from": "value",
          "value": "--model_url"
      },
      {
          "name": "model_url",
          "from": "submitter",
          "value": model_url
      },
      {
          "name": "version_opt",
          "from": "value",
          "value": "--version"
      },
      {
          "name": "version",
          "from": "submitter"
          "value": version
      },
      {
          "name": "tag_opt",
          "from": "value",
          "value": "--tag"
      },
      {
          "name": "tag",
          "from": "submitter"
          "value": tag
      }
    ]

    return rule, params


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model_url", help="url to performance estimator model", default="job.db", required=False)
    parser.add_argument("--host", help="host running performance estimator service", default="http://localhost:5000", required=False)
    parser.add_argument("--version", help="PGE docker image tag (release, version or branch) to propagate", default="main", required=False)
    parser.add_argument("--tag", help="human-readable job identifier", default="performance_estimator", required=False)
    args = parser.parse_args()

    rtime = datetime.utcnow()

    job_type = "job-performance_estimator_model_update"
    job_spec = "{}:{}".format(job_type, args.version)
    job_name = "%s-%s" % (job_spec, rtime.strftime("%d_%b_%Y_%H:%M:%S"))


    # get mozart job params
    rule, params = get_job_params(job_type, job_name, args.model_url, args.version)

    print("submitting job of type {}".format(job_spec))
    submit_mozart_job({}, rule,
        hysdsio = {"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": job_spec},
        job_name = job_name)