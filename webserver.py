from subprocess import Popen
from flask import Flask, request
import argparse
import psutil
import json

from model import runtime_prediction, queuetime_prediction

app = Flask(__name__)

instance2cost = {
    # unit cost per hour
    'c5.9xlarge': 0.,
}

def check_for_process(cmd):
    # https://psutil.readthedocs.io/en/latest/#psutil.Process.as_dict
    adict = {}
    for p in psutil.process_iter(['name','cmdline']):
        pdict = p.as_dict()
        if pdict['cmdline'] is not None:
            if isinstance(pdict['cmdline'], list):
                cmdline = ' '.join(pdict['cmdline'])
            else:
                cmdline = pdict['cmdline']
            if cmd in cmdline:
                return pdict
    return None

@app.route('/update', methods=['GET'])
def update():
    '''
    Launch a subprocess that adds new jobs to the model database.
    '''
    UPDATE_CMD = "python update.py"
    jdata = {} # json data to return

    # check if process is running, if not, launch it
    proc = check_for_process(cmd=UPDATE_CMD)
    if proc is not None:
        message = "process already running"
        jdata['pid'] = proc['pid']
    else:
        # may vary depending on the machine
        p = Popen(UPDATE_CMD.split(' '))
        message = "process launched"
        jdata['pid'] = check_for_process(cmd=UPDATE_CMD)['pid']

    jdata['message'] = message
    return json.dumps(jdata)

@app.route('/runtime', methods=['GET'])
def run_times():
    '''
     """ Query for the runtime of a process, must provide a process name and instance type. 

        Example:
            curl "localhost:5000/runtime?jobtype=job-standard*&instance=*"
    '''
    jobtype = request.args.get('jobtype')
    instance = request.args.get('instance')
    if jobtype == None or instance == None:
        return f'Please specify jobtype ({jobtype}) and instance ({instance})\n'

    mean,stdev,_,_ = runtime_prediction(jobtype, instance)

    jdata = {
        'name': jobtype,
        'instance': instance,
        'mean': f"{mean*24*60*60:.2f}",
        'stdev': f"{stdev*24*60*60:.2f}",
        'units': 'seconds'
    }
    return json.dumps(jdata)

@app.route('/runcost', methods=['GET'])
def run_cost():
    '''
     """ Query for the runtime of a process and estimate its cost based on the instance, 
            must provide a process name and instance type. 

        Example:
            curl "localhost:5000/runcost?jobtype=job-standard*&instance=*"
    '''
    jobtype = request.args.get('jobtype')
    instance = request.args.get('instance')
    unitcost = instance2cost.get(instance, 0)

    if jobtype == None or instance == None:
        return f'Please specify jobtype ({jobtype}) and instance ({instance})\n'

    mean,stdev,_,_ = runtime_prediction(jobtype, instance)

    jdata = {
        'name': jobtype,
        'instance': instance,
        'mean': f"{mean*24*unitcost:.2f}",
        'stdev': f"{stdev*24*unitcost:.2f}",
        'units': 'USD'
    }
    return json.dumps(jdata)

@app.route('/queuetime', methods=['GET'])
def queue_times():
    '''
     """ Query for the wait time of jobs the queue, must provide
            the number of jobs to get. List is sorted oldest to newest.

        Example:
            curl "localhost:5000/queue?size=4444&nodes=5"
    '''
    size = request.args.get('size',4444)
    nodes = float(request.args.get('nodes',1))
    qmin, qmax, njobs = queuetime_prediction(nodes=nodes, size=size)

    qdata = {
        'name': 'Queue Time Estimate',
        'njobs': njobs,
        'min': f"{qmin:.3f}",
        'max': f"{qmax:.3f}",
        'units': 'day'
    }
    return json.dumps(qdata)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Smart on demand analysis of multi-cloud performance model')

    # ------ Positional arguments
    parser.add_argument('--host', action='store', type=str, default='localhost',
                        help='Hostname or IP address')
    parser.add_argument('--port', action='store', type=int, default=5000,
                        help='https server port')
    parser.add_argument('--debug',action='store_true', default=False,
                        help='Debug mode')
    # parse arguments
    args = parser.parse_args()

    app.run(debug=True)
    #app.run(host='soamc-mozart.jpl.nasa.gov', debug=True)
