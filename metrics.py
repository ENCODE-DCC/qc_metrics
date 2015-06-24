import dxpy
import json
import requests
import time
from urllib import urlencode
from dxpy.exceptions import DXError
from dxpy.bindings import (
    dxjob,
    dxfile,
    dxanalysis,
    dxapplet
)

EPILOG = __doc__
HEADERS = {'content-type': 'application/json'}
data = json.load(open('properties.json'))


def post_encode_object(collection, props):
    path = '%s/%s/' % (data['encode_server'], collection)
    response = requests.post(path, auth=(
        data['encode_authid'], data['encode_authpw']),
        data=json.dumps(props),
        headers=HEADERS)
    if response.status_code == 201:
        return response.json()['@graph'][0]['@id']
    else:
        return None


def patch_file(props):
    """
    Patches the file with QC metrics and analysis step run details
    """
    pass


def check_workflow_run(analysis):
    """
    Checks if the workflow run already exists on the server
    if yes - return workflow_run '@id'
    else - None
    """
    params = {
        'field': ['@id'],
        'format': ['json'],
        'dx_analysis_id': [analysis],
        'type': ['workflow_run']
    }
    path = '%s/search/?%s' % (data['encode_server'], urlencode(params, True))
    response = requests.get(path, auth=(
        data['encode_authid'], data['encode_authpw']))
    if len(response.json()['@graph']):
        return response.json()['@graph'][0]['@id']
    else:
        return None


def post_workflow_run(analysis, pipeline):
    workflow_run = {
        'dx_analysis_id': analysis.id,
        'aliases': ['dnanexus:%s' % analysis.id],
        'pipeline': pipeline.get('@id', None),
        'started_running': time.strftime(
            '%Y-%m-%dT%H:%M:%SZ',
            time.gmtime(analysis.created / 1000.0)
        ),
        'stopped_running': time.strftime(
            '%Y-%m-%dT%H:%M:%SZ',
            time.gmtime(analysis.modified / 1000.0)
        ),
        'status': 'finished' if analysis.state == 'done' else analysis.state
    }
    workflow_run = post_encode_object('workflow_run', workflow_run)
    if workflow_run is None:
        return None
    else:
        return workflow_run


def check_step_run(alias):
    """
    Checks if the step run already exists on the server
    if yes - return analysis_step_run '@id'
    else - None
    """
    params = {
        'field': ['@id'],
        'format': ['json'],
        'aliases': ['dnanexus:%s' % alias],
        'type': ['analysis_step_run']
    }
    path = '%s/search/?%s' % (data['encode_server'], urlencode(params, True))
    response = requests.get(path, auth=(data['encode_authid'],
                                        data['encode_authpw']))
    if len(response.json()['@graph']):
        return response.json()['@graph'][0]['@id']
    else:
        return None


def post_step_run(job, workflow_run, analysis_step):
    """
    Post the step run to the specificed encode server
    """
    applet = dxapplet.DXApplet(job.applet)
    step_run = {
        'analysis_step': analysis_step,
        'workflow_run': workflow_run,
        'dx_applet_details': {
            'dx_job_id': job.id,
            'dx_app_json': applet.describe(),
            'started_running': time.strftime(
                '%Y-%m-%dT%H:%M:%SZ',
                time.gmtime(job.startedRunning / 1000.0)
            ),
            'stopped_running': time.strftime(
                '%Y-%m-%dT%H:%M:%SZ',
                time.gmtime(job.stoppedRunning / 1000.0)
            ),
            'dx_status': 'finished' if applet.state == 'done' else applet.state
        },
        'status': 'finished' if job.state == 'done' else job.state
    }
    step_run_id = post_encode_object('analysis_step_run', step_run)
    if workflow_run is None:
        return None
    else:
        return step_run_id


def load_dx_metadata(props, pipeline):
    """
    Takes file properties and parses out dx metadata from notes field
    returns updated file props with analysis step run and pipeline
    """
    notes_json = json.loads(props['notes'])
    if 'dx-createdBy' not in notes_json:
        return
    job = dxjob.DXJob(notes_json['dx-createdBy']['job'])
    file = dxfile.DXFile(notes_json['dx-id'])
    analysis = dxanalysis.DXAnalysis(job.analysis)
    props['aliases'].append('dnanexus:%s' % file.id)

    # if there is no workflow_run post it
    workflow_run = check_workflow_run(analysis.id)
    if workflow_run is None:
        workflow_run = post_workflow_run(analysis, pipeline)

    for stage in analysis.stages:
        if stage.get('id') == job.stage:
            stage_name = stage['execution']['name']
            break

    # if there is no step run post it
    step_run = check_step_run(job.id)
    if step_run is None:
        for stage in data['analysis_steps']:
            if data['analysis_steps'][stage]['dx_stage_name'] == stage_name:
                analysis_step = stage
        #Manual hack for methaltion pipeline
        if props['file_format'] == "bigBed" and data['dx_project'] == \
                "project-BKf7zV80z53QbqKQz18005vZ":
            analysis_step = "/analysis-steps/bigbed-conversion-v-2-6/"

    step_run = post_step_run(job, workflow_run, analysis_step)
    props['step_run'] = step_run
    patch_file(props)


def get_encode_object(href):
    """
    Retrieves object from specified ENCODE server
    """
    path = '%s%s' % (data['encode_server'], href)
    response = requests.get(path, auth=(data['encode_authid'],
                                        data['encode_authpw']))
    if response.status_code == requests.codes.ok:
        return response.json()
    else:
        return None


def get_assay_JSON(url):
    """
    Searches and returns one experiment at a time
    """
    params = {
        'format': ['json'],
        'limit': ['all'],
        'field': ['accession', 'original_files']
    }
    path = '%s&%s' % (url, urlencode(params, True))
    response = requests.get(path, auth=(data['encode_authid'],
                                        data['encode_authpw']))
    for exp in response.json()['@graph']:
        yield exp


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Module to grab QC metrics from ENCODE uniform processing pipelines",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--encode-url', help="ENCODE assay search URL")
    group.add_argument('--dx-file', help="Text file with DNA NEXUS IDs")
    parser.add_argument('--post-metrics', help="Post metrics to encoded",
                        action='store_true', default=False)

    args = parser.parse_args()
    pipeline = get_encode_object(data['pipeline'])

    try:
        dxpy.set_workspace_id(data['dx_project'])
    except DXError:
        print "Please enter a valid project ID in auth.json"
    else:
        if args.encode_url:
            for exp in get_assay_JSON(args.encode_url):
                for f in exp['original_files']:
                    f_json = get_encode_object(f)
                    if 'notes' in f_json:
                        load_dx_metadata(f_json, pipeline)
                print "Done - " + exp['accession']
        elif args.dx_file:
            pass


if __name__ == '__main__':
    main()
