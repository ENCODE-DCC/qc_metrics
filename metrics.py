import dxpy
import json
import requests
import time
import sys
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
    elif response.status_code == 422:
        # This should be handled better
        return id
    else:
        return None


def patch_encode_object(collection, props, id):
    """
    Patches the file with QC metrics and analysis step run details
    """
    path = '%s/%s/' % (data['encode_server'], id)
    response = requests.patch(path, auth=(
        data['encode_authid'], data['encode_authpw']),
        data=json.dumps(props),
        headers=HEADERS)
    if response.status_code == 200:
        return response.json()['@graph'][0]['@id']
    elif response.status_code == 422:
        # This should be handled better
        return id
    else:
        return None


def post_workflow_run(analysis):
    path = '%s/dnanexus:%s' % (data['encode_server'], analysis.id)
    response = requests.get(path, auth=(data['encode_authid'], data['encode_authpw']))
    if response.status_code == 200:
        return response.json()['@id']
    else:
        workflow_run = {
            'dx_analysis_id': analysis.id,
            'aliases': ['dnanexus:%s' % analysis.id],
            'pipeline': data.get('pipeline'),
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
        return workflow_run


def post_step_run(job, workflow_run, analysis_step):
    """
    Post the step run to the specificed encode server
    """
    path = '%s/dnanexus:%s' % (data['encode_server'], job.id)
    response = requests.get(path, auth=(data['encode_authid'], data['encode_authpw']))
    if response.status_code == 200:
        return response.json()['@id']
    else:
        applet = dxapplet.DXApplet(job.applet)
        step_run = {
            'analysis_step': analysis_step,
            'workflow_run': workflow_run,
            'aliases': ['dnanexus:%s' % job.id],
            'dx_applet_details': [{
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
                'dx_status': 'finished' if applet.state == 'closed' else applet.state
            }],
            'status': 'finished' if job.state == 'done' else job.state
        }
        step_run_id = post_encode_object('analysis_step_run', step_run)
        return step_run_id


def get_analysis_step(props):
    """
    This method is a dirty hack to make the DNA ME pipeline work
    Terrible programming but can't help it
    """
    if props['file_format'] == "bigBed":
        analysis_step = "/analysis-steps/bigbed-conversion-v-2-6/"
    elif props['file_format'] == "bam":
        analysis_step = "/analysis-steps/mott-trim-align-bismark-v-1-0/"
    elif props['file_format'] == "bed":
        analysis_step = "/analysis-steps/methylation-quantification-bismark-v-1-0/"
    return analysis_step


def post_qc_metrics(ana_step, step_run, exp_details, folder):
    """
    Should load metrics from DNA Nexus stage to file
    TODO: Will have to extend this to other assays
    """
    metrics = {
        'step_run': step_run,
        'assay_term_name': exp_details['assay_term_name'],
        'assay_term_id': exp_details['assay_term_id'],
        'aliases': ['dnanexus:qc-%s' % step_run]
    }
    schema = data['analysis_steps'][ana_step]['metrics']['encode_schema']
    path = '%s/profiles/%s?format=json' % (data['encode_server'], schema)
    response = requests.get(path, headers=HEADERS)
    files = list(dxpy.bindings.search.find_data_objects(
        folder=folder,
        name=data['analysis_steps'][ana_step]['metrics']['file_extensions'],
        name_mode="glob"))
    file_contents = []
    lambda_file_contents = []
    for file in files:
        file_json = dxfile.DXFile(file.get('id'))
        if file_json.folder.endswith('lambda'):
            lambda_file_contents = file_json.read().split('\n')
        else:
            file_contents = file_json.read().split('\n')

    # Debug statements to know which files are posted and which aren't
    if len(file_contents) == 0:
        print "Couldn't load map report for the file - %s" % folder
    if len(lambda_file_contents) == 0:
        print "Couldn't load map report for the file - %s" % folder

    for prop in response.json()['properties']:
        if prop not in ['@id', '@type', 'status', 'step_run', 'schema_version',
                        'assay_term_name', 'assay_term_id', 'applies_to',
                        'status', 'aliases']:
            # once again hack for DNA me pipeline
            if prop.startswith('lambda'):
                for line in lambda_file_contents:
                    if line.startswith(prop[7:]):
                        if response.json()['properties'][prop]['type'] == "number":
                            metrics[prop] = int(line.split('\t')[1])
                        else:
                            metrics[prop] = line.split('\t')[1]
            else:
                for line in file_contents:
                    if line.startswith(prop):
                        if response.json()['properties'][prop]['type'] == "number":
                            metrics[prop] = int(line.split('\t')[1])
                        else:
                            metrics[prop] = line.split('\t')[1]
    path = '%s/dnanexus:qc-%s' % (data['encode_server'], step_run)
    response = requests.get(path, auth=(data['encode_authid'], data['encode_authpw']))
    if response.status_code == 200:
        patch_encode_object('bismark_qc_metric', metrics, response.json()['@id'])
    else:
        post_encode_object('bismark_qc_metric', metrics)


def load_metadata(props, exp_details):
    """
    Takes file properties and parses out dx metadata from notes field
    returns updated file props with analysis step run and pipeline
    """
    notes_json = json.loads(props['notes'])
    if 'dx-createdBy' not in notes_json:
        print "notes field doesn't have DNA nexus metadata"
        return False
    job = dxjob.DXJob(notes_json['dx-createdBy']['job'])
    file = dxfile.DXFile(notes_json['dx-id'])
    analysis = dxanalysis.DXAnalysis(job.analysis)
    props['aliases'].append('dnanexus:%s' % file.id)

    if analysis is None:
        print "Job doesn't have analysis"
        return False

    workflow_run = post_workflow_run(analysis)

    if data['dx_project'] == "project-BKf7zV80z53QbqKQz18005vZ":
        analysis_step = get_analysis_step(props)
    else:
        # grabbing the name of the stage
        for stage in analysis.stages:
            if stage.get('id') == job.stage:
                stage_name = stage['execution']['name']
                break
        # selecting the analysis step
        for stage in data['analysis_steps']:
            if data['analysis_steps'][stage]['dx_stage_name'] == stage_name:
                analysis_step = stage
    step_run = post_step_run(job, workflow_run, analysis_step)
    if step_run is None:
        return False

    if 'metrics' in data['analysis_steps'][analysis_step]:
        post_qc_metrics(analysis_step, step_run, exp_details, job.folder)
    if 'step_run' in props and props['step_run'] == step_run:
        return True
    else:
        patch_dict = {
            'step_run': step_run,
            'aliases': props['aliases']
        }
        status = patch_encode_object('file', patch_dict, props['@id'])
        if status is None:
            return False
        else:
            return True


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
        'field': ['accession', 'original_files',
                  'assay_term_name', 'assay_term_id']
    }
    path = '%s&%s' % (url, urlencode(params, True))
    response = requests.get(path, auth=(data['encode_authid'],
                                        data['encode_authpw']))
    for exp in response.json()['@graph']:
        yield exp


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Module to grab QC metrics from ENCODE pipelines",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--encode-url', help="ENCODE assay search URL")
    group.add_argument('--dx-file', help="Text file with DNA NEXUS IDs")

    args = parser.parse_args()

    try:
        dxpy.set_workspace_id(data['dx_project'])
    except DXError:
        print "Please enter a valid project ID in auth.json"
    else:
        if args.encode_url:
            if 'assay_term_name' not in args.encode_url:
                print "Please select exactly one assay type."
                sys.exit(1)
            for exp in get_assay_JSON(args.encode_url):
                exp_details = {
                    'assay_term_name': exp['assay_term_name'],
                    'assay_term_id': exp['assay_term_id'],
                    'accession': exp['accession']
                }
                print "Started processing the experimet - %s" % exp['accession']
                for f in exp['original_files']:
                    f_json = get_encode_object(f)
                    if 'notes' in f_json:
                        status = load_metadata(f_json, exp_details)
                        if not status:
                            print "Couldn't update the - %s" % f_json['accession']
                print "     Finished processing the experiment - %s" % exp['accession']
        elif args.dx_file:
            pass


if __name__ == '__main__':
    main()
