import dxpy
import json
import requests
from urllib import urlencode
from dxpy.exceptions import DXError
from dxpy.bindings import (
    dxjob,
    dxfile,
    dxanalysis
)

EPILOG = __doc__
HEADERS = {'content-type': 'application/json'}
_PARAMS = {
    'format': ['json'],
    'limit': ['all'],
    'field': ['accession', 'original_files']
}


def patch_file(props):
    """
    Patches the file with QC metrics and analysis step run details
    """
    pass


def check_step_run(alias):
    """
    Checks if the step run already exists on the server
    if yes - return step_run '@id'
    else - None
    """
    params = {
        'field': ['@id'],
        'format': ['json'],
        'aliases': ['dnanexus:%s' % alias],
        'type': ['analysis_step_run']
    }
    path = '%s/search/?%s' % (ENCODE_SERVER, urlencode(params, True))
    response = requests.get(path, auth=(AUTHID, AUTHPW), verify=False)
    if len(response.json()['@graph']):
        return response.json()['@graph'][0]['@id']
    else:
        return None


def post_step_run(props):
    """
    Post the step run to the specificed encode server
    """
    pass


def load_dx_metadata(props, pipeline):
    """
    Takes file properties and parses out dx metadata from notes field
    returns updated file props with analysis step run and pipeline
    """
    notes_json = json.loads(props['notes'])
    if 'dx-createdBy' not in notes_json:
        return ()
    job = dxjob.DXJob(notes_json['dx-createdBy']['job'])
    file = dxfile.DXFile(notes_json['dx-id'])
    props['aliases'].append('dnanexus:%s' % file.id)
    step_run = check_step_run(job.id)
    if step_run is None:
        # if there is no step run post it
        step_run = post_step_run()
    props['step_run'] = step_run
    patch_file(props)


def get_encode_object(href):
    """
    Retrieves object from specified ENCODE server
    """
    path = '%s%s' % (ENCODE_SERVER, href)
    response = requests.get(path, auth=(AUTHID, AUTHPW), verify=False)
    return response.json()


def get_assay_JSON(url):
    """
    Searches and returns one experiment at a time
    """
    path = '%s&%s' % (url, urlencode(_PARAMS, True))
    response = requests.get(path, auth=(AUTHID, AUTHPW), verify=False)
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

    # Load details from JSON doc
    # TODO: should eliminate globals, terrible programming.
    data = json.load(open('auth.json'))
    global AUTHID, AUTHPW, ENCODE_SERVER
    AUTHID = data['encoded']['authid']
    AUTHPW = data['encoded']['authpw']
    ENCODE_SERVER = data['encoded']['server']

    # disable warning for requests should avoid them in first place
    requests.packages.urllib3.disable_warnings()

    # Check and load pipeline object from the server
    pipeline = get_encode_object(data['encoded']['pipeline'])

    try:
        dxproject = dxpy.DXProject(data['dnanexus']['project'])
    except DXError:
        print "Please enter a valid project ID in auth.json"
    else:
        if args.encode_url:
            data_dir = data['dnanexus']['data_dir']
            folders = dxpy.api.project_list_folder(
                dxproject.id,
                input_params={'folder': data_dir}
            )['folders']
            folders = [folder[(len(data_dir) + 1):] for folder in folders]
            for exp in get_assay_JSON(args.encode_url):
                if exp['accession'] not in folders:
                    continue
                for f in exp['original_files']:
                    f_json = get_encode_object(f)
                    if 'notes' in f_json:
                        load_dx_metadata(f_json, pipeline)
        elif args.dx_file:
            pass


if __name__ == '__main__':
    main()
