import dxpy
import json
import requests
from urllib import urlencode

EPILOG = __doc__
HEADERS = {'content-type': 'application/json'}
_PARAMS = {
    'format': ['json'],
    'limit': ['all'],
    'field': ['files.accession', 'accession', 'assay_term_name']
}


def get_assay_JSON(url):
    path = '%s&%s' % (url, urlencode(_PARAMS, True))
    json_data = open('auth.json')
    data = json.load(json_data)
    response = requests.get(path, auth=(data['encoded']['AUTHID'],
                                        data['encoded']['AUTHPW']))
    for exp in response.json()['@graph']:
        yield exp


def set_dx_properties():
    """
    Loads DX properties from auth.json file and sets the session
    """
    json_data = open('auth.json')
    data = json.load(json_data)
    try:
        dxpy.set_workspace_id(data['dnanexus']['project'])
    except:
        return False
    return True


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
    parser.add_argument('--post-files', help="Post files to encoded",
                        action='store_true', default=False)

    args = parser.parse_args()
    if not set_dx_properties:
        print "Please set valid DNANEXUS project in the auth.json"
        raise

    if args.encode_url:
        for exp in get_assay_JSON(args.encode_url):
            pass
    elif args.exp_file:
        pass


if __name__ == '__main__':
    main()
