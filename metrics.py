import dxpy
import json
import requests
from urllib import urlencode

EPILOG = __doc__
HEADERS = {'content-type': 'application/json'}
_PARAMS = {
    'format': ['json'],
    'limit': ['all'],
    'field': ['original_files']
}


def get_file_metadata(href, encode_server, authid, authpw):
    path = '%s%s' % (encode_server, href)
    response = requests.get(path, auth=(authid, authpw))
    return response.json()


def get_assay_JSON(url, authid, authpw):
    path = '%s&%s' % (url, urlencode(_PARAMS, True))
    response = requests.get(path, auth=(authid, authpw))
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
    json_data = open('auth.json')
    data = json.load(json_data)
    authid = data['encoded']['authid']
    authpw = data['encoded']['authpw']

    dxproject = dxpy.DXProject(data['dnanexus']['project'])
    if args.encode_url:
        data_dir = data['dnanexus']['data_dir']
        folders = dxpy.api.project_list_folder(
            dxproject.id,
            input_params={'folder': data_dir}
        )['folders']
        folders = [folder[(len(data_dir) + 1):] for folder in folders]
        for exp in get_assay_JSON(args.encode_url, data['encoded']['server'],
                                  authid, authpw):
            if exp['accession'] not in folders:
                continue
            for f in exp['original_files']:
                f_json = get_file_metadata(f, authid, authpw)
    elif args.exp_file:
        pass


if __name__ == '__main__':
    main()
