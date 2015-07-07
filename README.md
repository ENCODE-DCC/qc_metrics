# QC metrics
Module to grab QC metrics from pipelines

Step 1: Verify that homebrew is working properly

    $ brew doctor

Step 2: Install or update dependencies

    $ brew install python2.7
    $ brew install requests
    $ pip install dxpy

Step 3: Setup dx-tools and configure

    Follow the tutorial here - https://wiki.dnanexus.com/Downloads#DNAnexus-Platform-SDK

Step 3: Checkout the repo

    $ git clone --recursive https://github.com/ENCODE-DCC/qc_metrics.git

Step 4: Fill out authorization details and properties in "properties.json"

    {
        "encode_server": "",
        "encode_authid": "",
        "encode_authpw": "",
        "pipeline": "",
        "dx_project": "",
        "analysis_steps": {
            "": {
                "dx_stage_name": "",
                "metrics": {}
            },
            "": {
                "dx_stage_name": "",
                "metrics": {}
            },
            "": {
                "dx_stage_name": "",
                "metrics": {}
            },
            "": {
                "dx_stage_name": "",
                "metrics": {}
            }
        }
    }

Step 5: Run below command to start posting metrics to the server listed
    
    python2.7 metrics.py --encode-url <search-URL>

    eg:
    python2.7 metrics.py --encode-url https://<server-name>/search/?type=experiment&assay_term_name=whole-genome%20shotgun%20bisulfite%20sequencing
    
Notes: Included "properties-dna-me.json" file as an example proerties file.

TODO::

    1. Extend the functionality for different assays. Not been tested on any other assay other than WGBS.
    2. Post and patch have to be implemented in a cleaner way.
    3. Implement providing dna nexus analysis IDs
