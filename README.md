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

    $ git clone --recursive https://github.com/nikhilRP/data_provenance.git

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
