# QC metrics
Module to grab QC metrics from pipelines

Step 1: Verify that homebrew is working properly::

    $ brew doctor

Step 2: Install or update dependencies::

    $ brew install python2.7
    $ brew install requests
    $ pip install dxpy

Step 3: Setup dx-tools and configure::
  
    Follow the tutorial here - https://wiki.dnanexus.com/Downloads#DNAnexus-Platform-SDK
    
Step 3: Checkout the repo::

    $ git clone --recursive https://github.com/nikhilRP/data_provenance.git

Step 4: Fill out authorization details in "auth.json"

Step 5: Fill out "properties.json" and mention the properties to be retrieved from DNA NEXUS

TODO::

    Add sample "properties.json" file
