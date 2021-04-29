# README
This README is a developer README with technical information on the workflow. It is not intended for end users.

# Overview
![](https://github.com/disulfidebond/APOLLO/blob/main/media/APOLLO_README_fig.png)

# Preprocessing Steps
APOLLO requires several files to be formatted to complete the location mapping step. These formatting steps are completed using separate python scripts from the main APOLLO script, and are described below.

## create_business_db.py
This python script takes two input files: the outbreak text file (usually with 'NLP_Outbreak' in the name), and the NLP_RiskAndInterventionShortFields text file.

First, the Outbreak file is imported, duplicated entries are deleted, then residential addresses are eliminated by using a regex search that scans for strings starting with '\d+' or '\s\d+' in the OutbreakLocation column. The results are then merged into a single string with Name, Address, and County.

Then, the NLP_RiskAndInterventionShortFields file is imported, and columns matching Employer name/address and locations visited name/address are selected. After filtering these, an intermediate output file can optionally be created. Then, similar to before, residential results are filtered by using regex to match name fields starting with '\d+' or '\s\d+', and the two lists are merged into a single text file, with one line for each potential location or business address match.

## create_formattedLL.py
This python script takes one input file: the NLP_Patient text file. It requires a delimiter for the NLP file, which usually is '|'. It also requires a list of columns to select, which usually is:

`IncidentID,Address,City,Zip,County,CensusBlock,CensusTract,Latitude,Longitude`

This script creates two output files. The file ending in 'parsed.txt' will be used after the location mapping step to add Zip Code and County identifiers. The other output file starts with 'filterFile.formattedLL', and will be used in the next section to generate the input file for location mapping.

# NER Pipeline
![](https://github.com/disulfidebond/APOLLO/blob/main/media/APOLLO_DEV_README_fig1.png)

# Clustering and Mapping
>Note: This is being merged with the NER code so that running APOLLO on Silo will automatically create this file.
![](https://github.com/disulfidebond/APOLLO/blob/main/media/APOLLO_DEV_README_fig2.png)

To generate an output file for the location mapping, the python script filter_select.py needs to be run to create a list of NER term, centroid latitude, centroid longitude. This script takes as input the report from NER mapping, and the formattedLL file. It scans the report for the provided IncidenIDs, and then grabs the latitude and longitude for these from the formattedLL file. For each cluster of locations, a centroid is calculated using KMeans and then slightly obfuscated to protect privacy. This creates an internal list with the information from the report, along with a index-matched list of NER term, and coordinates.

Next, the Google Maps API is used to scan for businesses matching to the NER term within a 20 km radius of the centroid. If no results are found, then the search is repeated for 30 km. Matches for all clusters are gathered, and fuzzy string matching is used to select up to the top 3 hits to the NER term among all matches. Finally, these top 3 hits are validated against the business database text file that was created during preprocessing, and an output file is created with the business names that are near the IncidentID cluster.

# Cases Report

![](https://github.com/disulfidebond/APOLLO/blob/main/media/APOLLO_DEV_README_fig3.png)


Finally, the ZipCodes and County names from preprocessing are mapped back to the mapping results as columns, and this is merged with the NER report.
