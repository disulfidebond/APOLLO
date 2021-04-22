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

