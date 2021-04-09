# Overview
DHS has asked us to [retrieve](https://github.com/disulfidebond/APOLLO/blob/main/loc_mapping/pull_business_addresses.md) the Zip Code and County name for the entries. Unfortunately, the addresses pulled from the NLP_Outbreak file did not contain zip codes.

This was solved by generating a [text file](https://github.com/disulfidebond/APOLLO/blob/main/loc_mapping/zipCodeList.WI.csv) of ZipCode,City,County values, which can then be used with the mapping workflow to add the Zip Code.
