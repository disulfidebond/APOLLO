# README
This section describes the location mapping steps of the workflow. Briefly, the steps are parse the data for the incident IDs,

The input file is the CSV file with `Name`,`Type`,`Iterations`,`Score`,`Incidents`, and `Outbreaks` columns. An example is shown below:

| Name | Type | Iterations | Score | Incidents | Outbreaks|
|---|---|---|---|---|---|
|Sun Prairie|Location|2|0.234|['12345','23446','21212']|['OUTBREAK-1']
|Shop4Stuff|Organization|4|0.651|['22221','22223','224566']|[]

Use Bash or python to pull the latitude and longitude coordinates for each IncidentID (Bash is shown):

      # output will be column index with latitude, repeat for longitude and IncndentID
      head -n1 NLP_Patient_20210201_160117.txt | sed 's/|/\n/g' | grep -n Latitude
      # then create a parsed text file
      cat NLP_Patient_20210201_160117.txt | cut -d\| -f1,12,13 > NLP_Patient_20210201_160117_formattedLL.txt
      
Run [filter_select4Google.py](https://github.com/disulfidebond/COVID_tracking/blob/main/loc_mapping/filter_select.02192021.v2.py). This workflow will import both of the above files, then output an internal list with identifiers (this *must* stay on Silo) and an external list of NER term and Lat/Lon centroid coordinates for Google Places to scan.

Next, run [places_query.02202021.py](https://github.com/disulfidebond/COVID_tracking/blob/main/loc_mapping/places_query.02202021.py). This file requires two inputs: the external output file from filter_select4Google.py, and the business database file.

This script does the following: 
1. Import the list of places, and the business database file.
2. Run a query using the Google Places API on businesses within a 14 mile radius that match to the NER term.
3. Queries the business match terms against the business database.
* If there are no matches in the business database, then it logs this and continues
* If there are matches in the business database, and there is only one result, that result is returned
* If there are matches in the business database and there are more than one result, it returns up to three of the best matches using NER > Regex > Blind
4. The results are written to a CSV file

Finally, the CSV file results are merged to the original input CSV file for a combined report Excel Spreadsheet.
