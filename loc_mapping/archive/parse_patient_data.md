# README
This README details the steps required to extract addresses from the NLP files.

## Extract Field names and indices from Data Dictionary Excel Spreadsheet
First, manually by scan for rows that matched 'nlp' or 'nlp+' in the Data Dictionary Excel spreadsheet into filenames with the prefix `data_fields`, for example, `data_fields.NLP_ShortFields.parsed.txt`

Then the column headers for each NLP file were extracted (example filename is used):

`head -n1 NLP_RiskAndInterventionShortFields_20210318_181730.txt | sed 's/|/\n/g' > NLP_RiskAndInterventionShortFields_20210318_181730.headers.txt`

Next, bash was used to extract the column indices:

      ARR=($(<data_fields.NLP_ShortFields.parsed.txt))
      for i in "${ARR[@]}" ; do
        grep -n $i NLP_RiskAndInterventionShortFields_20210318_181730.headers.txt | sed 's/:.*//g' >> NLP_ShortFields.colnums.txt
        cat NLP_ShortFields.colnums.txt | sort -n > NLP_ShortFields.colnums.sorted.txt
      done

## Parse Selected columns from Data Files
This step was done using python. The 'solumns.sorted.txt' files from above serve as a list of column indices, which are then selected from the NLP patient data files:

      import pandas as pd
      import numpy as np
      
      df_indices = []
      with open('NLP_ShortFields.colnums.sorted.txt', 'r') as fOpen:
        for i in fOpen:
          i = i.rstrip('\r\n')
          df_indices.append(i)
      df_data = pd.read_csv('NLP_RiskAndInterventionShortFields_20210318_181730.txt', 
                            sep='|', 
                            encoding='iso-8859-1', 
                            error_bad_lines=False, 
                            warn_bad_lines=False)
      df_parsed = df_data.loc[:,df_indices]
      df_parsed.to_csv("NLP_RiskAndInterventionShortFields_20210318_181730.addressFields.csv")

## Create Merged column of all address data
Note that this step can be combined with the previous one

      import pandas as pd
      df_data = pd.read_csv("NLP_RiskAndInterventionShortFields_20210318_181730.addressFields.csv")
      df_data["Merged"] = df_data.iloc[:,1:].apply(lambda x: ','.join(x.dropna().astype(str)), axis=1)
      df_output = df_data.loc[:,["IncidentID", "Merged"]]
      df_output.to_csv("NLP_RiskAndInterventionShortFields_20210318_181730.addressFields-merged.csv")

