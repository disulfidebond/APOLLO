# Introduction

Google's ToS forbids caching data or web scraping with the Google Maps API. It is permissible to use the Google Maps API to match or validate business locations using a pre-created or existing database (phone conversation from JP Artega 03/03/2021).

I created a small database of business names and addresses by searching publicly-available information, but this is time-consuming and inaccurate. Majid suggested using the NLP_Outbreak data forms to pull business names and addresses instead, which I'll do.

# Steps
A simple Bash command can pull out the relevant fields:

    cat NLP_Outbreak_20210409_104156.txt | cut -d\| -f4-8 > NLP_outbreak_addresses.txt
    
Followed by using python to parse and format the fields into a Pandas dataframe and CSV output:    
    
    import pandas as pd
    df = pd.read_csv('NLP_outbreak_addresses.txt', delimiter='|', encoding='latin1')
    df = df[df['OutbreakLocation]].notna()]
    df.reset_index(drop=True)
    df = df.fillna("UNKNOWN")
    df_filtered = df.loc[:,['OutbreakLocation', 'OutbreakLocationAddress', 'OutbreakLocationJurisdiction']]
    df_dedup = df_filtered.drop_duplicates(subset=['OutbreakLocation'])
    df_dedup.to_csv('NLP_outbreak_addresses.loc-add-county.csv', index=False)
    
