# README
APOLLO (Automated Public Outbreak Localization through Lexical Operations) is a Natural Langauge Processing tool for detecting potential outbreaks from the Wisconsin Electronic Disease Surveillance System (WEDSS) for potential organizations and locations to assist in contact tracing efforts and reduce the burden on the health departments. The current version of our classifier is able to extract large amounts of surveillance data and summarize a report to highlight potential outbreaks and their associated addresses. The report was designed in weekly intervals by county so a systematic approach can be shared for any county in the state of Wisconsin. A [Data Dictionary](https://github.com/disulfidebond/APOLLO/blob/main/DataDictionary_DHS_NLP_2021-01-28.xlsx) of all the WEDSS fields examined for developing the pipeline is also available.

# Description of Output
The output columns are listed below, with descriptions:

* Name: The named entity extracted directly from the text fields of contact interviews, including Investigation Notes (retrieved from WEDSS). These are limited to potential organizations, locations, or miscellaneous.  The remainder of the columns in this report are related to this field.  All results are for the 'Name' moving forward.
* Type: The type of named entity, which is one of: Organization, Location, Miscellaneous.
* Iterations: The number of unique instances for the named entity within the 1-week time peroid.
* Name_Score: The average predicted probability (confidence score) from the model for the named entity (Score range 0-100, with 100 being highest confidence).
* IncidentIDs: The IncidentID's that are linked to the named entities from column 1.
* Outbreaks: Ongoing OutbreakIDs by health departments to avoid redundancy. The unique Outbreak identifiers associated with the IncidentIDs (can be one-to-one or many-to-one fuzzy match between the two).
* OutbreakIDs: numeric IDs from WEDSS for each outbreak in Outbreaks column
* OutbreakLocations: Location from WEDSS data (where it exists) for each outbreak in the Outbreaks column
* OutbreakProcessStatuses: Outbreak process status (e.g. New, Open Local Investigation, Final)  from WEDSS data for each outbreak in the Outbreaks column
* Address1: The top match for mapping an address to the name. FILTERED means no match found with confidence.
* Confidence1: The confidence score for the mapping results for the top mapping hit. A score greater than 80 is very confident, a score of 100 is a perfect match between the provided NER term and the mapping result.
* URL1: google map hyperlink for address1
* Address2: The second best mapping result, note this may be empty if only one mapping hit was found. FILTERED means no match found with confidence.
* Confidence2: The confidence score for the mapping results for the second best mapping hit. A score greater than 80 is very confident, a score of 100 is a perfect match between the provided NER term and the mapping result.
* URL2: google map hyperlink for address2
* Address3: The third best mapping result, note this may be empty. FILTERED means no match found with confidence.
* Confidence3: The confidence score for the mapping results for the second best mapping hit. A score greater than 80 is very confident, a score of 100 is a perfect match between the provided NER term and the mapping result. 
* URL3: google map hyperlink for address3
* ZipCode: The Zip Code associated with the identified Incident IDs
* County: The County associated with the identified Incident IDs

# Usage
![](https://github.com/disulfidebond/APOLLO/blob/main/media/APOLLO_README_fig.png)

To run APOLLO, provide the required input files and run `python apollo`.


# AMIA 2021 Abstract Submission
![](https://github.com/disulfidebond/APOLLO/blob/main/media/Abstract_page1.jpg)
![](https://github.com/disulfidebond/APOLLO/blob/main/media/Abstract_page2.jpg)
