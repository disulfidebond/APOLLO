# README
APOLLO (Automated Public Outbreak Localization through Lexical Operations) is a Natural Langauge Processing tool for detecting potential outbreaks from the Wisconsin Electronic Disease Surveillance System (WEDSS) for potential organizations and locations to assist in contact tracing efforts and reduce the burden on the health departments. The current version of our classifier is able to extract large amounts of surveillance data and summarize a report to highlight potential outbreaks and their associated addresses. The report was designed in weekly intervals by county so a systematic approach can be shared for any county in the state of Wisconsin.

# Description of Output
The output columns are listed below, with descriptions:

* Name: The name identified from data entered into WEDSS
* Type: The type of NER term, which is one of: Organization, Location, Miscellaneous.
* Iterations: How many iterations the NER workflow used.
* NER_Score: This score describes how confident the NER pipeline is in identifying a location or business.
* Incidents: The IncidentID's that are linked to the NER term in column 1.
* Outbreaks: The Outbreak identifiers associated with the IncidentIDs.
* Mapping1: The top hit for mapping.
* Mapping1_Confidence: The confidence score for the mapping results for the top mapping hit. A score greater than 80 is very confident, a score of 100 is a perfect match between the provided NER term and the mapping result.
* Mapping2: The second best mapping result, note this may be empty if only one mapping hit was found.
* Mapping2_Confidence: The confidence score for the mapping results for the second best mapping hit. A score greater than 80 is very confident, a score of 100 is a perfect match between the provided NER term and the mapping result.
* Mapping3: The third best mapping result, note this may be empty. 
* Mapping3_Confidence: The confidence score for the mapping results for the second best mapping hit. A score greater than 80 is very confident, a score of 100 is a perfect match between the provided NER term and the mapping result. 

# Usage
![](https://github.com/disulfidebond/APOLLO/blob/main/media/APOLLO_README_fig.png)

To run APOLLO, provide the required input files and run `python apollo`.
