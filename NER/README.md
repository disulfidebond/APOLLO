# APOLLO
## Automated Public Outbreak Localization though Lexical Operations
### NER Pipeline


in APOLLO/
pip install -e .

in project root:
python run_apollo_example.py

Given specific WEDSS extract files:
1. Identify all confirmed or probable IncidentIDs pertaining to a specified week
2. Find all relevant text for those IncidentIDs in the WEDSS contact tracing interview text
3. Extract named entities using a [BERT-base-cased model](https://huggingface.co/dslim/bert-base-NER) from [huggingface.co](https://huggingface.co/) trained on [coNLL-2003](https://www.aclweb.org/anthology/W03-0419.pdf) for NER
4. Rank all named entities associated with incidents by frequency
5. Extract all existing outbreak information from WEDSS outbreak data and process names and locations with the same NER model.
6. Match existing outbreak named entities with incidentID data derived named entities
7. Collate all similar rows by fuzzy matching on incidentID data derived named entities
8. Export CSV.

Example output:
|Named entity|Type|Iterations|Score|IncidentIDs|Outbreak Entity|
|--|--|--|--|--|--|
|Sun Prairie|Location |12|0.67|['12345','79900',...]|sun prairie||
|local retailer|Organization|7|0.54|['23245','23345'..]|local retailer|
|fast food place|Organization|3|0.45|['23456','67111']||

