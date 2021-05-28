# APOLLO - Automated Public Outbreak Localization though Lexical Operations
## NER Pipeline

### Install Pipeline Code:
in APOLLO/
```bash
pip install -e .
```

Run NER pipeline:
in project root:
```bash
bash run_apollo_example.sh
```

for help:
```bash
python run_apollo_example.py --help
```

help output:
```bash
here
```

### NER Pipeline Output
Given specific WEDSS extract files:
1. Identify all confirmed or probable IncidentIDs pertaining to a specified week
2. Find all relevant text for those IncidentIDs in the WEDSS contact tracing interview text (current feed includes over 25 character string fields concatenated together.  Please refer to data dictionary for more details).
3. Extract named entities using a [BERT-base-cased model](https://huggingface.co/dslim/bert-base-NER) from [huggingface.co](https://huggingface.co/) trained on [coNLL-2003](https://www.aclweb.org/anthology/W03-0419.pdf) for NER
4. Rank all named entities associated with incidents by frequency
5. Extract all existing outbreak information from WEDSS outbreak data and process names and locations with the same NER model.
6. Match existing outbreak named entities with incidentID named entitities
7. Collate all similar rows by fuzzy matching on incidentID data derived named entities
8. Export CSV.

Example output:
|Named entity|Type|Iterations|Score|IncidentIDs|Outbreak Name|Outbreak ID|Outbreak Location|Outbreak Process Status
|--|--|--|--|--|--|--|--|--|
|Sun Prairie|Location |12|0.67|['12345','79900',...]|2020_DANE_SUN_PRARIE|9784563|Street Address|Final
|local retailer|Organization|7|0.54|['23245','23345'..]|2020_DANE_LOCAL_RETAILER|9563784|Street Address| Local Investigation In Progess|
|fast food place|Organization|3|0.45|['23456','67111']||
