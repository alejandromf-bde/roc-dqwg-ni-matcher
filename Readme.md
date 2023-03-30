
# GLEIF vs National entity identifier matcher

This software is a work that is included within the GLEIF data quality control initiative, carried out by the ROC (https://www.leiroc.org/). It performs data quality comparisons between a Golden Copy data subset and a National entity dataset.

The project belongs to its author (Alejandro Morales Fernández) and to Banco de España. You are free to distribute it under the conditions described in attached LICENSE file.
It is written in python and uses the pandas library to manage the datasets. The author is available 
via his email: alejandro.morales@bde.es

After version v2.0 there is no need of python to be executed, as the python software and libraries needed are embedded in
the executable ni-matcher.exe. However, the python code is available if the user wanted to make some changes.

It creates a report where the number of companies which match in both datasets is shown. Also, the name in both datasets 
is considered and compared, performing a cleansing in the text names and then calculating the similarity metrics. More 
information can be seen in 	
- https://en.wikipedia.org/wiki/Longest_common_substring_problem 
- https://en.wikipedia.org/wiki/Gestalt_Pattern_Matching

Some of the ideas for Entity name harmonization have been inspired from GLEIF project https://github.com/Sociovestix/lenu and others from experience in previous works from Banco de España

## Dependencies

- pandas 1.1.4
- Other libraries used, already in the python distribution:
  - difflib (used to detect differences between text strings)
  - os
  - sys
  - logging
  - argparse
  

## Usage

It needs two datasets called exactly "Gleif Golden Copy.csv.zip" and "National Dataset.csv", the first one contains the GLEIF information, 
and the second one the national data

- Gleif Golden Copy.csv.zip, which should be the gleif golden copy downloaded from https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/
 
- National dataset, containing the following 
columns:
  - id1: national identifier 1
  -
    .
    .
    .
    
  - idn: national identifier n
  - Entity name: National entity name
  - LEI (optional)

Aditionally, it requires the following meta-datasets, completely available in the
GLEIF Website and API:


- lou_attributes.csv, containing all the LOU information, can be downloaded from the API: https://api.gleif.org/api/v1/lei-issuers


- ra_list_v1.7.xlsx, it contains all the Registration Authority metadata, can be downloaded from https://www.gleif.org/en/about-lei/code-lists/gleif-registration-authorities-list

- elf-code-list-v1.4.1.xlsx Entity Legal Form code list, downloaded from https://www.gleif.org/en/about-lei/code-lists/iso-20275-entity-legal-forms-code-list

### Parameters



```
Mandatory:
  - Country Code: Example ES for Spain, JP for Japan

  - Number of national identifiers (id1, id2, etc.), default = 3

Optionals:
  - Encoding: encoding of the national dataset: default utf_8_sig

```

Example of execution without python (easy way):

The user only has to double click the executable ni-matcher.exe and write the parameters. Alternatively, he can write the 
following in a windows console and execute the program with the parameters at the same time:
```
ni-matcher.exe ES 3
```

Example of execution with python:
```
python National Identifier matcher.py ES 3
```

Example of execution with python adding encoding mbcs:
```
python National Identifier matcher.py ES 3 -e mbcs
```

### Version 2.1 (10 August 2022)

This update allows not case sensitive matches between identifiers. Also, it removes the zeros and spaces in the left of the ids.

### Version: 2.2 (29 August 2022) 
Fixes a miscalculation in column Partial Name Coincidence NOT Case Sensitive (>80  %) and solves a problem of matching that arose when a numeric identifier was in the dataset present.


### Version: 2.3 (16 September 2022) 
Creates the full merged table from both datasets, instead of the worst 50 examples in the last sheets. 
Fixes some small errors regarding acronym transformations obtained from the ELF code list.

## Output

The code creates a directory called output_data and stores the final reports in that folder. Example for Spain: DQWG_ES_ISSUED.xlsx and DQWG_ES_LAPSED.xlsx
