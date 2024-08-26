
n_examples = 800000

description_cells = [
'File and Tables Description',
 """
This Excel file with the tables is the result of processing the Pyhton Code for the "Pairing LEI - National Identifier" project.
When the Python Code is processed, 2 Excel files are automatically generated. The names of the files depend on the Jurisdiction and the Registration Status, each one covers the scope of ISSUED Entities or LAPSED Entities, according to the file name. Example of file names generated for the Spanish Jurisdiction are: "DQWG_ES_ISSUED" and "DQWG_ES_LAPSED".

These file have 9 variable sheets which contain the following information:
- This first sheet is an explantion of the whole file describing the content of each sheet.
- The rest of the sheets contain one table each, created after the 2 datasets: the Golden Copy for the selected jurisdiction and the national dataset with the selected identifiers.
The name of each sheet indicates briefly the name of the table that it contains.
 """,

 """0. Report Description""","""
This sheet is an introduction to the file and a detailed description of the tables contained in each sheet.
 """
,
"""1. Number of LEIs by Status""","""
This table indicates the number of total LEIs in the Golden Copy, detailing the total LEIs for each of the Entity Status (ACTIVE/INACTIVE) and the different Registration Status.
"""
,
"""2. Entities in National Dataset""","""
This table indicates the total entities in the national dataset, the number of entities which have LEI in that dataset and the number of entities which have each of the selected national identifiers in the dataset.
"""
,

"""3. Location Information""","""
This table shows a summary of the three main variables for location: Headquarters Country, Legal Jursidiction (truncated the first 2 caracters) and Legal Address Country
The variable which country is selected in this work is by Legal Jursidiction (truncated the first 2 caracters)
"""
,
"""4. RA Summary""","""
This table shows the identifiers in GLEIF Golden Copy by RA: Number of LEIs, Number of RAEntityIDs, Number of VAEntityIDs, Number of OtherVAEntityIDs.
"""
,
"""5. LOU Summary""","""
This table shows the identifiers in GLEIF Golden Copy by LOU: Number of LEIs, Number of RAEntityIDs, Number of VAEntityIDs, Number of OtherVAEntityIDs. The last two columns show the figures of the differences between  Ras, Vas and OtherVAs
"""
 ,
"""6A. Cross National vs GLEIF""","""
This table is the result of a cross check of national dataset with Golden Copy for companies with Entity Status as ACTIVE and Registration Status as ISSUED or LAPSED (depending on the file name).
The datasets are crossed without any transformation, this is, the identifiers and the names are crossed exactly as they appear in each dataset.

The following lines summarise the content of each cell of the table, indicating first the row and then the columns:
- Row 2 (National id 1 in EntityID fields):
          - Columns B, C and D (GLEIF Golden Copy RAEntityID/ VAEntityID/ OtherVAEntityID): These cells indicate how many of the entities in the national dataset with ""Identifier 1"" match EXACTLY the identifier contained in the fields RAEntityID, VAEntityID and OtherVAEntityID of the GC, respectively.
          - Column E (GLEIF Golden Copy Any of the EntityID fields): This cell indicates the number of entities in the national dataset with ""Identifier 1"" that match EXACTLY any of the identifiers contained in any of the three ""EntityID"" fields mentioned before.
          - Column F (GLEIF Golden Copy % Any of the EntityID fields): This cell indicates the percent of entities in the national dataset with ""Identifier 1"" that match any of the identifiers in any of the three ""EntityID"" fields mentioned before, over the total ISSUED (or LAPSED depending on the case) entities in the GC.
          - Column G (Exact Name Coincidence (100% exact characters case sensitive)): This cell indicates the number of entities that having previously matched  ""Identifier 1"" with any of the identifiers in the three ""EntityID"" fields, also match EXACTLY, including matching capital letters and lower cases, the ""Entity.LegalName"" in the GC with the ""Entity Name"" in the national dataset.
          - Column H (Exact Name Coincidence (100% exact characters case sensitive)): This cell indicates the number of entities that having previously matched  ""Identifier 1"" with any of the identifiers in the three ""EntityID"" fields, also match EXACTLY but without being sensitive to capital letters and lower cases, the ""Entity.LegalName"" in the GC with the ""Entity Name"" in the national dataset.
           - Column I (Partial Name Coincidence NOT Case Sensitive (>80%)): This cell indicates the number of entities that having previously matched  ""Identifier 1"" with any of the identifiers in the three ""EntityID"" fields, don´t match exactly the ""Entity.LegalName"" in the GC with the ""Entity Name"" in the national dataset, but have a name similarity in more than 80% (without being sensitive to capital letters and lower cases), according to the Python metrics of similarity.
           - Column J (Average Name Similarity Metric (NOT Case Sensitive)): This cell indicates the average of the results to the Python metrics in name similarity for each entity, when matching  ""Entity.LegalName"" in the GC with the ""Entity Name"" in the national dataset, for the entities that had previously matched  ""Identifier 1"" with any of the identifiers in the three ""EntityID"" fields.

- Rows 3 and 4 (National id 2/3 in EntityID fields):
          - Columns B-J: These cells indicate the same as indicated for Row 2, 3 and 4, for the National Identifiers 2 and 3.

- Row 5 (Any of selected national identifiers in EntityID fields):
          - Columns B, C and D (GLEIF Golden Copy RAEntityID/ VAEntityID/ OtherVAEntityID):  These cells indicate how many of the identifiers contained in the fields RAEntityID, VAEntityID and OtherVAEntityID of the GC, respectively, match ANY of the National Identifiers in the national dataset.
          - Column E (GLEIF Golden Copy Any of the EntityID fields): This cell indicates indicates how many of the identifiers contained in ANY of the fields RAEntityID, VAEntityID and OtherVAEntityID of the GC match ANY of the National Identifiers in the national dataset.
          - Columns F-J: These cells indicate the same as for Rows 2, 3 and 4, for the identifiers contained in ANY of the fields RAEntityID, VAEntityID and OtherVAEntityID of the GC that match ANY of the National Identifiers in the national dataset.

- Row 6 (Total LEIs in EntityID fields):
           - Columns B-E: These cells indicate the total number of entities that have some kind of identifier in the corresponding ""EntityID"" field or in any ""EntityID"" field of the GC, respectively.
           - Column F: This cell indicates the percent of entities that have some kind of identifier in any of the corresponding ""EntityID"" field of the GC over the total number of ISSUED/ LAPSED entities in the GC.

- Row 7 (Other identifiers in EntityID fields): This row indicates the total number of entities that have some different identifier from the national identifiers in the national dataset,  in the ""EntityID"" fields of the GC, respectively.
- Row 8 (Empty field as EntityID): This row indicates the total number of entities that have no data in the ""EntityID"" fields of the GC, respectively.
"""
,

"""6B. Cross check transformation""","""
This table is exactly the same as Table 6A, but previous to the cross checks, both datasets go through a transformation of identifiers and names in order to be able to detect entities with same identifiers or names that don´t match due to punctuation marks, special characters, spaces, or other minor differences. 
This transformations consists in:
- For Identifiers: Spaces, puntuation marks, special characters, etc. are erased.
- For Entity Names: Spaces, puntuation marks, special characters, etc. are erased.  Entity Legal Form´s abbreviations are equaled to the complete Entity Legal Form Name (Treatment inspired in Legal Form´s Proyect from GLEIF) 
"""
,

"""6C. Cross check Summary""","""
This table is a summary of Table 6B
"""
,
"""7. Cross check By RAs""","""
This table shows the result of cross checking the identifiers in both datasets, with and without transformation, grouped by Registration Authorities. Each file show a different RA.
- Columns E, G and I: This columns shows the number of entities in the national dataset whose Identifier 1, 2 or 3 match EXACTLY, without transformation, to any of the identifiers contained in any of the three ""EntityID"" fields of the GC.
- Columns F, H and J: This columns shows the number of entities in the national dataset whose Identifier 1, 2 or 3, once having been TRANSFORMED,  matches any of the identifiers contained in any of the three ""EntityID"" fields of the GC after having also been TRANSFORMED.
"""

 ,
 """8. Cross check By LOUs""","""
 This table indicates the same information as table 4, grouped by LEI Issuers (LOUs) instead of RAs.
 """
  ,
"""9. matched entities sorted""","""
This table shows all the entities whose identifiers have matched in both datasets AFTER TRANSFORMATION, but the names in both datasets don´t match even AFTER TRANSFORMATION.
- Columns B, C and D (RAEntityID, VAEntityID and OtherEntitiyID) indicate the identifiers from the ""EntityID"" fields that has matched to the national identifiers.
- Columns E, F and G (Id2, Id2 and Id3) indicate the national identifier that have matched the ""EntityID"" field.
- Columns H and I show the original name of the entity in both datasets without transformation.
- Column J shows the  the % of similarity between the names of the entity in both datasets, according to the Python metrics in name similarity,
""",
"""10. RA Summaries""","""
In this case, there is a table for each Registration Authority. The aim of these tables is to show which RAs generate each indicator, according to the character length
"""



 ]