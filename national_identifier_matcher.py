#!/usr/bin/env python
# coding: utf-8
# @author: Alejandro Morales Fernández. Banco de España.


## Merge both GLEIF and National datasets and creates the report in Excel.
## Example of use for Spain and three identifiers: python National Identfier matcher.py ES 3


__author__ = "Alejandro Morales Fernández"
__version__ = "2.0"

import os
import sys
import pandas as pd
import argparse
from string_transformations import harmonize, purge
from description_text_sheet import description_cells, n_examples
from comparison_functions import format_header, diff_string, table_calculator
from pandas.api.types import is_numeric_dtype


import warnings
warnings.simplefilter("ignore")


import logging
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)


## Default Params ##

threshold_partial_name = 0.8 # sets the similarity threshold to decide if two entities are the same according to their names

pd.set_option('display.max_columns', None)


def main():

    
    ## Params read ##
    ## First receive the params via code. Example: python National Identifier matcher.py ES 3 -e mbcs
    ## If it fails, it can be executed via input reading
    
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("country_code", help="Introduce your country code (example: ES for Spain)",
                            type=str)
        parser.add_argument("n_ids", help="Introduce the number of national identifiers to be used",
                            type=int)
        parser.add_argument('-e', '--encoding', nargs='?', const='utf_8_sig', default = 'utf_8_sig',
                            help='Introduce your encoding (example: utf_8_sig for japan, mbcs for Spain)', type = str)
        args = parser.parse_args()
    
        country_code = args.country_code
        n_ids = args.n_ids
        encoding = args.encoding
    
    
    except:
        sys.stdout.write('Introduce country code (example: ES for Spain)')
        country_code = input('$: ')
        sys.stdout.write('Introduce the number of national identifiers to be used')
        n_ids = int(input('$: '))
        sys.stdout.write('Introduce the encoding of the national dataset (default utf_8_sig)')
        encoding = input('$: ')
        if encoding == '':
            encoding = 'utf_8_sig'
    
    
    registrationStatus_list = ['ISSUED', 'LAPSED']
    
    exec_dir = os.getcwd()
    output_dir = os.path.join(exec_dir, 'output_data')
    
    if not os.path.exists(output_dir):
        # creates the output directory
        os.makedirs(output_dir)
    
    
    
    # Declare the ids that are going to be used from the golden copy, and the ones from the national dataset
    columns_from_golden_copy = ['LEI',
                                'Entity.LegalName',
                                'Entity.RegistrationAuthority.RegistrationAuthorityID',
                                'Entity.RegistrationAuthority.RegistrationAuthorityEntityID',
                                'Entity.LegalJurisdiction',
                                'Registration.RegistrationStatus',
                                'Registration.ManagingLOU',
                                'Registration.ValidationAuthority.ValidationAuthorityEntityID',
                                'Registration.OtherValidationAuthorities.OtherValidationAuthority.1.ValidationAuthorityEntityID',
                                'Entity.EntityStatus'
                                ]
    entityIds_original = ['Entity.RegistrationAuthority.RegistrationAuthorityEntityID',
                 'Registration.ValidationAuthority.ValidationAuthorityEntityID',
                 'Registration.OtherValidationAuthorities.OtherValidationAuthority.1.ValidationAuthorityEntityID']
    
    entityIds = ['RAEntityID',
                 'VAEntityID',
                 'OtherVAEntityID']
    
    ra_id = 'Entity.RegistrationAuthority.RegistrationAuthorityID'
    lou_id = 'Registration.ManagingLOU'
    
    # The ids must be 'id1', 'id2', etc.
    ids = ['id'+str(i+1) for i in range(n_ids)]
    # The following variables can be modified to take into account addresses instead of names. Or could be a concatenation of both, etc.
    national_entity_name = 'Entity Name'
    gleif_entity_name = 'Entity.LegalName'
    national_entity_address = 'Address'
    gleif_entity_address = 'Entity.LegalAddress.FirstAddressLine'
    gleif_legaljurisdiction = 'Entity.LegalJurisdiction'
    entityStatus_allowed = ['ACTIVE']
    
    
    
    ## Input reading ##
    # 2 data files are required: "Gleif Golden Copy.csv.zip" and "National Dataset.csv.zip"
    logging.info("Loading Gleif  Golden Copy")
    
    data_file_name = "Gleif Golden Copy.csv.zip"
    data_path = os.path.join(exec_dir, data_file_name).replace("\\", "/")
    lei_gc_original_df = pd.read_csv(data_path, sep=",", low_memory = False, encoding = 'utf_8_sig')
    
    logging.info(f"Loading Country {country_code} from  Gleif  Golden Copy")
    
    
    ## Gleif golden copy data extraction ##
    lei_gc_original_df = lei_gc_original_df[columns_from_golden_copy]
    lei_gc_original_df = lei_gc_original_df[lei_gc_original_df['Entity.LegalJurisdiction'].str.slice(start=0, stop=2) == country_code]
    lei_gc_original_df = lei_gc_original_df.rename(columns = dict(zip(entityIds_original, entityIds)))
    
    
    
    logging.info("Loading Metadata")
    
    # 3 metadata files are required:
    # "lou_attributes.csv", extracted from https://api.gleif.org/api/v1/lei-issuers
    # "2022-03-23_ra_list_v1.7.xlsx", extracted from https://www.gleif.org/en/about-lei/code-lists/gleif-registration-authorities-list#
    # elf-code-list-v1.4.1.xlsx, extracted from https://www.gleif.org/en/about-lei/code-lists/iso-20275-entity-legal-forms-code-list
    
    data_file_name = "lou_attributes.csv"
    data_path = os.path.join(exec_dir, data_file_name).replace("\\", "/")
    lou_name_country_df = pd.read_csv(data_path, sep=";", encoding = 'utf_8_sig')
    
    data_file_name = "ra_list_v1.7.xlsx"
    data_path = os.path.join(exec_dir, data_file_name).replace("\\", "/")
    ra_name_country_df = pd.read_excel(data_path)
    
    # Read and clean the elf code dataset
    
    data_file_name = "elf-code-list-v1.4.1.xlsx"
    data_path = os.path.join(exec_dir, data_file_name).replace("\\", "/")
    elf_code_df = pd.read_excel(data_path)
    elf_code_df = elf_code_df[elf_code_df['Country Code \n(ISO 3166-1)'] == country_code]
    elf_code_df = elf_code_df[['Entity Legal Form name Local name', 'Abbreviations Local language']]
    elf_code_df['Abbreviations Local language'] = elf_code_df['Abbreviations Local language'].str.upper().str.split(';')
    elf_code_df['Entity Legal Form name Local name'] = elf_code_df['Entity Legal Form name Local name'].str.upper()
    elf_code_df = elf_code_df.explode('Abbreviations Local language')
    elf_code_df['Abbreviations Local language'] = elf_code_df['Abbreviations Local language'].apply(purge)
    elf_code_df['Entity Legal Form name Local name'] = elf_code_df['Entity Legal Form name Local name'].apply(purge)
    elf_code_df = elf_code_df.dropna()
    # transform abbreviations table to dataframe to apply to entity names
    legal_form_abb_dict = dict(zip(elf_code_df['Entity Legal Form name Local name'].to_list(),elf_code_df['Abbreviations Local language'].to_list()))
    
    
    active_inactive = lei_gc_original_df[['LEI', 'Entity.EntityStatus', 'Registration.RegistrationStatus']].groupby([ 'Entity.EntityStatus', 'Registration.RegistrationStatus']).agg(['nunique'])
    active_inactive.columns = active_inactive.columns.droplevel(1)
    active_inactive = active_inactive.sort_values(['Entity.EntityStatus', 'LEI'], ascending = [True, False])
    active_inactive.reset_index(inplace = True)
    
    # Calculating totals
    total_active = active_inactive[active_inactive['Entity.EntityStatus']=='ACTIVE']['LEI'].sum()
    total_inactive = active_inactive[active_inactive['Entity.EntityStatus']=='INACTIVE']['LEI'].sum()
    active_inactive = active_inactive.append({'Entity.EntityStatus': 'TOTAL ACTIVE', 'Registration.RegistrationStatus': '', 'LEI': total_active}, ignore_index=True)
    active_inactive = active_inactive.append({'Entity.EntityStatus': 'TOTAL INACTIVE', 'Registration.RegistrationStatus': '', 'LEI': total_inactive}, ignore_index=True)
    active_inactive = active_inactive.append({'Entity.EntityStatus': 'TOTAL', 'Registration.RegistrationStatus': '', 'LEI': total_active + total_inactive}, ignore_index=True)
    logging.info("Loading National Dataset")
    
    
    data_file_name = "National Dataset.csv"
    data_path = os.path.join(exec_dir, data_file_name)
    national_dataset_original_df = pd.read_csv(data_path, sep=";", encoding=encoding, error_bad_lines=False)
    
    # check if LEI column is present in the National Dataset. If it is found,
    # a LEI match is also performed, along the others quality controls
    lei_present = True if 'LEI' in national_dataset_original_df.columns else False
    
    for registrationStatus_allowed in registrationStatus_list:
        logging.info(f"Processing Analysis for {registrationStatus_allowed} companies")
    
        # The report is going to be stored in a excel file
        logging.info("Creating Report")
        writer = pd.ExcelWriter(os.path.join(output_dir, f"DQWG_{country_code}_{registrationStatus_allowed}.xlsx"), engine='xlsxwriter')
        workbook = writer.book
    
        # Excel formatting
    
        header_format = workbook.add_format(
            {'bold': True,
             'text_wrap': True,
             'font_size': 9,
             'valign': 'top',
             'fg_color': '#AEDB20',
             'center_across': True,
             'border': 1}
        )
        header_format.set_align('vcenter')
    
        logging.info("Creating sheet 0")
    
        workbook = writer.book
        worksheet = workbook.add_worksheet('0. Report Description')
    
        for row_num, value in enumerate(description_cells):
            # if row_num in [2,4,6,8,10,12,14]:
            #     cells = worksheet.getCells()
            if row_num in [0]:
                cell_format = workbook.add_format(
                    {'bold': True,
                     'font_size': 15,
                     'text_wrap': True,
                     'valign': 'top',
                     'fg_color': '#EC9E5A',
                     'border': 1}
                )
                cell_format.set_align('vcenter')
                cell_format.set_text_wrap()
            elif row_num in [2, 4, 6, 8, 10, 12, 14, 16]:
                cell_format = workbook.add_format(
                    {'bold': True,
                     'font_size': 13,
                     'text_wrap': True,
                     'valign': 'top',
                     'fg_color': '#F5C088',
                     'border': 1}
                )
                cell_format.set_align('vcenter')
                cell_format.set_text_wrap()
            else:
                cell_format = workbook.add_format(
                    {'bold': False,
                     'text_wrap': True,
                     'valign': 'top',
                     'fg_color': '#FFFFFF',
                     'border': 1}
                )
                cell_format.set_align('vcenter')
            worksheet.write(row_num, 0, value, cell_format)
    
        worksheet.set_column(0, 0, 200)
    
        logging.info("Creating Table 1")
    
    
        active_inactive.to_excel(writer, sheet_name= '1. Number of LEIs by Status', index=False)
    
    
        worksheet = writer.sheets['1. Number of LEIs by Status']
        format_header(worksheet, active_inactive, header_format, index = False)
    
        # Select the subset of the GLEIF Golden copy to use according to Entity and Registration Status
        lei_gc_df = lei_gc_original_df[(lei_gc_original_df['Entity.EntityStatus'].isin(entityStatus_allowed)) &
                                       (lei_gc_original_df['Registration.RegistrationStatus']==registrationStatus_allowed)]
        
        
    
    
        logging.info("Creating Table 2")
    
        index_table_2 = ['Entities with LEIs'] + ['Entities with ' + id for id in ids] +  ['Total Entities in National Dataset']
        table_2_dict = {}
    
        if lei_present:
            table_2_dict['Number of Entities'] = [national_dataset_original_df['LEI'].nunique()]
        else:
            table_2_dict['Number of Entities'] = [0]
    
        for id in ids:
             table_2_dict['Number of Entities'].append(national_dataset_original_df[id].nunique())
    
        table_2_dict['Number of Entities'].append(national_dataset_original_df.shape[0])
        table_2 = pd.DataFrame(table_2_dict, index = index_table_2).fillna(0)
        table_2.index.name = 'Entities in National Dataset'
    
    
        table_2.to_excel(writer, sheet_name= '2. Entities in National Dataset')
        worksheet = writer.sheets['2. Entities in National Dataset']
        format_header(worksheet, table_2, header_format, index = True)
        worksheet.write(0, 0, 'Entities in National Dataset', header_format)
    
    
        # Ensure the fields to compare are string type
    
    
        national_dataset_df = national_dataset_original_df[:]
    
        for entityId in entityIds:
            if is_numeric_dtype(lei_gc_df[entityId]):
                lei_gc_df = lei_gc_df.astype({entityId: 'Int64'})
                
                
            lei_gc_df = lei_gc_df.astype({entityId: 'string'})
    
        for id in ids:
            if is_numeric_dtype(national_dataset_df[id]):
                national_dataset_df = national_dataset_df.astype({id: 'Int64'})
                
            national_dataset_df = national_dataset_df.astype({id: 'string'})
    
        # Initialize the class table_calculator
    
        table_calculator_instance = table_calculator(lei_present, entityIds, ids, n_ids, lou_id, ra_id, national_entity_name, gleif_entity_name)
    
    
        ## LEI -- Entity match
        ## df_trace is the main table used in this program. It contains all the possible matches between ids and LEIs
    
    
        df_trace = table_calculator_instance.calculate_matches(lei_gc_df, national_dataset_df)
        if df_trace.shape[0] == 0:
            logging.error(f"The datasets do not match in any id for {registrationStatus_allowed}")
            raise AssertionError(f"No matching for {registrationStatus_allowed}")
    
        df_trace[gleif_entity_name + '_no_cs'] = df_trace[gleif_entity_name].apply(lambda x: x.upper())
        df_trace[national_entity_name + '_no_cs'] = df_trace[national_entity_name].apply(lambda x: x.upper())
    
        df_trace['names_similarity_metric'] = df_trace.apply(lambda x: diff_string(x, national_entity_name, gleif_entity_name), axis = 1)
        df_trace['names_similarity_metric_no_cs'] = df_trace.apply(lambda x: diff_string(x, national_entity_name + '_no_cs', gleif_entity_name + '_no_cs'), axis = 1)
    
    
        cross_ids_df = table_calculator_instance.calculate_cross_table(df_trace, lei_gc_df, threshold_partial_name)
    
        logging.info("Creating Table 3A")
    
        cross_ids_df.to_excel(writer, sheet_name= '3A. Cross National vs GLEIF')
        worksheet = writer.sheets['3A. Cross National vs GLEIF']
        format_header(worksheet, cross_ids_df, header_format, index = True)
    
        # purge the ids
    
        for id in ids:
            national_dataset_df[id] = national_dataset_df[id].str.upper().str.strip().str.lstrip('0')
            national_dataset_df[id] = national_dataset_df[id].apply(purge)
        for entityId in entityIds:
            lei_gc_df[entityId] = lei_gc_df[entityId].str.upper().str.strip().str.lstrip('0')
            lei_gc_df[entityId] = lei_gc_df[entityId].apply(purge)
    
        # df_trace_h = df_trace harmonized. It is similar to df_trace, but the matches have been done with cleansed ids
    
        df_trace_h = table_calculator_instance.calculate_matches(lei_gc_df, national_dataset_df)
    
        df_trace_h[gleif_entity_name + '_original'] = df_trace_h[gleif_entity_name]
        df_trace_h[gleif_entity_name] = df_trace_h[gleif_entity_name].apply(lambda x: harmonize(x, legal_form_abb_dict))
        df_trace_h[national_entity_name + '_original'] = df_trace_h[national_entity_name]
        df_trace_h[national_entity_name] = df_trace_h[national_entity_name].apply(lambda x: harmonize(x, legal_form_abb_dict))
        df_trace_h[gleif_entity_name + '_no_cs'] = df_trace_h[gleif_entity_name].apply(lambda x: x.upper())
        df_trace_h[national_entity_name + '_no_cs'] = df_trace_h[national_entity_name].apply(lambda x: x.upper())
    
    
        df_trace_h['names_similarity_metric'] = df_trace_h.apply(
            lambda x: diff_string(x, national_entity_name, gleif_entity_name), axis=1)
    
    
        df_trace_h['names_similarity_metric_no_cs'] = df_trace_h.apply(
            lambda x: diff_string(x, national_entity_name + '_no_cs', gleif_entity_name + '_no_cs'), axis=1)
    
        df_trace_h_example = df_trace_h.sort_values(['names_similarity_metric_no_cs'], ascending = True).head(n_examples)
    
        columns_example = ['LEI'] + entityIds + ids + [gleif_entity_name+'_original', national_entity_name + '_original', gleif_entity_name, national_entity_name, 
        'names_similarity_metric_no_cs','Entity.RegistrationAuthority.RegistrationAuthorityID', 'Registration.ManagingLOU']
    
        df_trace_h_example = df_trace_h_example[columns_example].rename(columns = {'names_similarity_metric_no_cs':'Average Name Similarity Metric (NOT Case Sensitive)'})
        
        df_trace_h_example = df_trace_h_example.merge(lou_name_country_df[['lei', 'name']], left_on = ['Registration.ManagingLOU'], right_on = ['lei'], how = 'left')
        
        df_trace_h_example = df_trace_h_example.merge(ra_name_country_df[['Registration Authority Code', 'Local name of Register', 'International name of organisation responsible for the Register']], 
        left_on = ['Entity.RegistrationAuthority.RegistrationAuthorityID'], right_on = ['Registration Authority Code'], how = 'left')
        
        df_trace_h_example = df_trace_h_example.drop(columns = ['lei', 'Entity.RegistrationAuthority.RegistrationAuthorityID'])
        df_trace_h_example = df_trace_h_example.rename(columns = {'name':'ManagingLOU Name'})
    
        logging.info("Creating Table 3B")
    
        cross_ids_harmonized_df = table_calculator_instance.calculate_cross_table(df_trace_h, lei_gc_df, threshold_partial_name)  # cross_ids_harmonized_df
    
        cross_ids_harmonized_df.to_excel(writer, sheet_name= '3B. Cross check transformation')
        worksheet = writer.sheets['3B. Cross check transformation']
        format_header(worksheet, cross_ids_harmonized_df, header_format, index = True)
    
    
    
        ra_table = table_calculator_instance.calculate_tables_lou_ra(df_trace, df_trace_h, lei_gc_df, ra_id, lou_name_country_df, ra_name_country_df)
    
        logging.info("Creating Table 4")
    
        ra_table.to_excel(writer, sheet_name= '4. By RAs', index = False)
        worksheet = writer.sheets['4. By RAs']
        format_header(worksheet, ra_table, header_format, index = False)
    
        lou_table = table_calculator_instance.calculate_tables_lou_ra(df_trace, df_trace_h, lei_gc_df, lou_id, lou_name_country_df, ra_name_country_df)
    
    
        logging.info("Creating Table 5")
    
        lou_table.to_excel(writer, sheet_name= '5. By LOUs', index = False)
        worksheet = writer.sheets['5. By LOUs']
        format_header(worksheet, lou_table, header_format, index = False)
    
    
        logging.info("Creating Table 6")
    
        df_trace_h_example.to_excel(writer, sheet_name= f'6. Matched entities sorted', index = False)
        worksheet = writer.sheets[f'6. Matched entities sorted']
        format_header(worksheet, df_trace_h_example, header_format, index = False)
    
        writer.save()


if __name__ == "__main__":
    main()