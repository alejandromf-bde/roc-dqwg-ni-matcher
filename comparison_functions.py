


from difflib import SequenceMatcher
import pandas as pd




def diff_string(row_text, str1, str2):
    """
    :param row_text: dict-type input, containg str1 and str2
    :return: ratio or metric of similarity between strings 1 and 2
    """
    return round(SequenceMatcher(None, row_text[str1], row_text[str2]).ratio() * 100, 1)


# Formatting and writting to excel sheet

def format_header(worksheet, df, header_format, index = True):
    if index:
        column_values = [''] + list(df.columns)
    else:
        column_values = list(df.columns)
    for col_num, value in enumerate(column_values):
        worksheet.write(0, col_num , value, header_format)
        worksheet.set_column(0, 0, 40)
    worksheet.set_row(0, 60)


class table_calculator:
    """
    Contains functions for the creation of the tables in the report
    """

    def __init__(self,lei_present, entityIds, ids, n_ids, lou_id, ra_id, national_entity_name, gleif_entity_name):
        self.lei_present = lei_present
        self.entityIds = entityIds
        self.ids = ids
        self.n_ids = n_ids
        self.lou_id = lou_id
        self.ra_id = ra_id
        self.gleif_entity_name = gleif_entity_name
        self.national_entity_name = national_entity_name


    def calculate_matches(self, lei_gc_df, national_dataset_df):
        """
        Calculates the matches of identifiers in different columns
        :param lei_gc_df: gleif golden copy file
        :param national_dataset_df: national dataset file
        :return: a dataframe with all the possible combination of matches between national identifiers and Gleif identifiers
        """

        # merge by LEI additionally if it is present in the National Dataset


        if not self.lei_present:

            dfs2concat = []
            for id in self.ids:
                for entityId in self.entityIds:

                    dfs2concat.append(pd.merge(lei_gc_df[['LEI', entityId, self.gleif_entity_name, self.lou_id, self.ra_id]].dropna(subset=[entityId]) ,national_dataset_df[[id, self.national_entity_name]].dropna(subset=[id]), how='inner', left_on = [entityId], right_on = [id]))

        else:

            ## Entity match


            dfs2concat = []
            for id in self.ids:
                for entityId in self.entityIds:
                    dfs2concat.append(pd.merge(lei_gc_df[['LEI', entityId, self.gleif_entity_name, self.lou_id, self.ra_id]].dropna(subset=[entityId]) ,national_dataset_df[['LEI', id, self.national_entity_name]].dropna(subset=[id]), how='inner', left_on = ['LEI', entityId], right_on = ['LEI', id]))

        # saves the match in a dataframe, keeping all the possible fields that matched
        return pd.concat(dfs2concat).drop_duplicates()

    def calculate_cross_table(self, df_trace, lei_gc_df, threshold_partial_name):
        """
        caclulate tables 3A and 3B (cross gleif ids with national ids)
        :param df_trace: dataframe input
        :return: a dataframe ready to be written in the excel output
        """


        dict_cross_ids = {} # will feed the output dataframe in the end of the function
        index_id = []

        total_leis = lei_gc_df.shape[0]

        for id in self.ids:
            index_id.append(f"National {id} in EntityID fields")
            LEIS_anyEntityIDs = []
            for entityId in self.entityIds:
                crossed_df = df_trace[df_trace[entityId]==df_trace[id]]
                try:
                    dict_cross_ids['GLEIF Golden Copy ' + entityId].append(crossed_df.shape[0])
                except KeyError:
                    dict_cross_ids['GLEIF Golden Copy ' + entityId] = [crossed_df.shape[0]]
                LEIS_anyEntityIDs += list(crossed_df['LEI'])

            LEIS_anyEntityIDs = list(set(LEIS_anyEntityIDs))
            leis_id_num = len(LEIS_anyEntityIDs)
            any_entity_df = df_trace[df_trace['LEI'].isin(LEIS_anyEntityIDs)]
            any_entity_df = any_entity_df[[self.national_entity_name, self.gleif_entity_name, 'names_similarity_metric', 'names_similarity_metric_no_cs']].drop_duplicates()
            exact_coincidence_num_cs = any_entity_df[any_entity_df['names_similarity_metric'] == 100].shape[0]
            exact_coincidence_num = any_entity_df[any_entity_df['names_similarity_metric_no_cs'] == 100].shape[0]
            partial_coincidence_num = any_entity_df[any_entity_df['names_similarity_metric_no_cs'] >= threshold_partial_name * 100].shape[0]
            average_similary_metric = round(any_entity_df['names_similarity_metric_no_cs'].agg(['average'])[0], 1)
            try:
                # this try except is to avoid error when the first element of the column is appended
                dict_cross_ids['GLEIF Golden Copy Any of the EntityID fields'].append(leis_id_num)
                dict_cross_ids['GLEIF Golden Copy % Any of the EntityID fields'].append(round(100 * leis_id_num/total_leis, 1))
                dict_cross_ids[f'Exact Name Coincidence (100% exact characters case sensitive)'].append(exact_coincidence_num_cs)
                dict_cross_ids[f'Exact Name Coincidence (100% exact characters NOT case sensitive)'].append(exact_coincidence_num)
                dict_cross_ids[f'Partial Name Coincidence NOT Case Sensitive (>{int(threshold_partial_name* 100)}  %)'].append(partial_coincidence_num)
                dict_cross_ids['Average Name Similarity Metric (NOT Case Sensitive)'].append(average_similary_metric)
            except KeyError:
                dict_cross_ids['GLEIF Golden Copy Any of the EntityID fields'] = [leis_id_num]
                dict_cross_ids['GLEIF Golden Copy % Any of the EntityID fields'] = [round(100 * leis_id_num/total_leis, 1)]
                dict_cross_ids[f'Exact Name Coincidence (100% exact characters case sensitive)'] = [exact_coincidence_num_cs]
                dict_cross_ids[f'Exact Name Coincidence (100% exact characters NOT case sensitive)'] = [exact_coincidence_num]
                dict_cross_ids[f'Partial Name Coincidence NOT Case Sensitive (>{int(threshold_partial_name* 100)}  %)'] = [partial_coincidence_num]
                dict_cross_ids['Average Name Similarity Metric (NOT Case Sensitive)'] = [average_similary_metric]

        index_id.append('Any of selected national identifiers in EntityID fields')
        LEIS_anyIDs_anyEntityIDs = list(df_trace['LEI'])
        for entityId in self.entityIds:
            LEIS_anyIDs = []
            for id in self.ids:
                crossed_df = df_trace[df_trace[entityId]==df_trace[id]]
                LEIS_anyIDs += list(crossed_df['LEI'])

            LEIS_anyIDs = list(set(LEIS_anyIDs))
            dict_cross_ids['GLEIF Golden Copy ' + entityId].append(len(LEIS_anyIDs))

        leis_id_num = len(set(LEIS_anyIDs_anyEntityIDs))
        dict_cross_ids['GLEIF Golden Copy Any of the EntityID fields'].append(leis_id_num)
        dict_cross_ids['GLEIF Golden Copy % Any of the EntityID fields'].append(round(100 * leis_id_num/total_leis, 1))

        any_entity_df = df_trace[df_trace['LEI'].isin(LEIS_anyIDs_anyEntityIDs)]
        any_entity_df = any_entity_df[
            [self.national_entity_name, self.gleif_entity_name, 'names_similarity_metric', 'names_similarity_metric_no_cs']].drop_duplicates()
        exact_coincidence_num_cs = any_entity_df[any_entity_df['names_similarity_metric'] == 100].shape[0]
        exact_coincidence_num = any_entity_df[any_entity_df['names_similarity_metric_no_cs'] == 100].shape[0]
        partial_coincidence_num = any_entity_df[any_entity_df['names_similarity_metric_no_cs'] >= threshold_partial_name * 100].shape[0]
        average_similary_metric = any_entity_df['names_similarity_metric_no_cs'].agg(['average'])[0]
        dict_cross_ids[f'Exact Name Coincidence (100% exact characters case sensitive)'].append(exact_coincidence_num_cs)
        dict_cross_ids[f'Exact Name Coincidence (100% exact characters NOT case sensitive)'].append(exact_coincidence_num)
        dict_cross_ids[f'Partial Name Coincidence NOT Case Sensitive (>{int(threshold_partial_name* 100)}  %)'].append(partial_coincidence_num)
        dict_cross_ids['Average Name Similarity Metric (NOT Case Sensitive)'].append(average_similary_metric)

        index_id.append('Total LEIs in EntityID fields')

        LEIS_entityIDs = []
        for entityId in self.entityIds:
            dict_cross_ids['GLEIF Golden Copy ' + entityId].append(lei_gc_df[entityId].count())
            LEIS_entityIDs += list(lei_gc_df[lei_gc_df[entityId].notnull()]['LEI'])

        leis_id_num = len(set(LEIS_entityIDs))
        dict_cross_ids['GLEIF Golden Copy Any of the EntityID fields'].append(leis_id_num)
        dict_cross_ids['GLEIF Golden Copy % Any of the EntityID fields'].append(round(100 * leis_id_num/total_leis, 1))
        dict_cross_ids[f'Exact Name Coincidence (100% exact characters case sensitive)'].append(exact_coincidence_num_cs)
        dict_cross_ids[f'Exact Name Coincidence (100% exact characters NOT case sensitive)'].append(exact_coincidence_num)
        dict_cross_ids[f'Partial Name Coincidence NOT Case Sensitive (>{int(threshold_partial_name* 100)}  %)'].append(partial_coincidence_num)
        dict_cross_ids['Average Name Similarity Metric (NOT Case Sensitive)'].append(average_similary_metric)

        index_id.append('Other identifiers in EntityID fields')

        for i, key in enumerate(dict_cross_ids.keys()):
            # loop to calculate row "Other identifiers in EntityID fields"
            if i < self.n_ids:
                dict_cross_ids[key].append(dict_cross_ids[key][-1] - dict_cross_ids[key][-2])
            else:
                dict_cross_ids[key].append('-')

        index_id.append('Empty field as EntityID')

        for i, key in enumerate(dict_cross_ids.keys()):
            # loop to calculate row "Empty field as EntityID"
            if i < self.n_ids:
                dict_cross_ids[key].append(lei_gc_df.shape[0] - dict_cross_ids[key][-2])
            else:
                dict_cross_ids[key].append('-')

        df = pd.DataFrame(dict_cross_ids, index = index_id)
        numeric_columns = df.select_dtypes(include=['number']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        return df


    def calculate_tables_lou_ra(self, df_trace, df_trace_h, lei_gc_df, lou_ra, lou_name_country_df, ra_name_country_df):
        """
        calculate tables 4 and 5

        :param df_trace: cross check with raw identifiers
        :param df_trace_h: cross check with harmonized identifiers
        :param lou_ra: group by lou or by ra
        :return: a dataframe ready to be written in the excel output
        """

        dict_cross_ids = {}  # will feed the output dataframe in the end of the function
        any_transformation = [] # it is neccesary to keep the LEIs to calculate the last columns without duplicates

        # First of all, it is neccesary to calculate the total LEIs by RA or LOU before counting ids individually:

        if lou_ra == self.lou_id:
            # some distinctions between RA and LOU apply, as the metadata is different
            summary_lei_ra = lei_gc_df[['LEI', lou_ra]].\
                groupby([lou_ra]).agg(['count'])
            summary_lei_ra.columns = summary_lei_ra.columns.droplevel(1)
            summary_lei_ra.reset_index(inplace=True)

            summary_lei_ra = summary_lei_ra.merge(lou_name_country_df[['lei', 'name']], left_on = lou_ra, right_on = 'lei', how = 'left').\
                sort_values(['LEI', lou_ra], ascending = [False, True])


            dict_cross_ids['Managing LOU (LEI)'] = list(summary_lei_ra[lou_ra])
            dict_cross_ids['Managing LOU (Name)'] = list(summary_lei_ra['name'])
            dict_cross_ids['Number of LEIs'] = list(summary_lei_ra['LEI'])


        else:
            summary_lei_ra = lei_gc_df[['LEI', lou_ra]].\
                groupby([lou_ra]).agg(['count'])
            summary_lei_ra.columns = summary_lei_ra.columns.droplevel(1)
            summary_lei_ra.reset_index(inplace = True)

            summary_lei_ra = summary_lei_ra.merge(ra_name_country_df[['Registration Authority Code', 'Local name of Register', 'International name of organisation responsible for the Register']], left_on = lou_ra, right_on = 'Registration Authority Code', how = 'left').\
                sort_values(['LEI', lou_ra], ascending = [False, True])

            dict_cross_ids['Registration Authority Code'] = list(summary_lei_ra[lou_ra])
            dict_cross_ids['Local name of Register'] = list(summary_lei_ra['Local name of Register'])
            dict_cross_ids['International name of organisation responsible for the Register'] = list(summary_lei_ra['International name of organisation responsible for the Register'])
            dict_cross_ids['Number of LEIs'] = list(summary_lei_ra['LEI'])

        # Count id columns

        for id in self.ids:
            summary_id_ra = df_trace.dropna(subset=[id])[['LEI', lou_ra]].drop_duplicates().\
                groupby([lou_ra]).agg(['count'])
            summary_id_ra.columns = summary_id_ra.columns.droplevel(1)
            summary_id_ra.reset_index(inplace = True)
            if  lou_ra == self.lou_id:
                summary_id_ra = summary_id_ra.rename(columns = {'LEI': 'LEI_with_id'}).merge(summary_lei_ra[['LEI', lou_ra]], left_on = lou_ra, right_on = lou_ra, how = 'right').\
                    sort_values(['LEI', lou_ra], ascending = [False, True])
            else:
                summary_id_ra = summary_id_ra.rename(columns = {'LEI': 'LEI_with_id'}).merge(summary_lei_ra[['LEI', lou_ra]], left_on = lou_ra, right_on = lou_ra, how = 'right').\
                    sort_values(['LEI', lou_ra], ascending = [False, True])
            dict_cross_ids[f'Any EntityID matching {id}'] = list(summary_id_ra['LEI_with_id'])
            summary_id_ra = df_trace_h.dropna(subset=[id])[['LEI', lou_ra]].drop_duplicates().\
                groupby([lou_ra]).agg(['count'])
            summary_id_ra.columns = summary_id_ra.columns.droplevel(1)
            summary_id_ra.reset_index(inplace = True)
            any_transformation.append(df_trace_h[['LEI', id, lou_ra]].rename(columns = {id:'id'}))
            if  lou_ra == self.lou_id:
                summary_id_ra = summary_id_ra.rename(columns = {'LEI': 'LEI_with_id'}).merge(summary_lei_ra[['LEI', lou_ra]], left_on = lou_ra, right_on = lou_ra, how = 'right').\
                    sort_values(['LEI', lou_ra], ascending = [False, True])
            else:
                summary_id_ra = summary_id_ra.rename(columns = {'LEI': 'LEI_with_id'}).merge(summary_lei_ra[['LEI', lou_ra]], left_on = lou_ra, right_on = lou_ra, how = 'right').\
                    sort_values(['LEI', lou_ra], ascending = [False, True])

            dict_cross_ids[f'Any EntityID matching transformed {id}'] = list(summary_id_ra['LEI_with_id'])


        # Last columns require previous counts

        any_transformation_df = pd.concat(any_transformation)

        summary_ra = any_transformation_df.dropna(subset=['id'])[['LEI', lou_ra]].drop_duplicates(). \
            groupby([lou_ra]).agg(['count'])
        summary_ra.columns = summary_ra.columns.droplevel(1)
        summary_ra.reset_index(inplace = True)
        if lou_ra == self.lou_id:
            summary_ra = summary_ra.rename(columns = {'LEI': 'LEI_with_id'}).merge(summary_lei_ra[['LEI', lou_ra]], left_on=lou_ra, right_on=lou_ra,
                                                how='right'). \
                sort_values(['LEI', lou_ra], ascending=[False, True])
        else:
            summary_ra = summary_ra.rename(columns = {'LEI': 'LEI_with_id'}).merge(summary_lei_ra[['LEI', lou_ra]], left_on=lou_ra,
                                                right_on=lou_ra, how='right'). \
                sort_values(['LEI', lou_ra], ascending=[False, True])

        dict_cross_ids['Any EntityID matching any of selected national identifiers after Id transformation'] = list(summary_ra['LEI_with_id'])

        dict_cross_ids['% Any EntityIDs matching any of selected national identifiers after Id transformation'] = list(round(100 * summary_ra['LEI_with_id']/summary_ra['LEI'], 1))

        df = pd.DataFrame(dict_cross_ids)
        numeric_columns = df.select_dtypes(include=['number']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)

        numeric_columns = df.select_dtypes(include=['number']).columns.to_list()

        df = df.append(df.sum(numeric_only=True), ignore_index=True)

        df.iloc[-1, 0] = 'TOTAL'

        if lou_ra == self.ra_id:
            df[['Local name of Register', 'International name of organisation responsible for the Register']] = df[['Local name of Register',
                                                                                                                    'International name of organisation responsible for the Register']].fillna('-')
        else:
            df[['Managing LOU (LEI)']] = df[['Managing LOU (LEI)']].fillna('-')

        df.loc[:, ~df.columns.isin(numeric_columns)] = df.loc[:, ~df.columns.isin(numeric_columns)].fillna('')


        df.iloc[-1, -1] = round(
            100 * df['Any EntityID matching any of selected national identifiers after Id transformation'].iloc[-1] /
            df['Number of LEIs'].iloc[-1], 1)

        return df