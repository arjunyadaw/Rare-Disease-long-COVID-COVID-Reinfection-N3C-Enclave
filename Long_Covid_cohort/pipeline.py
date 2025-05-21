

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4a64c3f7-3c51-42f6-8e07-c6ebc7eb8cff"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
# Long COVID-19 diagnosis based on condition source code

from pyspark.sql import functions as F
def long_covid_based_on_U09pt9(condition_occurrence):
    df = condition_occurrence
    df = df.filter((df['condition_source_value'].contains('U09.9')) | (df['condition_source_value'].contains('B94.8')))
    #df = df.filter((df['condition_source_value'].contains('U09.9','B94.8')))
    df = df.select('person_id','condition_start_date','data_partner_id' ,'condition_source_value')
    df = df.withColumnRenamed('condition_source_value', 'long_covid_diagnosis_code')
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.955d296d-8fa4-4598-b718-08a624ac7e13"),
    observation=Input(rid="ri.foundry.main.dataset.b998b475-b229-471c-800e-9421491409f3")
)
from pyspark.sql import functions as F
def long_covid_based_on_observation(observation):
    df = observation
    df = df.filter(df['observation_concept_id'] == 2004207791)

    distinct_pid = df.select(F.countDistinct("person_id"))
    print(distinct_pid.show())
    
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.44374c7c-032d-4b6d-a808-517ce5c39e19"),
    Covid_cohort_final=Input(rid="ri.foundry.main.dataset.7eb11610-f14d-4ceb-9ed1-b0651a3ab75d"),
    long_covid_based_on_U09pt9=Input(rid="ri.foundry.main.dataset.4a64c3f7-3c51-42f6-8e07-c6ebc7eb8cff"),
    long_covid_based_on_observation=Input(rid="ri.foundry.main.dataset.955d296d-8fa4-4598-b718-08a624ac7e13")
)
# Long COVID diagnosis after 45 days of COVID-19 diagnosis date

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff
def long_covid_condition_observation_data(long_covid_based_on_U09pt9, long_covid_based_on_observation, Covid_cohort_final):
    df_cohort = Covid_cohort_final.select("person_id","COVID_first_poslab_or_diagnosis_date")

    df1 = long_covid_based_on_U09pt9
    df2 = long_covid_based_on_observation
    # Assuming you have DataFrames 'long_covid_based_on_U09pt9' and 'long_covid_based_on_observation'
    df1 = df1.select('person_id', 'condition_start_date').withColumnRenamed('condition_start_date', 'long_covid_start_date')
    df2 = df2.select('person_id', 'observation_date').withColumnRenamed('observation_date', 'long_covid_start_date')

    # Union the DataFrames
    df = df1.union(df2)

    # Sort the DataFrame by 'long_covid_start_date' in ascending order
    df = df.orderBy(col("long_covid_start_date"))

    # Drop duplicates by 'person_id'
    df3 = df.dropDuplicates(subset=['person_id'])

    df = df_cohort.join(df3, on=['person_id'], how='inner')

    df_filtered = df.filter(datediff(col("COVID_first_poslab_or_diagnosis_date"), col("long_covid_start_date")) < -90)

    return df_filtered

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8f052619-2e7f-4cac-8b28-118a7b7b334c"),
    date_model=Input(rid="ri.foundry.main.dataset.31728ad8-884c-4015-997b-f5a5f4a9ba17")
)
# Date_model table is created by machine learning model at N3C enclave and patients with model score>0.75 are selected as long COVID patients (236,626) Total patients in date_model_file

from pyspark.sql import functions as F

def long_covid_data(date_model):
    df = date_model.select('person_id','min_covid_dt')
    df = df.filter(df.min_covid_dt > '2020-01-01').toPandas()
    df.rename(columns = {'min_covid_dt': 'long_covid_start_date'},inplace = True)
    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.afbeb9be-ec92-4aec-a405-e0969ce20a22"),
    long_covid_condition_observation_data=Input(rid="ri.foundry.main.dataset.44374c7c-032d-4b6d-a808-517ce5c39e19"),
    long_covid_data=Input(rid="ri.foundry.main.dataset.8f052619-2e7f-4cac-8b28-118a7b7b334c")
)
'''
Merging model based long COVID and (diagnosis or observation) based long COVID together as final long COVID cohort after 45 days after COVID-19 diagnosis date. 
'''

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
def long_covid_table(long_covid_data, long_covid_condition_observation_data):
    # Create DataFrame df1 from long_covid_data
    df1 = long_covid_data.select('person_id', 'long_covid_start_date').withColumn('long_covid', lit(1))

    # Create DataFrame df2 from long_covid_condition_observation_data
    df2 = long_covid_condition_observation_data.select('person_id', 'long_covid_start_date').withColumn('long_covid', lit(1))
    df2 = df2.filter(col('long_covid_start_date') > '2020-04-01') # atleast 90 days after first diagnosis ('2020-04-01') data

    # Join df2 and df1 on person_id, long_covid_start_date, and long_covid
    df = df2.join(df1, ['person_id', 'long_covid_start_date', 'long_covid'], 'outer').toPandas()

    # Sort by long_covid_start_date in ascending order
    df.sort_values(by='long_covid_start_date', ascending=True, inplace=True)

    # Keep the first occurrence of each person_id
    df.drop_duplicates(subset='person_id', keep='first', inplace=True)

    return df

