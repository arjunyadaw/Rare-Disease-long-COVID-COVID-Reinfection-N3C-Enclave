

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1810fdc9-b10e-4867-97cf-b1fd4032e4f1"),
    Bebtelovimab_data=Input(rid="ri.foundry.main.dataset.728c5752-e7bf-4e36-a8e5-bf6dfad92daf")
)
# Keeping first exposure of drug by odering in ascending order and dropping duplicates

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
def Bebtelovimab_data_unique(Bebtelovimab_data):
    df = Bebtelovimab_data
    # define the window Specification
    window_spec = Window.partitionBy('person_id').orderBy('drug_exposure_start_date')
    # Assign row numbers based on drug_exposure_start_date within each person_id partition
    df_ranked = df.withColumn("RowNum", F.row_number().over(window_spec))
    #Filter rows where RowNum is 1 to keep the first occurance based on drug_exposure_start_date
    result_df = df_ranked.filter("RowNum = 1").drop("RowNum")
    return result_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f317c7cc-476b-441a-80b1-9fee2c84c4c9"),
    Bebtelovimab_data_unique=Input(rid="ri.foundry.main.dataset.1810fdc9-b10e-4867-97cf-b1fd4032e4f1"),
    Covid_cohort_final=Input(rid="ri.foundry.main.dataset.7eb11610-f14d-4ceb-9ed1-b0651a3ab75d")
)
# Drug treatment started within 7 days to COVID-19 diagnosis date

import pandas as pd
import numpy as np
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit

def COVID_Bebtelovimab_data_within_index_date(Bebtelovimab_data_unique, Covid_cohort_final):
    df2 = Covid_cohort_final.select('person_id','data_partner_id','COVID_first_poslab_or_diagnosis_date')
    df1 = Bebtelovimab_data_unique.select('person_id','drug_exposure_start_date','Bebtelovimab')
    df = df1.select("person_id", "drug_exposure_start_date","Bebtelovimab").join(df2,'person_id','inner')
    # Patients considered if antiviral drugs administered within 7 days to index date (within 3days early treatment)
    df = df.withColumn('Bebtelovimab_within_cutoff_date', F.when((F.datediff('COVID_first_poslab_or_diagnosis_date','drug_exposure_start_date')>= -7) & (F.datediff('COVID_first_poslab_or_diagnosis_date','drug_exposure_start_date')<=0), 1).otherwise(0))  
    df = df.dropDuplicates(['person_id'])
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b9edbbf7-87c8-42b6-8a05-c8ce592b7ef4"),
    Covid_cohort_final=Input(rid="ri.foundry.main.dataset.7eb11610-f14d-4ceb-9ed1-b0651a3ab75d"),
    Molnupiravir_data_unique=Input(rid="ri.foundry.main.dataset.e79776e1-d6b6-406b-b770-18a4bd391786")
)
# Drug treatment started within 5 days to COVID-19 diagnosis date

import pandas as pd
import numpy as np
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit

def COVID_Molnupiravir_data_within_index_date(Molnupiravir_data_unique, Covid_cohort_final):
    df2 = Covid_cohort_final.select('person_id','data_partner_id','COVID_first_poslab_or_diagnosis_date')
    df1 = Molnupiravir_data_unique.select('person_id','drug_exposure_start_date','Molnupiravir')
    df = df1.select("person_id", "drug_exposure_start_date","Molnupiravir").join(df2,'person_id','inner')
    # Patients considered if antiviral drugs administered within 5 days to index date 
    df = df.withColumn('Molnupiravir_within_cutoff_date', F.when((F.datediff('COVID_first_poslab_or_diagnosis_date','drug_exposure_start_date')>= -5) & (F.datediff('COVID_first_poslab_or_diagnosis_date','drug_exposure_start_date')<=0), 1).otherwise(0))  
    df = df.dropDuplicates(['person_id'])
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e3a8b7b4-8506-4ffa-b155-d4c524ab5b57"),
    Covid_cohort_final=Input(rid="ri.foundry.main.dataset.7eb11610-f14d-4ceb-9ed1-b0651a3ab75d"),
    Paxlovid_data_unique=Input(rid="ri.foundry.main.dataset.263fd7f0-871e-4349-af67-060be5012709")
)
# Drug treatment started within 5 days to COVID-19 diagnosis date

import pandas as pd
import numpy as np
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit

def COVID_Paxlovid_data_within_index_date(Paxlovid_data_unique, Covid_cohort_final):
    df2 = Covid_cohort_final.select('person_id','data_partner_id','COVID_first_poslab_or_diagnosis_date')
    df1 = Paxlovid_data_unique.select('person_id','drug_exposure_start_date','Paxlovid')
    df = df1.select("person_id", "drug_exposure_start_date","Paxlovid").join(df2,'person_id','inner')
    # Patients considered if antiviral drugs administered within 5 days to index date 
    df = df.withColumn('Paxlovid_within_cutoff_date', F.when((F.datediff('COVID_first_poslab_or_diagnosis_date','drug_exposure_start_date')>= -5) & (F.datediff('COVID_first_poslab_or_diagnosis_date','drug_exposure_start_date')<=0), 1).otherwise(0))  
    df = df.dropDuplicates(['person_id'])
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e79776e1-d6b6-406b-b770-18a4bd391786"),
    Molnupiravir_data=Input(rid="ri.foundry.main.dataset.a73b02f7-99bf-4ba4-bb90-08b83ed53faa")
)
# Keeping first exposure of drug by odering in ascending order and dropping duplicates

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
def Molnupiravir_data_unique(Molnupiravir_data):
    df = Molnupiravir_data

    # define the window Specification
    window_spec = Window.partitionBy('person_id').orderBy('drug_exposure_start_date')

    # Assign row numbers based on drug_exposure_start_date within each person_id partition
    df_ranked = df.withColumn("RowNum", F.row_number().over(window_spec))

    #Filter rows where RowNum is 1 to keep the first occurance based on drug_exposure_start_date

    result_df = df_ranked.filter("RowNum = 1").drop("RowNum")

    return result_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.263fd7f0-871e-4349-af67-060be5012709"),
    Paxlovid_data=Input(rid="ri.foundry.main.dataset.25bf6f1f-8a7b-497e-9bb4-c25d0d82f28e")
)

# Keeping first exposure of drug by odering in ascending order and dropping duplicates

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def Paxlovid_data_unique(Paxlovid_data):
    df = Paxlovid_data
    # define the window Specification
    window_spec = Window.partitionBy('person_id').orderBy('drug_exposure_start_date')
    # Assign row numbers based on drug_exposure_start_date within each person_id partition
    df_ranked = df.withColumn("RowNum", F.row_number().over(window_spec))
    #Filter rows where RowNum is 1 to keep the first occurance based on drug_exposure_start_date
    result_df = df_ranked.filter("RowNum = 1").drop("RowNum")
    return result_df
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9b49d62c-05a8-4bfc-8e62-cca5c09b6291"),
    COVID_Bebtelovimab_data_within_index_date=Input(rid="ri.foundry.main.dataset.f317c7cc-476b-441a-80b1-9fee2c84c4c9"),
    COVID_Molnupiravir_data_within_index_date=Input(rid="ri.foundry.main.dataset.b9edbbf7-87c8-42b6-8a05-c8ce592b7ef4"),
    COVID_Paxlovid_data_within_index_date=Input(rid="ri.foundry.main.dataset.e3a8b7b4-8506-4ffa-b155-d4c524ab5b57")
)
"""
Cohort of drug treatment (Paxlovid or Molnupiravir or Bebtelovimab) if either of drug treatment administered within grace period then it considered as antiviral treatment.
"""

import pandas as pd
import numpy as np
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit

def covid_drug_treatment( COVID_Paxlovid_data_within_index_date, COVID_Molnupiravir_data_within_index_date, COVID_Bebtelovimab_data_within_index_date):

    df1 = COVID_Paxlovid_data_within_index_date.select('person_id','drug_exposure_start_date','Paxlovid_within_cutoff_date').where(F.col('Paxlovid_within_cutoff_date') ==1)
    print('Paxlovid usage #', df1.count())
    df2 = COVID_Molnupiravir_data_within_index_date.select('person_id','drug_exposure_start_date','Molnupiravir_within_cutoff_date').where(F.col('Molnupiravir_within_cutoff_date') ==1)
    print('Molnupiravir usage #', df2.count())
    df3 = COVID_Bebtelovimab_data_within_index_date.select('person_id','drug_exposure_start_date','Bebtelovimab_within_cutoff_date').where(F.col('Bebtelovimab_within_cutoff_date') ==1)
    print('Bebtelovimab usage #', df3.count())
    # Renaming columns name for further usage
    df1 = df1.withColumnRenamed('Paxlovid_within_cutoff_date', 'Paxlovid')
    df2 = df2.withColumnRenamed('Molnupiravir_within_cutoff_date', 'Molnupiravir')
    df3 = df3.withColumnRenamed('Bebtelovimab_within_cutoff_date', 'Bebtelovimab')
    dataframes = [df1,df2,df3]
    def merge_dataframes(df1,df2):
        return df1.join(df2, on = ['person_id','drug_exposure_start_date'], how = 'outer')
    # Use reduce to apply the merge function across all DataFrames in the list
    merged_dataframe = reduce(merge_dataframes, dataframes)
    # Fill null values with 0
    final_dataframe = merged_dataframe.fillna(0)
    # Create new column of antiviral treatment  
    final_dataframe = final_dataframe.withColumn("selected_antiviral_treatment", lit(1))
    return final_dataframe

"""
def covid_drug_treatment(COVID_Remdesivir_data_within_index_date, COVID_Paxlovid_data_within_index_date, COVID_Molnupiravir_data_within_index_date, COVID_Evushield_data_within_index_date, COVID_Bebtelovimab_data_within_index_date):
    df1 = COVID_Remdesivir_data_within_index_date.select('person_id','drug_exposure_start_date','Remdesivir_within_cutoff_date').where(F.col('Remdesivir_within_cutoff_date') ==1)
    print('Remdesivir usage #', df1.count())
    df2 = COVID_Paxlovid_data_within_index_date.select('person_id','drug_exposure_start_date','Paxlovid_within_cutoff_date').where(F.col('Paxlovid_within_cutoff_date') ==1)
    print('Paxlovid usage #', df2.count())
    df3 = COVID_Molnupiravir_data_within_index_date.select('person_id','drug_exposure_start_date','Molnupiravir_within_cutoff_date').where(F.col('Molnupiravir_within_cutoff_date') ==1)
    print('Molnupiravir usage #', df3.count())
    df4 = COVID_Evushield_data_within_index_date.select('person_id','drug_exposure_start_date','Evushield_within_cutoff_date').where(F.col('Evushield_within_cutoff_date') ==1)
    print('Evushield usage #', df4.count())
    df5 = COVID_Bebtelovimab_data_within_index_date.select('person_id','drug_exposure_start_date','Bebtelovimab_within_cutoff_date').where(F.col('Bebtelovimab_within_cutoff_date') ==1)
    print('Bebtelovimab usage #', df5.count())
    # Renaming columns name for further usage
    df1 = df1.withColumnRenamed('Remdesivir_within_cutoff_date', 'Remdesivir')
    df2 = df2.withColumnRenamed('Paxlovid_within_cutoff_date', 'Paxlovid')
    df3 = df3.withColumnRenamed('Molnupiravir_within_cutoff_date', 'Molnupiravir')
    df4 = df4.withColumnRenamed('Evushield_within_cutoff_date', 'Evushield')
    df5 = df5.withColumnRenamed('Bebtelovimab_within_cutoff_date', 'Bebtelovimab')
    dataframes = [df1,df2,df3,df4,df5]
    def merge_dataframes(df1,df2):
        return df1.join(df2, on = ['person_id','drug_exposure_start_date'], how = 'outer')
    # Use reduce to apply the merge function across all DataFrames in the list
    merged_dataframe = reduce(merge_dataframes, dataframes)
    # Fill null values with 0
    final_dataframe = merged_dataframe.fillna(0)
    # Create new column of antiviral treatment  
    final_dataframe = final_dataframe.withColumn("selected_antiviral_treatment", lit(1))
    return final_dataframe
"""

