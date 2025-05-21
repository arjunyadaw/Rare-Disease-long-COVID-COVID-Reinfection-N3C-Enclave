

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ed69046c-c721-4711-a24d-9b9bc5decd01"),
    Covid_cohort_final=Input(rid="ri.foundry.main.dataset.7eb11610-f14d-4ceb-9ed1-b0651a3ab75d"),
    first_rare_abdominal_surgical_diseases=Input(rid="ri.foundry.main.dataset.f335e77b-939d-4a2d-9e9e-0b6e992ce41c"),
    first_rare_bone_diseases=Input(rid="ri.foundry.main.dataset.ec2a51eb-d475-4a32-88b3-c90dc5272338"),
    first_rare_cardiac_diseases=Input(rid="ri.foundry.main.dataset.e176007b-bfb2-41c1-a515-c479e5865b11"),
    first_rare_circulatory_system_disease=Input(rid="ri.foundry.main.dataset.f7a18651-258a-453f-8043-d37ddebc643d"),
    first_rare_developmental_defect_during_embryogenesis=Input(rid="ri.foundry.main.dataset.e319061d-cc2b-4765-bffb-b97215109992"),
    first_rare_disorder_due_to_toxic_effects=Input(rid="ri.foundry.main.dataset.821c4d56-2c25-45ad-b12e-01749aa35ab1"),
    first_rare_endocrine_disease=Input(rid="ri.foundry.main.dataset.2fa3cfc4-93da-4963-9f02-b82eb9de0248"),
    first_rare_gastroenterologic_disease=Input(rid="ri.foundry.main.dataset.b46758c1-6a47-4284-93cb-ab7e294fae16"),
    first_rare_gynecologic_or_obstetric_disease=Input(rid="ri.foundry.main.dataset.ff9f5083-f63b-4a37-a3c9-911ca48a84e3"),
    first_rare_hematologic_disease=Input(rid="ri.foundry.main.dataset.2584c685-f673-4bf3-a59e-ca2f86a1c69f"),
    first_rare_hepatic_disease=Input(rid="ri.foundry.main.dataset.94b89e17-5c0e-4d84-9951-506480c7a1ef"),
    first_rare_immune_disease=Input(rid="ri.foundry.main.dataset.01e7f690-6ecd-4a6e-ace8-dd122fd66cdf"),
    first_rare_inborn_errors_of_metabolism=Input(rid="ri.foundry.main.dataset.312b0bcb-5d57-4a85-87f0-787aa456c97b"),
    first_rare_infectious_disease=Input(rid="ri.foundry.main.dataset.227bb4b8-5d68-451d-add3-2e0654d0969b"),
    first_rare_maxillo_facial_surgical_disease=Input(rid="ri.foundry.main.dataset.e4d04326-8a21-49f9-93f7-7ebeea30a1ef"),
    first_rare_neoplastic_disease=Input(rid="ri.foundry.main.dataset.63f1b24e-dfa6-4371-93d6-0d9efd8fb798"),
    first_rare_neurologic_disease=Input(rid="ri.foundry.main.dataset.0b1b0842-8924-4496-a605-0af5b5fb85bd"),
    first_rare_odontologic_disease=Input(rid="ri.foundry.main.dataset.1874259d-80a3-467e-be3e-e1e69cc42662"),
    first_rare_ophthalmic_disorder=Input(rid="ri.foundry.main.dataset.989a5311-6f9a-409b-af2e-2216e04d02d0"),
    first_rare_otorhinolaryngologic_disease=Input(rid="ri.foundry.main.dataset.c79a45d1-2fc7-4480-8855-4ada182a1e88"),
    first_rare_renal_disease=Input(rid="ri.foundry.main.dataset.bd8232fb-d88f-4143-9b46-66ae90c38a44"),
    first_rare_respiratory_disease=Input(rid="ri.foundry.main.dataset.8d067240-1fd0-40a7-885d-4579f1a68b54"),
    first_rare_surgical_thoracic_disease=Input(rid="ri.foundry.main.dataset.48bd8e4c-4d0e-4540-b2d0-7ac4f026d5c4"),
    first_rare_systemic_or_rheumatologic_disease=Input(rid="ri.foundry.main.dataset.69a15fb8-31bd-4905-84b3-9a8b8b5f4332"),
    first_rare_transplantation_disease_data_icd10=Input(rid="ri.foundry.main.dataset.b7fa9033-a53c-4fd6-8caa-8cc614354c07"),
    first_rare_urogenital_disease_data_icd10=Input(rid="ri.foundry.main.dataset.b31d5643-c5ac-4d8c-8dc1-6709830a911d"),
    rare_skin_disease_new=Input(rid="ri.foundry.main.dataset.80b4d22a-13b6-49b0-885a-954c076f3163")
)

"""
Based on rare disease domain expert(Eric & Dominique) suggestion, We have excluded rare disease class 'rare disorder due to toxic effets' from our analysis.
In following cohort (rare_disorder_due_to_toxic_effects == 0) which will exclude patients from all cohorts.  
"""
from pyspark.sql.functions import col
from pyspark.sql import functions as F

def RD_class_with_covid(
    first_rare_abdominal_surgical_diseases,
    first_rare_bone_diseases,
    first_rare_cardiac_diseases,
    first_rare_circulatory_system_disease,
    first_rare_developmental_defect_during_embryogenesis,
    first_rare_endocrine_disease,
    first_rare_gastroenterologic_disease,
    first_rare_gynecologic_or_obstetric_disease,
    first_rare_hematologic_disease,
    first_rare_hepatic_disease,
    first_rare_immune_disease,
    first_rare_inborn_errors_of_metabolism,
    first_rare_infectious_disease,
    first_rare_maxillo_facial_surgical_disease,
    first_rare_neoplastic_disease,
    first_rare_neurologic_disease,
    first_rare_odontologic_disease,
    first_rare_ophthalmic_disorder,
    first_rare_otorhinolaryngologic_disease,
    first_rare_renal_disease,
    first_rare_respiratory_disease,
    rare_skin_disease_new,
    first_rare_surgical_thoracic_disease,
    first_rare_systemic_or_rheumatologic_disease,
    first_rare_transplantation_disease_data_icd10,
    first_rare_urogenital_disease_data_icd10,
    Covid_cohort_final,
    first_rare_disorder_due_to_toxic_effects
):
    df = Covid_cohort_final

    # List of individual rare disease DataFrames with renamed columns
    rare_disease_dfs = [
        first_rare_abdominal_surgical_diseases.select('person_id','rare_abdominal_surgical_diseases'),
        first_rare_bone_diseases.select('person_id','rare_bone_diseases'),
        first_rare_cardiac_diseases.select('person_id','rare_cardiac_diseases'),
        first_rare_circulatory_system_disease.select('person_id','rare_circulatory_system_disease'),
        first_rare_developmental_defect_during_embryogenesis.select('person_id','rare_developmental_defect_during_embryogenesis'),
        first_rare_disorder_due_to_toxic_effects.select('person_id','rare_disorder_due_to_toxic_effects'),
        first_rare_endocrine_disease.select('person_id','rare_endocrine_disease'),
        first_rare_gastroenterologic_disease.select('person_id','rare_gastroenterologic_disease'),
        first_rare_gynecologic_or_obstetric_disease.select('person_id','rare_gynecologic_or_obstetric_disease'),
        first_rare_hematologic_disease.select('person_id','rare_hematologic_disease'),
        first_rare_hepatic_disease.select('person_id','rare_hepatic_disease'),
        first_rare_immune_disease.select('person_id','rare_immune_disease'),
        first_rare_inborn_errors_of_metabolism.select('person_id','rare_inborn_errors_of_metabolism'),
        first_rare_infectious_disease.select('person_id','rare_infectious_disease'),
        first_rare_maxillo_facial_surgical_disease.select('person_id','rare_maxillo_facial_surgical_disease'),
        first_rare_neoplastic_disease.select('person_id','rare_neoplastic_disease'),
        first_rare_neurologic_disease.select('person_id','rare_neurologic_disease'),
        first_rare_odontologic_disease.select('person_id','rare_odontologic_disease'),
        first_rare_ophthalmic_disorder.select('person_id','rare_ophthalmic_disorder'),
        first_rare_otorhinolaryngologic_disease.select('person_id','rare_otorhinolaryngologic_disease'),
        first_rare_renal_disease.select('person_id','rare_renal_disease'),
        first_rare_respiratory_disease.select('person_id','rare_respiratory_disease'),
        rare_skin_disease_new.select('person_id','RSD_without_pff'),
        first_rare_surgical_thoracic_disease.select('person_id','rare_surgical_thoracic_disease'),
        first_rare_systemic_or_rheumatologic_disease.select('person_id','rare_systemic_or_rheumatologic_disease'),
        first_rare_transplantation_disease_data_icd10.select('person_id','rare_transplantation_disease'),
        first_rare_urogenital_disease_data_icd10.select('person_id','rare_urogenital_disease')
    ]

    # Join all rare disease dataframes with Covid cohort
    for rdf in rare_disease_dfs:
        df = df.join(rdf, on='person_id', how='leftouter')

    # Rename RSD_without_pff after exclusion of palantir facial fibromatosis
    df = df.withColumnRenamed("RSD_without_pff", "rare_skin_disease")

    # Columns to fill null with 0 (excluding rare_disorder_due_to_toxic_effects because we will drop it)
    selected_rd_columns = [
        'rare_abdominal_surgical_diseases', 'rare_bone_diseases', 'rare_cardiac_diseases',
        'rare_circulatory_system_disease', 'rare_developmental_defect_during_embryogenesis','rare_disorder_due_to_toxic_effects',
        'rare_endocrine_disease', 'rare_gastroenterologic_disease', 'rare_gynecologic_or_obstetric_disease',
        'rare_hematologic_disease', 'rare_hepatic_disease', 'rare_immune_disease',
        'rare_inborn_errors_of_metabolism', 'rare_infectious_disease',
        'rare_maxillo_facial_surgical_disease', 'rare_neoplastic_disease',
        'rare_neurologic_disease', 'rare_odontologic_disease', 'rare_ophthalmic_disorder',
        'rare_otorhinolaryngologic_disease', 'rare_renal_disease', 'rare_respiratory_disease',
        'rare_skin_disease', 'rare_surgical_thoracic_disease',
        'rare_systemic_or_rheumatologic_disease', 'rare_transplantation_disease',
        'rare_urogenital_disease'
    ]

    # Fill nulls with 0
    df = df.fillna(0, subset=selected_rd_columns)

    # Filter to exclude patients with rare disorder due to toxic effects
#    df = df.filter(col("rare_disorder_due_to_toxic_effects") == 0)

    # Drop the excluded column
#    df = df.drop("rare_disorder_due_to_toxic_effects")

    return df

"""
# merging covid positive patients with different classess of rare diseases 
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F
def RD_class_with_covid( first_rare_abdominal_surgical_diseases, first_rare_bone_diseases, first_rare_cardiac_diseases, first_rare_circulatory_system_disease, first_rare_developmental_defect_during_embryogenesis, first_rare_endocrine_disease, first_rare_gastroenterologic_disease, first_rare_gynecologic_or_obstetric_disease, first_rare_hematologic_disease, first_rare_hepatic_disease, first_rare_immune_disease, first_rare_inborn_errors_of_metabolism, first_rare_infectious_disease, first_rare_maxillo_facial_surgical_disease, first_rare_neoplastic_disease, first_rare_neurologic_disease, first_rare_odontologic_disease, first_rare_ophthalmic_disorder, first_rare_otorhinolaryngologic_disease, first_rare_renal_disease, first_rare_respiratory_disease, rare_skin_disease_new, first_rare_surgical_thoracic_disease, first_rare_systemic_or_rheumatologic_disease, first_rare_transplantation_disease_data_icd10, first_rare_urogenital_disease_data_icd10, Covid_cohort_final, first_rare_disorder_due_to_toxic_effects):
    df = Covid_cohort_final
    df1 =first_rare_abdominal_surgical_diseases.select('person_id','rare_abdominal_surgical_diseases')
    df2 = first_rare_bone_diseases.select('person_id','rare_bone_diseases')
    df3 = first_rare_cardiac_diseases.select('person_id','rare_cardiac_diseases')
    df4 = first_rare_circulatory_system_disease.select('person_id','rare_circulatory_system_disease')
    df5 = first_rare_developmental_defect_during_embryogenesis.select('person_id','rare_developmental_defect_during_embryogenesis') 
    df6 = first_rare_disorder_due_to_toxic_effects.select('person_id','rare_disorder_due_to_toxic_effects')
    df7 = first_rare_endocrine_disease.select('person_id','rare_endocrine_disease')
    df8 = first_rare_gastroenterologic_disease.select('person_id','rare_gastroenterologic_disease')
    df9 = first_rare_gynecologic_or_obstetric_disease.select('person_id','rare_gynecologic_or_obstetric_disease') 
    df10 = first_rare_hematologic_disease.select('person_id','rare_hematologic_disease') 
    df11 = first_rare_hepatic_disease.select('person_id','rare_hepatic_disease') 
    df12 = first_rare_immune_disease.select('person_id','rare_immune_disease') 
    df13 = first_rare_inborn_errors_of_metabolism.select('person_id','rare_inborn_errors_of_metabolism') 
    df14 = first_rare_infectious_disease.select('person_id','rare_infectious_disease') 
    df15 = first_rare_maxillo_facial_surgical_disease.select('person_id','rare_maxillo_facial_surgical_disease')
    df16 = first_rare_neoplastic_disease.select('person_id','rare_neoplastic_disease') 
    df17 = first_rare_neurologic_disease.select('person_id','rare_neurologic_disease')
    df18 = first_rare_odontologic_disease.select('person_id','rare_odontologic_disease')
    df19 = first_rare_ophthalmic_disorder.select('person_id','rare_ophthalmic_disorder')
    df20 = first_rare_otorhinolaryngologic_disease.select('person_id','rare_otorhinolaryngologic_disease')
    df21 = first_rare_renal_disease.select('person_id','rare_renal_disease')
    df22 = first_rare_respiratory_disease.select('person_id','rare_respiratory_disease')
    df23 = rare_skin_disease_new.select('person_id','RSD_without_pff')
    df24 = first_rare_surgical_thoracic_disease.select('person_id','rare_surgical_thoracic_disease')
    df25 = first_rare_systemic_or_rheumatologic_disease.select('person_id','rare_systemic_or_rheumatologic_disease')
    df26 = first_rare_transplantation_disease_data_icd10.select('person_id','rare_transplantation_disease')
    df27 = first_rare_urogenital_disease_data_icd10.select('person_id','rare_urogenital_disease')
    # join all rare diseases dataframes with covid positive table
    dfs_list = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20,df21,df22,df23,df24,df25,df26,df27]
    for df_i in dfs_list:
        df = df.join(df_i, on = 'person_id', how = 'leftouter')
    # list of selected rare disease columns where NaN will be replaced with 0
    selected_rd_columns = ['rare_abdominal_surgical_diseases','rare_bone_diseases','rare_cardiac_diseases','rare_circulatory_system_disease','rare_developmental_defect_during_embryogenesis','rare_disorder_due_to_toxic_effects','rare_endocrine_disease','rare_gastroenterologic_disease','rare_gynecologic_or_obstetric_disease','rare_hematologic_disease','rare_hepatic_disease','rare_immune_disease','rare_inborn_errors_of_metabolism','rare_infectious_disease','rare_maxillo_facial_surgical_disease','rare_neoplastic_disease','rare_neurologic_disease','rare_odontologic_disease','rare_ophthalmic_disorder','rare_otorhinolaryngologic_disease','rare_renal_disease','rare_respiratory_disease','RSD_without_pff','rare_surgical_thoracic_disease','rare_systemic_or_rheumatologic_disease','rare_transplantation_disease','rare_urogenital_disease' ]
    ## rename RSD_without_pff after exclusion of palantir facial firomatosis
    df = df.withColumnRenamed("RSD_without_pff", "rare_skin_disease")
    # Fill NaN with 0 in selected columns 
    df = df.fillna(0, subset = selected_rd_columns)
    #for column_name in selected_rd_columns:
    #    df = df.withColumn(column_name, col(column_name).fillna(0))
#    df = df.filter(df.rare_disorder_due_to_toxic_effects == 0)
    # since we excluding rare_disorder_due_to_toxic_effects from analysis
#    df = df.drop('rare_disorder_due_to_toxic_effects')
    return df
"""

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1e19ff64-0c69-477c-9cec-5d6cc0e25588"),
    Covid_cohort_final=Input(rid="ri.foundry.main.dataset.7eb11610-f14d-4ceb-9ed1-b0651a3ab75d"),
    first_rare_abdominal_surgical_diseases=Input(rid="ri.foundry.main.dataset.f335e77b-939d-4a2d-9e9e-0b6e992ce41c"),
    first_rare_bone_diseases=Input(rid="ri.foundry.main.dataset.ec2a51eb-d475-4a32-88b3-c90dc5272338"),
    first_rare_cardiac_diseases=Input(rid="ri.foundry.main.dataset.e176007b-bfb2-41c1-a515-c479e5865b11"),
    first_rare_circulatory_system_disease=Input(rid="ri.foundry.main.dataset.f7a18651-258a-453f-8043-d37ddebc643d"),
    first_rare_developmental_defect_during_embryogenesis=Input(rid="ri.foundry.main.dataset.e319061d-cc2b-4765-bffb-b97215109992"),
    first_rare_disorder_due_to_toxic_effects=Input(rid="ri.foundry.main.dataset.821c4d56-2c25-45ad-b12e-01749aa35ab1"),
    first_rare_endocrine_disease=Input(rid="ri.foundry.main.dataset.2fa3cfc4-93da-4963-9f02-b82eb9de0248"),
    first_rare_gastroenterologic_disease=Input(rid="ri.foundry.main.dataset.b46758c1-6a47-4284-93cb-ab7e294fae16"),
    first_rare_gynecologic_or_obstetric_disease=Input(rid="ri.foundry.main.dataset.ff9f5083-f63b-4a37-a3c9-911ca48a84e3"),
    first_rare_hematologic_disease=Input(rid="ri.foundry.main.dataset.2584c685-f673-4bf3-a59e-ca2f86a1c69f"),
    first_rare_hepatic_disease=Input(rid="ri.foundry.main.dataset.94b89e17-5c0e-4d84-9951-506480c7a1ef"),
    first_rare_immune_disease=Input(rid="ri.foundry.main.dataset.01e7f690-6ecd-4a6e-ace8-dd122fd66cdf"),
    first_rare_inborn_errors_of_metabolism=Input(rid="ri.foundry.main.dataset.312b0bcb-5d57-4a85-87f0-787aa456c97b"),
    first_rare_infectious_disease=Input(rid="ri.foundry.main.dataset.227bb4b8-5d68-451d-add3-2e0654d0969b"),
    first_rare_maxillo_facial_surgical_disease=Input(rid="ri.foundry.main.dataset.e4d04326-8a21-49f9-93f7-7ebeea30a1ef"),
    first_rare_neoplastic_disease=Input(rid="ri.foundry.main.dataset.63f1b24e-dfa6-4371-93d6-0d9efd8fb798"),
    first_rare_neurologic_disease=Input(rid="ri.foundry.main.dataset.0b1b0842-8924-4496-a605-0af5b5fb85bd"),
    first_rare_odontologic_disease=Input(rid="ri.foundry.main.dataset.1874259d-80a3-467e-be3e-e1e69cc42662"),
    first_rare_ophthalmic_disorder=Input(rid="ri.foundry.main.dataset.989a5311-6f9a-409b-af2e-2216e04d02d0"),
    first_rare_otorhinolaryngologic_disease=Input(rid="ri.foundry.main.dataset.c79a45d1-2fc7-4480-8855-4ada182a1e88"),
    first_rare_renal_disease=Input(rid="ri.foundry.main.dataset.bd8232fb-d88f-4143-9b46-66ae90c38a44"),
    first_rare_respiratory_disease=Input(rid="ri.foundry.main.dataset.8d067240-1fd0-40a7-885d-4579f1a68b54"),
    first_rare_skin_disease=Input(rid="ri.foundry.main.dataset.91a0b221-d56d-4fdb-a17a-b5cdaa859b5b"),
    first_rare_surgical_thoracic_disease=Input(rid="ri.foundry.main.dataset.48bd8e4c-4d0e-4540-b2d0-7ac4f026d5c4"),
    first_rare_systemic_or_rheumatologic_disease=Input(rid="ri.foundry.main.dataset.69a15fb8-31bd-4905-84b3-9a8b8b5f4332"),
    first_rare_transplantation_disease_data_icd10=Input(rid="ri.foundry.main.dataset.b7fa9033-a53c-4fd6-8caa-8cc614354c07"),
    first_rare_urogenital_disease_data_icd10=Input(rid="ri.foundry.main.dataset.b31d5643-c5ac-4d8c-8dc1-6709830a911d")
)
"""
Based on rare disease domain expert(Eric & Dominique) suggestion, We have excluded rare disease class 'rare disorder due to toxic effets' from our analysis.
In following cohort (rare_disorder_due_to_toxic_effects == 0) which will exclude patients from all cohorts.  
"""

# merging covid positive patients with different classess of rare diseases 
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F
def RD_linearization_class_with_covid( first_rare_abdominal_surgical_diseases, first_rare_bone_diseases, first_rare_cardiac_diseases, first_rare_circulatory_system_disease, first_rare_developmental_defect_during_embryogenesis, first_rare_endocrine_disease, first_rare_gastroenterologic_disease, first_rare_gynecologic_or_obstetric_disease, first_rare_hematologic_disease, first_rare_hepatic_disease, first_rare_immune_disease, first_rare_inborn_errors_of_metabolism, first_rare_infectious_disease, first_rare_maxillo_facial_surgical_disease, first_rare_neoplastic_disease, first_rare_neurologic_disease, first_rare_odontologic_disease, first_rare_ophthalmic_disorder, first_rare_otorhinolaryngologic_disease, first_rare_renal_disease, first_rare_respiratory_disease, first_rare_skin_disease, first_rare_surgical_thoracic_disease, first_rare_systemic_or_rheumatologic_disease, first_rare_transplantation_disease_data_icd10, first_rare_urogenital_disease_data_icd10, Covid_cohort_final, first_rare_disorder_due_to_toxic_effects):
    df = Covid_cohort_final
    df1 =first_rare_abdominal_surgical_diseases.select('person_id','rare_abdominal_surgical_diseases')
    df2 = first_rare_bone_diseases.select('person_id','rare_bone_diseases')
    df3 = first_rare_cardiac_diseases.select('person_id','rare_cardiac_diseases')
    df4 = first_rare_circulatory_system_disease.select('person_id','rare_circulatory_system_disease')
    df5 = first_rare_developmental_defect_during_embryogenesis.select('person_id','rare_developmental_defect_during_embryogenesis') 
    df6 = first_rare_disorder_due_to_toxic_effects.select('person_id','rare_disorder_due_to_toxic_effects')
    df7 = first_rare_endocrine_disease.select('person_id','rare_endocrine_disease')
    df8 = first_rare_gastroenterologic_disease.select('person_id','rare_gastroenterologic_disease')
    df9 = first_rare_gynecologic_or_obstetric_disease.select('person_id','rare_gynecologic_or_obstetric_disease') 
    df10 = first_rare_hematologic_disease.select('person_id','rare_hematologic_disease') 
    df11 = first_rare_hepatic_disease.select('person_id','rare_hepatic_disease') 
    df12 = first_rare_immune_disease.select('person_id','rare_immune_disease') 
    df13 = first_rare_inborn_errors_of_metabolism.select('person_id','rare_inborn_errors_of_metabolism') 
    df14 = first_rare_infectious_disease.select('person_id','rare_infectious_disease') 
    df15 = first_rare_maxillo_facial_surgical_disease.select('person_id','rare_maxillo_facial_surgical_disease')
    df16 = first_rare_neoplastic_disease.select('person_id','rare_neoplastic_disease') 
    df17 = first_rare_neurologic_disease.select('person_id','rare_neurologic_disease')
    df18 = first_rare_odontologic_disease.select('person_id','rare_odontologic_disease')
    df19 = first_rare_ophthalmic_disorder.select('person_id','rare_ophthalmic_disorder')
    df20 = first_rare_otorhinolaryngologic_disease.select('person_id','rare_otorhinolaryngologic_disease')
    df21 = first_rare_renal_disease.select('person_id','rare_renal_disease')
    df22 = first_rare_respiratory_disease.select('person_id','rare_respiratory_disease')
    df23 = first_rare_skin_disease.select('person_id','rare_skin_disease')
    df24 = first_rare_surgical_thoracic_disease.select('person_id','rare_surgical_thoracic_disease')
    df25 = first_rare_systemic_or_rheumatologic_disease.select('person_id','rare_systemic_or_rheumatologic_disease')
    df26 = first_rare_transplantation_disease_data_icd10.select('person_id','rare_transplantation_disease')
    df27 = first_rare_urogenital_disease_data_icd10.select('person_id','rare_urogenital_disease')
    # join all rare diseases dataframes with covid positive table
    dfs_list = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20,df21,df22,df23,df24,df25,df26,df27]
    for df_i in dfs_list:
        df = df.join(df_i, on = 'person_id', how = 'leftouter')
    # list of selected rare disease columns where NaN will be replaced with 0
    selected_rd_columns = ['rare_abdominal_surgical_diseases','rare_bone_diseases','rare_cardiac_diseases','rare_circulatory_system_disease','rare_developmental_defect_during_embryogenesis','rare_disorder_due_to_toxic_effects','rare_endocrine_disease','rare_gastroenterologic_disease','rare_gynecologic_or_obstetric_disease','rare_hematologic_disease','rare_hepatic_disease','rare_immune_disease','rare_inborn_errors_of_metabolism','rare_infectious_disease','rare_maxillo_facial_surgical_disease','rare_neoplastic_disease','rare_neurologic_disease','rare_odontologic_disease','rare_ophthalmic_disorder','rare_otorhinolaryngologic_disease','rare_renal_disease','rare_respiratory_disease','rare_skin_disease','rare_surgical_thoracic_disease','rare_systemic_or_rheumatologic_disease','rare_transplantation_disease','rare_urogenital_disease' ]
    # Fill NaN with 0 in selected columns 
    df = df.fillna(0, subset = selected_rd_columns)
    #for column_name in selected_rd_columns:
    #    df = df.withColumn(column_name, col(column_name).fillna(0))
#    df = df.filter(df.rare_disorder_due_to_toxic_effects == 0)
    # since we excluding rare_disorder_due_to_toxic_effects from analysis
#    df = df.drop('rare_disorder_due_to_toxic_effects')
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f335e77b-939d-4a2d-9e9e-0b6e992ce41c"),
    rare_abdominal_surgical_diseases_data_icd10=Input(rid="ri.foundry.main.dataset.ee05d8d3-8194-428c-8d50-546ae186c19b")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_abdominal_surgical_diseases(rare_abdominal_surgical_diseases_data_icd10):
    df = rare_abdominal_surgical_diseases_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_abdominal_surgical_diseases', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ec2a51eb-d475-4a32-88b3-c90dc5272338"),
    rare_bone_diseases_data_icd10=Input(rid="ri.foundry.main.dataset.d4f49b55-dd0b-46e9-b1b0-603e40a77c51")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_bone_diseases(rare_bone_diseases_data_icd10):
    df = rare_bone_diseases_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_bone_diseases', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e176007b-bfb2-41c1-a515-c479e5865b11"),
    rare_cardiac_diseases_data_icd10=Input(rid="ri.foundry.main.dataset.f92cb30f-37f6-4916-99d3-cf0cbdddb9ed")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_cardiac_diseases(rare_cardiac_diseases_data_icd10):
    df = rare_cardiac_diseases_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_cardiac_diseases', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f7a18651-258a-453f-8043-d37ddebc643d"),
    rare_circulatory_system_disease_data_icd10=Input(rid="ri.foundry.main.dataset.81219981-77fe-4ff6-8ad5-79a88ecd5bde")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_circulatory_system_disease(rare_circulatory_system_disease_data_icd10):
    df = rare_circulatory_system_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_circulatory_system_disease', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e319061d-cc2b-4765-bffb-b97215109992"),
    rare_developmental_defect_during_embryogenesis_data_icd10=Input(rid="ri.foundry.main.dataset.873c0368-60b9-4f8b-9522-40f33f90af78")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_developmental_defect_during_embryogenesis(rare_developmental_defect_during_embryogenesis_data_icd10):
    df = rare_developmental_defect_during_embryogenesis_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_developmental_defect_during_embryogenesis', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.821c4d56-2c25-45ad-b12e-01749aa35ab1"),
    rare_disorder_due_to_toxic_effects_data_icd10=Input(rid="ri.foundry.main.dataset.d0890b78-2138-4332-945f-50b306cbf262")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_disorder_due_to_toxic_effects(rare_disorder_due_to_toxic_effects_data_icd10):
    df = rare_disorder_due_to_toxic_effects_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_disorder_due_to_toxic_effects', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2fa3cfc4-93da-4963-9f02-b82eb9de0248"),
    rare_endocrine_disease_data_icd10=Input(rid="ri.foundry.main.dataset.e248d5f8-4e5c-4ca0-b0bf-013d303502a8")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_endocrine_disease(rare_endocrine_disease_data_icd10):
    df = rare_endocrine_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_endocrine_disease', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b46758c1-6a47-4284-93cb-ab7e294fae16"),
    rare_gastroenterologic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.e298ae32-b94a-4c36-b066-8c78da4c9335")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_gastroenterologic_disease(rare_gastroenterologic_disease_data_icd10):
    df = rare_gastroenterologic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_gastroenterologic_disease', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ff9f5083-f63b-4a37-a3c9-911ca48a84e3"),
    rare_gynecologic_or_obstetric_disease_data_icd10=Input(rid="ri.foundry.main.dataset.c5fc7003-adf2-4ea4-945d-cda6b448b1b0")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_gynecologic_or_obstetric_disease(rare_gynecologic_or_obstetric_disease_data_icd10):
    df = rare_gynecologic_or_obstetric_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_gynecologic_or_obstetric_disease', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2584c685-f673-4bf3-a59e-ca2f86a1c69f"),
    rare_hematologic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.205db57f-8da7-408a-a44d-ad0de4300e71")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_hematologic_disease(rare_hematologic_disease_data_icd10):
    df = rare_hematologic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_hematologic_disease', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.94b89e17-5c0e-4d84-9951-506480c7a1ef"),
    rare_hepatic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.26c4ace1-5c3f-413b-93ed-f87ef0413c97")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_hepatic_disease(rare_hepatic_disease_data_icd10):
    df = rare_hepatic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_hepatic_disease', lit(1))
    return df
    
    
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.01e7f690-6ecd-4a6e-ace8-dd122fd66cdf"),
    rare_immune_disease_data_icd10=Input(rid="ri.foundry.main.dataset.8614e4b4-d3b9-413c-8786-16a61fc0bfc5")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_immune_disease(rare_immune_disease_data_icd10):
    df = rare_immune_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_immune_disease', lit(1))
    return df
    
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.312b0bcb-5d57-4a85-87f0-787aa456c97b"),
    rare_inborn_errors_of_metabolism_data_icd10=Input(rid="ri.foundry.main.dataset.85bfffa4-b16d-4684-b5a0-5a17b1d2d044")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_inborn_errors_of_metabolism(rare_inborn_errors_of_metabolism_data_icd10):
    df = rare_inborn_errors_of_metabolism_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_inborn_errors_of_metabolism', lit(1))
    return df
    
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.227bb4b8-5d68-451d-add3-2e0654d0969b"),
    rare_infectious_disease_data_icd10=Input(rid="ri.foundry.main.dataset.ff22e10d-b481-4051-b572-bf744abc65a8")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_infectious_disease(rare_infectious_disease_data_icd10):
    df = rare_infectious_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_infectious_disease', lit(1))
    return df
    
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e4d04326-8a21-49f9-93f7-7ebeea30a1ef"),
    rare_maxillo_facial_surgical_disease_data_icd10=Input(rid="ri.foundry.main.dataset.943bdbf2-bd07-431f-bf10-96f3db509d94")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_maxillo_facial_surgical_disease(rare_maxillo_facial_surgical_disease_data_icd10):
    df = rare_maxillo_facial_surgical_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_maxillo_facial_surgical_disease', lit(1))
    return df
    
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.63f1b24e-dfa6-4371-93d6-0d9efd8fb798"),
    rare_neoplastic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.9f2dfa46-9b4d-4de8-9595-98b9e70b7171")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_neoplastic_disease(rare_neoplastic_disease_data_icd10):
    df = rare_neoplastic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_neoplastic_disease', lit(1))
    return df
    
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0b1b0842-8924-4496-a605-0af5b5fb85bd"),
    rare_neurologic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.be5bfeb6-e7b9-4043-92b4-26ed3beb15aa")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_neurologic_disease(rare_neurologic_disease_data_icd10):
    df = rare_neurologic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_neurologic_disease', lit(1))
    return df
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1874259d-80a3-467e-be3e-e1e69cc42662"),
    rare_odontologic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.80a26cd8-3940-431a-95d1-376b6dcabcd0")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_odontologic_disease(rare_odontologic_disease_data_icd10):
    df = rare_odontologic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_odontologic_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.989a5311-6f9a-409b-af2e-2216e04d02d0"),
    rare_ophthalmic_disorder_data_icd10=Input(rid="ri.foundry.main.dataset.b9b1a407-81e4-4df6-b467-7789685a1b38")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_ophthalmic_disorder(rare_ophthalmic_disorder_data_icd10):
    df = rare_ophthalmic_disorder_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_ophthalmic_disorder', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c79a45d1-2fc7-4480-8855-4ada182a1e88"),
    rare_otorhinolaryngologic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.138dd1f8-9ff0-452e-a385-6009d8acf048")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_otorhinolaryngologic_disease(rare_otorhinolaryngologic_disease_data_icd10):
    df = rare_otorhinolaryngologic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_otorhinolaryngologic_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bd8232fb-d88f-4143-9b46-66ae90c38a44"),
    rare_renal_disease_data_icd10=Input(rid="ri.foundry.main.dataset.6bc4da83-2cc6-4c12-99ce-2a7a4a81900f")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_renal_disease(rare_renal_disease_data_icd10):
    df = rare_renal_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_renal_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8d067240-1fd0-40a7-885d-4579f1a68b54"),
    rare_respiratory_disease_data_icd10=Input(rid="ri.foundry.main.dataset.f6a5c2dd-ca5c-4b03-b0d8-348b87cc8bed")
)

# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_respiratory_disease(rare_respiratory_disease_data_icd10):
    df = rare_respiratory_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_respiratory_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.91a0b221-d56d-4fdb-a17a-b5cdaa859b5b"),
    rare_skin_disease_data_icd10=Input(rid="ri.foundry.main.dataset.4eede74d-647a-4c28-b104-51272c043d20")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_skin_disease(rare_skin_disease_data_icd10):
    df = rare_skin_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_skin_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.48bd8e4c-4d0e-4540-b2d0-7ac4f026d5c4"),
    rare_surgical_thoracic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.6771f87a-a36b-4e4d-bdff-7993a5a7fffd")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_surgical_thoracic_disease(rare_surgical_thoracic_disease_data_icd10):
    df = rare_surgical_thoracic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_surgical_thoracic_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.69a15fb8-31bd-4905-84b3-9a8b8b5f4332"),
    rare_systemic_or_rheumatologic_disease_data_icd10=Input(rid="ri.foundry.main.dataset.350ba113-30fe-4ffd-9ca9-eb24cfe89fce")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_systemic_or_rheumatologic_disease(rare_systemic_or_rheumatologic_disease_data_icd10):
    df = rare_systemic_or_rheumatologic_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_systemic_or_rheumatologic_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b7fa9033-a53c-4fd6-8caa-8cc614354c07"),
    rare_transplantation_disease_data_icd10=Input(rid="ri.foundry.main.dataset.0a7ba25e-f224-43e0-9a59-e725b9a89e21")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_transplantation_disease_data_icd10(rare_transplantation_disease_data_icd10):
    df = rare_transplantation_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_transplantation_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b31d5643-c5ac-4d8c-8dc1-6709830a911d"),
    rare_urogenital_disease_data_icd10=Input(rid="ri.foundry.main.dataset.248cf8e8-e40e-458d-8201-a890b8c58b50")
)
# rare disease first occurance recorded for all patients in the table
# each patients had rare disease yes/no recorded as 1/0.
from pyspark.sql.functions import isnull, when, count, col, lit
from pyspark.sql import functions as F

def first_rare_urogenital_disease_data_icd10(rare_urogenital_disease_data_icd10):
    df = rare_urogenital_disease_data_icd10
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    df = df.groupby("person_id").pivot("condition_concept_name").agg(F.min('condition_start_date'))
    
    cols = df.columns[1:]
#    exprs = [when(col(c).isNull(),0).therwise(1).alias(c) for c in cols]
    df = df.select(col(df.columns[0]), *[F.when(F.col(col).isNull(), 0).otherwise(1).alias(col) for col in cols])
    df = df.withColumn('rare_urogenital_disease', lit(1))
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8f8570df-1729-4969-b128-1f0318ac5854"),
    rd_covid_linear_cohort=Input(rid="ri.foundry.main.dataset.ffcbf658-a0af-43b3-807f-ff852389b256")
)

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.functions import when, col
import pandas as pd
import numpy as np
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer, KNNImputer
from pyspark.sql.functions import mean

from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit, regexp_replace,mean

def hormonised_data_rds(rd_covid_linear_cohort):
    df2 = rd_covid_linear_cohort
    # merge mild_liver_disease & severe_liver_disease in to binary columns liver_disease
    df2 = df2.withColumn("liver_disease", when((df2.MILD_liver == 1) | (df2.Moderat_severe_liver == 1), 1).otherwise(0))

    # merge Malignant_cancer & Metastatic_solid_tumor_cancer in to binary columns cancer
    df2 = df2.withColumn("Cancer", when((df2.Malignant_cancer == 1) | (df2.Metastatic_solid_tumor_cancer == 1), 1).otherwise(0))

    # merge DMCx_before & Diabetes_uncomplicated in to binary columns Diabetes
    df2 = df2.withColumn("Diabetes", when((df2.DMCx_before == 1) | (df2.Diabetes_uncomplicated == 1), 1).otherwise(0))

    df2 = df2.drop("MILD_liver","Moderat_severe_liver", "Malignant_cancer", "Metastatic_solid_tumor_cancer", "DMCx_before", "Diabetes_uncomplicated")
 
    # Define a function to fill null values with the column mean
    def fillna_mean(df, include=set()):
        means = df.agg(*(mean(x).alias(x) for x in df.columns if x in include))
        return df.na.fill(means.first().asDict())

    # Specify the columns you want to impute (e.g., "BMI")
    columns_to_impute = ["BMI"]

    # Apply the imputation function to the DataFrame
    df2 = fillna_mean(df2, include=columns_to_impute)

    # Define BMI categories
    df2 = df2.withColumn("BMI_category", when(col("BMI") < 18.5, "BMI_under_weight")
                        .when((col("BMI") >= 18.5) & (col("BMI") < 25), "BMI_normal")
                        .when((col("BMI") >= 25) & (col("BMI") < 30), "BMI_over_weight")
                        .otherwise("BMI_obese"))

    # StringIndexer to convert categorical column to numeric indices
    indexer = StringIndexer(inputCol="BMI_category", outputCol="BMI_index")
    indexed_df = indexer.fit(df2).transform(df2)   
    return indexed_df 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.80b4d22a-13b6-49b0-885a-954c076f3163"),
    first_rare_skin_disease=Input(rid="ri.foundry.main.dataset.91a0b221-d56d-4fdb-a17a-b5cdaa859b5b")
)
from pyspark.sql.functions import when, col

def rare_skin_disease_new(first_rare_skin_disease):
    df = first_rare_skin_disease.select(
        "person_id",
        "plantar_fascial_fibromatosis",
        "rare_skin_disease"
    )

    df = df.withColumn(
        "RSD_with_pff",
        when(
            (col("plantar_fascial_fibromatosis") == 1), 
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "RSD_without_pff",
        1 - col("RSD_with_pff")
    )

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ffcbf658-a0af-43b3-807f-ff852389b256"),
    RD_linearization_class_with_covid=Input(rid="ri.foundry.main.dataset.1e19ff64-0c69-477c-9cec-5d6cc0e25588")
)
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit, regexp_replace

def rd_covid_linear_cohort(RD_linearization_class_with_covid):
    covid_with_RD_linearization_class = RD_linearization_class_with_covid
    df = covid_with_RD_linearization_class.select("person_id","data_partner_id","COVID_first_PCR_or_AG_lab_positive", "COVID_first_poslab_or_diagnosis_date","age_at_covid","BMI_max_observed_or_calculated_before_covid","COVID_hospitalization_length_of_stay","COVID_associated_hospitalization_indicator" ,"COVID_patient_death_indicator","number_of_COVID_vaccine_doses_before_covid","had_at_least_one_reinfection_post_covid_indicator","gender_concept_name","ethnicity_concept_name","race_concept_name","smoking_status","Severity_Type",   "postal_code","TUBERCULOSIS_before_covid_indicator","MILDLIVERDISEASE_before_covid_indicator","MODERATESEVERELIVERDISEASE_before_covid_indicator","RHEUMATOLOGICDISEASE_before_covid_indicator","DEMENTIA_before_covid_indicator","CONGESTIVEHEARTFAILURE_before_covid_indicator","KIDNEYDISEASE_before_covid_indicator","MALIGNANTCANCER_before_covid_indicator","DIABETESCOMPLICATED_before_covid_indicator","CEREBROVASCULARDISEASE_before_covid_indicator","PERIPHERALVASCULARDISEASE_before_covid_indicator","HEARTFAILURE_before_covid_indicator","HEMIPLEGIAORPARAPLEGIA_before_covid_indicator",     
"PSYCHOSIS_before_covid_indicator","OBESITY_before_covid_indicator","CORONARYARTERYDISEASE_before_covid_indicator","SYSTEMICCORTICOSTEROIDS_before_covid_indicator","DEPRESSION_before_covid_indicator",   
"METASTATICSOLIDTUMORCANCERS_before_covid_indicator","HIVINFECTION_before_covid_indicator","CHRONICLUNGDISEASE_before_covid_indicator","PEPTICULCER_before_covid_indicator","MYOCARDIALINFARCTION_before_covid_indicator","DIABETESUNCOMPLICATED_before_covid_indicator","CARDIOMYOPATHIES_before_covid_indicator", "HYPERTENSION_before_covid_indicator","TOBACCOSMOKER_before_covid_indicator","REMDISIVIR_during_covid_hospitalization_indicator",'rare_abdominal_surgical_diseases','rare_bone_diseases','rare_cardiac_diseases','rare_circulatory_system_disease','rare_developmental_defect_during_embryogenesis',"rare_disorder_due_to_toxic_effects",'rare_endocrine_disease','rare_gastroenterologic_disease','rare_gynecologic_or_obstetric_disease','rare_hematologic_disease','rare_hepatic_disease','rare_immune_disease','rare_inborn_errors_of_metabolism','rare_infectious_disease','rare_maxillo_facial_surgical_disease','rare_neoplastic_disease','rare_neurologic_disease','rare_odontologic_disease','rare_ophthalmic_disorder','rare_otorhinolaryngologic_disease','rare_renal_disease','rare_respiratory_disease','rare_skin_disease','rare_surgical_thoracic_disease','rare_systemic_or_rheumatologic_disease','rare_transplantation_disease','rare_urogenital_disease')

    df = df.withColumnRenamed('COVID_hospitalization_length_of_stay','Length_of_stay')\
           .withColumnRenamed('COVID_associated_hospitalization_indicator','hospitalized')\
           .withColumnRenamed('COVID_patient_death_indicator', 'death')\
           .withColumnRenamed('age_at_covid','Age')\
           .withColumnRenamed('gender_concept_name','Gender')\
           .withColumnRenamed('ethnicity_concept_name','Ethnicity')\
           .withColumnRenamed('race_concept_name','Race')\
           .withColumnRenamed('BMI_max_observed_or_calculated_before_covid','BMI')\
           .withColumnRenamed('TUBERCULOSIS_before_covid_indicator','Tuberculosis')\
           .withColumnRenamed('MILDLIVERDISEASE_before_covid_indicator','MILD_liver')\
           .withColumnRenamed('MODERATESEVERELIVERDISEASE_before_covid_indicator','Moderat_severe_liver')\
           .withColumnRenamed('THALASSEMIA_before_covid_indicator','Thalassemia')\
           .withColumnRenamed('RHEUMATOLOGICDISEASE_before_covid_indicator','Rheumatologic_disease')\
           .withColumnRenamed('DEMENTIA_before_covid_indicator','Dementia_before')\
           .withColumnRenamed('CONGESTIVEHEARTFAILURE_before_covid_indicator','Congestive_heart_failure')\
           .withColumnRenamed('KIDNEYDISEASE_before_covid_indicator','Kidney_disease')\
           .withColumnRenamed('MALIGNANTCANCER_before_covid_indicator','Malignant_cancer')\
           .withColumnRenamed('DIABETESCOMPLICATED_before_covid_indicator','DMCx_before')\
           .withColumnRenamed('CEREBROVASCULARDISEASE_before_covid_indicator','Cerebro_vascular_disease')\
           .withColumnRenamed('PERIPHERALVASCULARDISEASE_before_covid_indicator','Peripheral_vascular_disease')\
           .withColumnRenamed('HEARTFAILURE_before_covid_indicator','Heart_failure')\
           .withColumnRenamed('HEMIPLEGIAORPARAPLEGIA_before_covid_indicator','Hemiplegia_or_paraplegia')\
           .withColumnRenamed('PSYCHOSIS_before_covid_indicator','Psychosis')\
           .withColumnRenamed('CORONARYARTERYDISEASE_before_covid_indicator','Coronary_artery_disease')\
           .withColumnRenamed('SYSTEMICCORTICOSTEROIDS_before_covid_indicator','Systemic_corticosteroids')\
           .withColumnRenamed('DEPRESSION_before_covid_indicator','Depression')\
           .withColumnRenamed('METASTATICSOLIDTUMORCANCERS_before_covid_indicator','Metastatic_solid_tumor_cancer')\
           .withColumnRenamed('HIVINFECTION_before_covid_indicator','HIV_infection')\
           .withColumnRenamed('CHRONICLUNGDISEASE_before_covid_indicator','Chronic_lung_disease')\
           .withColumnRenamed('MYOCARDIALINFARCTION_before_covid_indicator','Myocardial_infraction')\
           .withColumnRenamed('CARDIOMYOPATHIES_before_covid_indicator','Cardiomyophaties')\
           .withColumnRenamed('HYPERTENSION_before_covid_indicator','HTN')\
           .withColumnRenamed('TOBACCOSMOKER_before_covid_indicator','Tobacco_smoker')\
           .withColumnRenamed('had_at_least_one_reinfection_post_covid_indicator','COVID_reinfection')\
           .withColumnRenamed("number_of_COVID_vaccine_doses_before_covid","Covid_vaccine_dose")\
           .withColumnRenamed("REMDISIVIR_during_covid_hospitalization_indicator",'Remdisvir_during_hospitalization')\
           .withColumnRenamed("DIABETESUNCOMPLICATED_before_covid_indicator",'Diabetes_uncomplicated')
           #.withColumnRenamed("OTHERIMMUNOCOMPROMISED_before_covid_indicator",'Immunocompromised')\
           #\.withColumnRenamed("SICKLECELLDISEASE_before_covid_indicator",'Sickle_cell_disease')
           #.withColumnRenamed('PULMONARYEMBOLISM_before_covid_indicator','Pulmonaryembolism')\
           #.withColumnRenamed('SOLIDORGANORBLOODSTEMCELLTRANSPLANT_before_covid_indicator','Solid_organ_or_blood_stem_cell_transplant')\
           #\.withColumnRenamed("PREGNANCY_before_covid_indicator", 'Pregenancy_before_covid')
   
    # Age binning
    df = df.withColumn("age_1_20", when((col('Age') >1) & (col('Age') <=20), 1).otherwise(0))\
            .withColumn("age_20_40", when((col('Age') >20) & (col('Age') <=40), 1).otherwise(0))\
            .withColumn("age_40_65", when((col('Age') >40) & (col('Age') <=65), 1).otherwise(0))\
            .withColumn("age_65_above", when(col('Age')>65, 1).otherwise(0))
    # renamed Severity_types long named into small names
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Mild_No_ED_or_Hosp_around_COVID_index','Mild'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Mild_ED_around_COVID_index','Mild_ED'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Moderate_Hosp_around_COVID_index','Moderate'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Severe_ECMO_IMV_in_Hosp_around_COVID_index','Severe'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Death_after_COVID_index','Death'))
  
    # Define a dictionary of values to replace in Race
    replace_dict = {'Black or African American':'Black_or_African_American',
                    'Black':'Black_or_African_American', 
                    'Asian or Pacific islander':'Asian',
                    'Asian Indian':'Asian',
                    'Filipino':'Asian',
                    'Chinese':'Asian',
                    'Korean':'Asian',
                    'Vietnamese':'Asian',
                    'Japanese':'Asian',
                    'Native Hawaiian or Other Pacific Islander':'Unknown',
                    'Other Pacific Islander':'Unknown',
                    'No matching concept':'Unknown',
                    'Other Race':'Unknown',
                    'Other':'Unknown',
                    'No information':'Unknown',
                    'Multiple races':'Unknown',
                    'Multiple race':'Unknown',
                    'Unknown racial group':'Unknown',
                    'Hispanic':'Unknown',
                    'Polynesian':'Unknown',
                    'Refuse to answer':'Unknown',
                    'More than one race':'Unknown', 
                    'Middle Eastern or North African' : 'Unknown',
                    'What Race Ethnicity: Race Ethnicity None Of These' : 'Unknown',
                    'American Indian or Alaska Native': 'Unknown'}

    # Replace values in Race column according to the dictionary
    df = df.replace(replace_dict, subset=['Race'])
    # Replace NaN values with 'Unknown'
    df = df.fillna('Unknown', subset=['Race'])

    # renamed Ethnicity long named into small names or grouped in wide cotegory
    replace_dict_ethnicity = {'Not Hispanic or Latino':'Not_Hispanic_or_Latino',
                    'Hispanic or Latino':'Hispanic_or_Latino',
                    'No matching concept':'Unknown',
                    'Other/Unknown':'Unknown',
                    'No information':'Unknown',
                    'Patient ethnicity unknown':'Unknown',
                    'Other':'Unknown',
                    'Refuse to answer':'Unknown'}

    # Replace values in Ethnicity column according to the dictionary
    df = df.replace(replace_dict_ethnicity, subset=['Ethnicity'])
    # Replace NaN values with 'Unknown'
    df = df.fillna('Unknown', subset=['Ethnicity'])
    # Replace values in smoking status column according to the dictionary
    df = df.replace({'Current or Former':'Current_or_Former'}, subset=['smoking_status'])
    df = df.withColumn("rare_disease", when(df['rare_abdominal_surgical_diseases'] == 1, 1)
                    .when(df['rare_bone_diseases'] == 1, 1)
                    .when(df['rare_cardiac_diseases'] == 1, 1)
                    .when(df['rare_circulatory_system_disease'] == 1, 1)
                    .when(df['rare_developmental_defect_during_embryogenesis'] == 1, 1) 
                    .when(df['rare_disorder_due_to_toxic_effects'] == 1, 1)
                    .when(df['rare_endocrine_disease'] == 1, 1)
                    .when(df['rare_gastroenterologic_disease'] == 1, 1)
                    .when(df['rare_gynecologic_or_obstetric_disease'] == 1, 1)
                    .when(df['rare_hematologic_disease'] == 1, 1)
                    .when(df['rare_hepatic_disease'] == 1, 1)
                    .when(df['rare_immune_disease'] == 1, 1)
                    .when(df['rare_inborn_errors_of_metabolism'] == 1, 1)
                    .when(df['rare_infectious_disease'] == 1, 1)
                    .when(df['rare_maxillo_facial_surgical_disease'] == 1, 1)
                    .when(df['rare_neoplastic_disease'] == 1, 1)
                    .when(df['rare_neurologic_disease'] == 1, 1)
                    .when(df['rare_odontologic_disease'] == 1, 1)
                    .when(df['rare_ophthalmic_disorder'] == 1, 1)
                    .when(df['rare_otorhinolaryngologic_disease'] == 1, 1)
                    .when(df['rare_renal_disease'] == 1, 1)
                    .when(df['rare_respiratory_disease'] == 1, 1)
                    .when(df['rare_skin_disease'] == 1, 1)
                    .when(df['rare_surgical_thoracic_disease'] == 1, 1)
                    .when(df['rare_systemic_or_rheumatologic_disease'] == 1, 1)
                    .when(df['rare_transplantation_disease'] == 1, 1)
                    .when(df['rare_urogenital_disease'] == 1, 1)
                    .otherwise(0))      

    return df

# removed these features here ,"PREGNANCY_before_covid_indicator", "SICKLECELLDISEASE_before_covid_indicator", #"OTHERIMMUNOCOMPROMISED_before_covid_indicator", "PULMONARYEMBOLISM_before_covid_indicator",#"SOLIDORGANORBLOODSTEMCELLTRANSPLANT_before_covid_indicator",

