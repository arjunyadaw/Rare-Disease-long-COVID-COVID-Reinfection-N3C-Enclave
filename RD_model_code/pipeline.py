

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5879be37-dfde-472f-85a3-cf9174996907"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace, when
def Cohort_for_sensiity_analysis(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort
    # Here cutoff data considered 23rd December 2021 because FDA/EUA drugs approval time
    df_omicron = df.where(df.COVID_first_poslab_or_diagnosis_date >= '2021-12-23') 
#    df = df_omicron
    # better vaccine & antiviral sites (site with minimum 20 % vaccination & minimum 10 antiviral)
    l = ['362','183','406','688','806','507','207','819','23','524','77','569','793','526','798','828','75','726','198','294','399','134']

    # filter out records by data prtner ids by list l
    df = df_omicron.filter(df_omicron.data_partner_id.isin(l))
    
    # Encoding object type features into binary features
    bmi_categories = df.select('BMI_category').distinct().rdd.flatMap(lambda x: x).collect()
    gender_categories = df.select('Gender').distinct().rdd.flatMap(lambda x: x).collect()
    ethnicity_categories = df.select('Ethnicity').distinct().rdd.flatMap(lambda x: x).collect()
    race_categories = df.select('Race').distinct().rdd.flatMap(lambda x: x).collect()
    smoking_status_categories = df.select('smoking_status').distinct().rdd.flatMap(lambda x: x).collect()

    bmi_exprs = [F.when(F.col('BMI_category') == cat, 1).otherwise(0).alias(f"{cat}") for cat in bmi_categories]
    gender_exprs = [F.when(F.col('Gender') == cat, 1).otherwise(0).alias(f"gender_{cat}") for cat in gender_categories]
    ethnicity_exprs = [F.when(F.col('Ethnicity') == cat, 1).otherwise(0).alias(f"ethnicity_{cat}") for cat in ethnicity_categories]
    race_exprs = [F.when(F.col('Race') == cat, 1).otherwise(0).alias(f"race_{cat}") for cat in race_categories]
    smoking_status_exprs = [F.when(F.col('smoking_status') == cat, 1).otherwise(0).alias(f"smoking_status_{cat}") for cat in smoking_status_categories]

    df2 = df.select(bmi_exprs + gender_exprs + race_exprs + ethnicity_exprs + smoking_status_exprs + df.columns)
#    cols_to_drop = ["Gender", "Race", "Ethnicity","smoking_status"]
#    df2 = df.drop(*cols_to_drop)

    # merge mild_liver_disease & severe_liver_disease in to binary columns liver_disease
    df2 = df2.withColumn("liver_disease", when((df2.MILD_liver == 1) | (df2.Moderat_severe_liver == 1), 1).otherwise(0))

    # merge Malignant_cancer & Metastatic_solid_tumor_cancer in to binary columns cancer
    df2 = df2.withColumn("Cancer", when((df2.Malignant_cancer == 1) | (df2.Metastatic_solid_tumor_cancer == 1), 1).otherwise(0))

    # merge DMCx_before & Diabetes_uncomplicated in to binary columns Diabetes
    df2 = df2.withColumn("Diabetes", when((df2.DMCx_before == 1) | (df2.Diabetes_uncomplicated == 1), 1).otherwise(0))

    df2 = df2.withColumn("life_thretening",
                     when(col("Severity_Type")=='Death', 1)
                    .when(col("Severity_Type")=='Severe', 1)
                    .otherwise(0))

    df2 = df2.withColumn("antiviral_treatment",
                     when(col("Paxlovid")== 1, 1)
                    .when(col("Molnupiravir")==1, 1)
                    .otherwise(0)) # .when(col("Bebtelovimab")==1, 1)

    df2 = df2.withColumn('Age_bin', 
                    when((df2.Age > 1) & (df2.Age <= 20), '1-20')
                    .when((df2.Age > 20) & (df2.Age <= 40), '20-40')
                    .when((df2.Age > 40) & (df2.Age <= 65), '40-65')
                    .otherwise('>65'))

    df2 = df2.drop("MILD_liver","Moderat_severe_liver", "Malignant_cancer", "Metastatic_solid_tumor_cancer", "DMCx_before", "Diabetes_uncomplicated","Severity_Type")

    return df2

"""
    # Following data partners have better representation of vaccination status and antiviral treatments    
#    l = ['526','726','793','294','406','439','819','507','569','207','828','806','399','688','23','134','183','524','77','362','798'] 

#    l = ['526','207','793','23','726','399','524','507','183','294']

    # better vaccine & antiviral sites (site with minimum 20 % vaccination)
#    l = ['526','207','793','23','726','198','399','524','507','183','294','77','806','798','828','688','569','819','362','134','406','75','678','157','770','439','213'] # 
### I have used these sites for vaccination status which is giving better results
### these are the site with more 50% vaccination 
###    l = ['526','207','793','23','726','198','399','524','960','507','183','124']
"""

@transform_pandas(
    Output(rid="ri.vector.main.execute.07dedff5-a475-4326-975d-e9a899c82b1d"),
    rd_cohort=Input(rid="ri.foundry.main.dataset.56a84162-780d-4199-a416-28f41120e969")
)

import pandas as pd
import numpy as np
import statsmodels.api as sm
import seaborn as sns
import matplotlib.pyplot as plt

print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)

def RD_HOSP_univariate(rd_cohort):# "rare_gynecologic_or_obstetric_disease",
    df = rd_cohort.select("life_threatening","hospitalized","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease").toPandas()

    print('hospitalized patients_yes:', df['hospitalized'].sum())
    print('hospitalized patients_no:', df.shape[0]-df['hospitalized'].sum())
    print('total size:', df.shape[0])

    # Initialize an empty DataFrame to store results
    results_df = pd.DataFrame(columns=['rds', 'coefficient', 'odds_ratio', 'CI_lower', 'CI_upper', 'pvalue', 'N'])
    # Loop through each rare disease ( "rare_gynecologic_or_obstetric_disease",)
    for rd_col in ["rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease"]:

        X = df[rd_col]
        X = sm.add_constant(X)  # Add an intercept term
        y = df['hospitalized']

        # Fit logistic regression model
        model = sm.Logit(y, X).fit()

        # Extract relevant coefficients and confidence intervals
        coef = model.params[rd_col]
        odds_ratio = np.exp(coef)
        ci_lower, ci_upper = np.exp(model.conf_int().loc[rd_col])
        pvalue = model.pvalues[rd_col]
        N = len(df)

        # Append results to the DataFrame
        results_df = results_df.append({
            'rds': rd_col,
            'coefficient': round(coef,2),
            'odds_ratio': round(odds_ratio,2),
            'CI_lower': round(ci_lower,2),
            'CI_upper': round(ci_upper,2),
            'pvalue': pvalue,
            'N': N
        }, ignore_index=True)
        results_df['odds_ratio_CI'] = results_df.apply(lambda row: f"{row['odds_ratio']} ({row['CI_lower']} - {row['CI_upper']})", axis = 1)

    return results_df[['rds','odds_ratio_CI','pvalue']]

@transform_pandas(
    Output(rid="ri.vector.main.execute.fd48b7c9-216f-4931-8d91-6e7c0d509b6c"),
    rd_cohort=Input(rid="ri.foundry.main.dataset.56a84162-780d-4199-a416-28f41120e969")
)

import pandas as pd
import numpy as np
import statsmodels.api as sm
import seaborn as sns
import matplotlib.pyplot as plt
def RD_LT_univariate(rd_cohort): 
    df = rd_cohort.select("life_threatening","hospitalized","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease").toPandas()

    print('life_threatening_yes:', df['life_threatening'].sum())
    print('life_threatening_no:', df.shape[0]-df['life_threatening'].sum())
    print('total size:', df.shape[0])

    # Initialize an empty DataFrame to store results
    results_df = pd.DataFrame(columns=['rds', 'coefficient', 'odds_ratio', 'CI_lower', 'CI_upper', 'pvalue', 'N'])
    # Loop through each rare disease ( "rare_gynecologic_or_obstetric_disease",)
    for rd_col in ["rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease"]:
     
        X = df[rd_col]
        X = sm.add_constant(X)  # Add an intercept term
        y = df['life_threatening']

        # Fit logistic regression model
        model = sm.Logit(y, X).fit()

        # Extract relevant coefficients and confidence intervals
        coef = model.params[rd_col]
        odds_ratio = np.exp(coef)
        ci_lower, ci_upper = np.exp(model.conf_int().loc[rd_col])
        pvalue = model.pvalues[rd_col]
        N = len(df)

        # Append results to the DataFrame
        results_df = results_df.append({
            'rds': rd_col,
            'coefficient': round(coef,2),
            'odds_ratio': round(odds_ratio,2),
            'CI_lower': round(ci_lower,2),
            'CI_upper': round(ci_upper,2),
            'pvalue': pvalue,
            'N': N
        }, ignore_index=True)
        results_df['odds_ratio_CI'] = results_df.apply(lambda row: f"{row['odds_ratio']} ({row['CI_lower']} - {row['CI_upper']})", axis = 1)

    return results_df[['rds','odds_ratio_CI','pvalue']]

@transform_pandas(
    Output(rid="ri.vector.main.execute.0d75d009-b618-496f-8312-c40956c7d7dd"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

def Severity_hospitalization_per_rd(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort.toPandas()

    # Select relevant columns
    cols = [
        "hospitalized", "rare_bone_diseases", "rare_cardiac_diseases", 
        "rare_developmental_defect_during_embryogenesis", "rare_endocrine_disease", 
        "rare_gastroenterologic_disease", "rare_hematologic_disease", "rare_hepatic_disease", 
        "rare_immune_disease", "rare_inborn_errors_of_metabolism", "rare_infectious_disease", 
        "rare_neoplastic_disease", "rare_neurologic_disease", "rare_ophthalmic_disorder", 
        "rare_otorhinolaryngologic_disease", "rare_renal_disease", "rare_respiratory_disease", 
        "rare_skin_disease", "rare_systemic_or_rheumatologic_disease"
    ]
    df = df[cols]

    # Harmonize Severity column
    df['Severity_Type'] = df['hospitalized'].replace({
        1: 'Hospitalized', 
        0: 'Not hospitalized'
    })
    df.drop('hospitalized', axis=1, inplace=True)

    # Aggregate counts
    df1 = df.groupby('Severity_Type').sum()

    # Format for plotting
    disease_classes = [x.replace('_', ' ').capitalize() for x in df1.columns]
    data = pd.DataFrame({
        'Rare disease class': disease_classes,
        'Hospitalized': df1.loc['Hospitalized'],
        'Not hospitalized': df1.loc['Not hospitalized']
    })

    # Calculate totals and percentages
    data['Total'] = data['Hospitalized'] + data['Not hospitalized']
    data['Hospitalized %'] = (data['Hospitalized'] / data['Total']) * 100
    data['Not hospitalized %'] = (data['Not hospitalized'] / data['Total']) * 100

    # Sort by hospitalized %
    data = data.sort_values(by='Hospitalized %', ascending=False)

    # Melt for seaborn grouped bar plot
    data_melted = data.melt(
        id_vars=["Rare disease class", "Total"],
        value_vars=["Not hospitalized %", "Hospitalized %"],
        var_name="Severity", value_name="Percentage"
    )

    # Add absolute counts for annotation
    count_mapping = {
        ('Hospitalized %', row['Rare disease class']): data.loc[idx, 'Hospitalized']
        for idx, row in data.iterrows()
    }
    count_mapping.update({
        ('Not hospitalized %', row['Rare disease class']): data.loc[idx, 'Not hospitalized']
        for idx, row in data.iterrows()
    })

    data_melted['Count'] = data_melted.apply(
        lambda x: count_mapping[(x['Severity'], x['Rare disease class'])], axis=1
    )

    # Rename severity labels to match legend
    data_melted['Severity'] = data_melted['Severity'].replace({
        'Hospitalized %': 'Hospitalized',
        'Not hospitalized %': 'Not hospitalized'
    })

    # Plotting
    sns.set(style="white")
    plt.figure(figsize=(16, 14), dpi=600)

    palette = {
        "Hospitalized": "#d55e00",
        "Not hospitalized": "#0072b2"
    }

    order = data['Rare disease class'].tolist()

    ax = sns.barplot(
        data=data_melted,
        y="Rare disease class",
        x="Percentage",
        hue="Severity",
        hue_order=["Not hospitalized", "Hospitalized"],
        palette=palette,
        order=order
    )

    # Set x-axis from 0 to 100 (percentage)
    ax.set_xlim(0, 100)

    # Remove spines
    sns.despine(left=True, bottom=True)

    # Add formatted labels: "10.7% (3,495)"
    for p, (_, row) in zip(ax.patches, data_melted.iterrows()):
        width = p.get_width()
        height = p.get_height()
        y = p.get_y()
        if width > 0:
            percentage = row['Percentage']
            count = int(row['Count'])
            label = f'{percentage:.1f}% ({count:,})'  # Thousand separator
            ax.text(width + 1, y + height / 2,
                    label,
                    ha='left', va='center', fontsize=14, color='black')

    # Titles and labels
    plt.title("Distribution of Hospitalized vs Not hospitalized", 
              fontsize=24, pad=20, weight='bold')
    plt.xlabel("Proportion of Patients Hospitalized Disease per RD Class", fontsize=22, labelpad=15)
    plt.ylabel("", fontsize=16)

    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)

    # Legend placed further to the right
    plt.legend(
        title="", fontsize=14, loc='center left', bbox_to_anchor=(1.28, 0.0), borderaxespad=0.
    )

    # Adjust layout to make space for legend
    plt.tight_layout() # rect=[0, 0, 0.85, 1]

    plt.show()

    # Return summary table
    return data[['Rare disease class', 'Hospitalized %', 'Not hospitalized %', 'Hospitalized', 'Not hospitalized']]

@transform_pandas(
    Output(rid="ri.vector.main.execute.33ca82a4-4daa-4a74-ad2d-9ea986749c94"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

def Severity_lt_per_rd(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort.toPandas()

    # Select relevant columns
    cols = [
        "rare_bone_diseases", "rare_cardiac_diseases", "rare_developmental_defect_during_embryogenesis",
        "rare_endocrine_disease", "rare_gastroenterologic_disease", "rare_hematologic_disease",
        "rare_hepatic_disease", "rare_immune_disease", "rare_inborn_errors_of_metabolism",
        "rare_infectious_disease", "rare_neoplastic_disease", "rare_neurologic_disease",
        "rare_ophthalmic_disorder", "rare_otorhinolaryngologic_disease", "rare_renal_disease",
        "rare_respiratory_disease", "rare_skin_disease", "rare_systemic_or_rheumatologic_disease",
        'Severity_Type'
    ]
    df = df[cols]

    # Harmonize Severity Types
    df['Severity_Type'] = df['Severity_Type'].replace({
        'Death': 'Life-threatening',
        'Severe': 'Life-threatening',
        'Moderate': 'Not life-threatening',
        'Mild_ED': 'Not life-threatening',
        'Mild': 'Not life-threatening'
    })

    # Aggregate counts
    df1 = df.groupby('Severity_Type').sum()

    # Format for plotting
    disease_classes = [x.replace('_', ' ').capitalize() for x in df1.columns]
    data = pd.DataFrame({
        'Rare disease class': disease_classes,
        'Life-threatening': df1.loc['Life-threatening'],
        'Not life-threatening': df1.loc['Not life-threatening']
    })

    # Calculate totals and percentages
    data['Total'] = data['Life-threatening'] + data['Not life-threatening']
    data['Life-threatening %'] = (data['Life-threatening'] / data['Total']) * 100
    data['Not life-threatening %'] = (data['Not life-threatening'] / data['Total']) * 100

    # Sort by 'Life-threatening %' descending
    data = data.sort_values(by='Life-threatening %', ascending=False)

    # Melt for seaborn grouped bar plot
    data_melted = data.melt(
        id_vars=["Rare disease class", "Total"],
        value_vars=["Not life-threatening %", "Life-threatening %"],
        var_name="Severity", value_name="Percentage"
    )

    # Add absolute counts for annotation
    count_mapping = {
        ('Life-threatening %', row['Rare disease class']): data.loc[idx, 'Life-threatening']
        for idx, row in data.iterrows()
    }
    count_mapping.update({
        ('Not life-threatening %', row['Rare disease class']): data.loc[idx, 'Not life-threatening']
        for idx, row in data.iterrows()
    })

    data_melted['Count'] = data_melted.apply(
        lambda x: count_mapping[(x['Severity'], x['Rare disease class'])], axis=1
    )

    # Rename severity labels to match legend
    data_melted['Severity'] = data_melted['Severity'].replace({
        'Life-threatening %': 'Life-threatening',
        'Not life-threatening %': 'Not life-threatening'
    })

    # Plotting
    sns.set(style="white")
    plt.figure(figsize=(16, 14), dpi=600)

    palette = {
        "Life-threatening": "#d55e00",
        "Not life-threatening": "#0072b2"
    }

    order = data['Rare disease class'].tolist()

    ax = sns.barplot(
        data=data_melted,
        y="Rare disease class",
        x="Percentage",
        hue="Severity",
        hue_order=["Not life-threatening", "Life-threatening"],
        palette=palette,
        order=order
    )

    # Set x-axis from 0 to 100 (percentage)
    ax.set_xlim(0, 100)

    # Remove spines
    sns.despine(left=True, bottom=True)

    # Add formatted labels: "10.7% (3,495)"
    for p, (_, row) in zip(ax.patches, data_melted.iterrows()):
        width = p.get_width()
        height = p.get_height()
        y = p.get_y()
        if width > 0:
            percentage = row['Percentage']
            count = int(row['Count'])
            label = f'{percentage:.1f}% ({count:,})'  # Thousand separator
            ax.text(width + 1, y + height / 2,
                    label,
                    ha='left', va='center', fontsize=14, color='black')

    # Titles and labels
    plt.title("Distribution of Life-threatening vs Not Life-threatening", 
              fontsize=24, pad=20, weight='bold')
    plt.xlabel("Proportion of Patients Life-threatening Disease per RD Class", fontsize=22, labelpad=15)
    plt.ylabel("", fontsize=16)

    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)

    # Legend placed further to the right
    plt.legend(
        title="", fontsize=14, loc='center left', bbox_to_anchor=(1.28, 0.0), borderaxespad=0.
    )

    # Adjust layout to make space for legend
    plt.tight_layout() # rect=[0, 0, 0.85, 1]

    plt.show()

    # Return summary table
    return data[['Rare disease class', 'Life-threatening %', 'Not life-threatening %', 'Life-threatening', 'Not life-threatening']]

"""
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

def Severity_lt_per_rd(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort.toPandas()

    # Select relevant columns
    cols = [
        "rare_bone_diseases", "rare_cardiac_diseases", "rare_developmental_defect_during_embryogenesis",
        "rare_endocrine_disease", "rare_gastroenterologic_disease", "rare_hematologic_disease",
        "rare_hepatic_disease", "rare_immune_disease", "rare_inborn_errors_of_metabolism",
        "rare_infectious_disease", "rare_neoplastic_disease", "rare_neurologic_disease",
        "rare_ophthalmic_disorder", "rare_otorhinolaryngologic_disease", "rare_renal_disease",
        "rare_respiratory_disease", "rare_skin_disease", "rare_systemic_or_rheumatologic_disease",
        'Severity_Type'
    ]
    df = df[cols]

    # Harmonize Severity Types
    df['Severity_Type'] = df['Severity_Type'].replace({
        'Death': 'Life-threatening',
        'Severe': 'Life-threatening',
        'Moderate': 'Not life-threatening',
        'Mild_ED': 'Not life-threatening',
        'Mild': 'Not life-threatening'
    })

    # Aggregate counts
    df1 = df.groupby('Severity_Type').sum()

    # Format for plotting
    disease_classes = [x.replace('_', ' ').capitalize() for x in df1.columns]
    data = pd.DataFrame({
        'Rare disease class': disease_classes,
        'Life-threatening': df1.loc['Life-threatening'],
        'Not life-threatening': df1.loc['Not life-threatening']
    })

    # Calculate totals and percentages
    data['Total'] = data['Life-threatening'] + data['Not life-threatening']
    data['Life-threatening %'] = (data['Life-threatening'] / data['Total']) * 100
    data['Not life-threatening %'] = (data['Not life-threatening'] / data['Total']) * 100

    # Sort by 'Life-threatening %' descending
    data = data.sort_values(by='Life-threatening %', ascending=False)

    # Melt for seaborn grouped bar plot
    data_melted = data.melt(
        id_vars=["Rare disease class", "Total"],
        value_vars=["Not life-threatening", "Life-threatening"],
        var_name="Severity", value_name="Count"
    )

    # Merge percentages into melted data
    percentage_mapping = {}
    for idx, row in data.iterrows():
        percentage_mapping[(row['Rare disease class'], 'Life-threatening')] = row['Life-threatening %']
        percentage_mapping[(row['Rare disease class'], 'Not life-threatening')] = row['Not life-threatening %']

    data_melted['Percentage'] = data_melted.apply(
        lambda x: percentage_mapping[(x['Rare disease class'], x['Severity'])], axis=1)

    # Plotting
    sns.set(style="white")
    plt.figure(figsize=(16, 14), dpi=600)

    palette = {
        "Life-threatening": "#d55e00",
        "Not life-threatening": "#0072b2"
    }

    # Maintain rare disease order by sorted 'Life-threatening %'
    order = data['Rare disease class'].tolist()

    ax = sns.barplot(
        data=data_melted,
        y="Rare disease class",
        x="Count",
        hue="Severity",
        hue_order=["Not life-threatening", "Life-threatening"],
        palette=palette,
        order=order
    )

    # Set x-axis to log scale
    ax.set_xscale('log')

    # Remove spines and grid
    sns.despine(left=True, bottom=True)

    # Add count and % labels
    for p, (_, row) in zip(ax.patches, data_melted.iterrows()):
        width = p.get_width()
        height = p.get_height()
        y = p.get_y()
        if width > 0:
            count = int(width)
            percentage = row['Percentage']
            label = f'{count} ({percentage:.1f}%)'
            ax.text(width * 1.05, y + height / 2,
                    label,
                    ha='left', va='center', fontsize=15, color='black')

    # Titles and labels
    plt.title("Distribution of Life-threatening vs Not Life-threatening", 
              fontsize=24, pad=20, weight='bold')
    plt.xlabel("Proportion of Patients with Life-threatening Disease per RD Class", fontsize=22, labelpad=15)
    plt.ylabel("", fontsize=16)

    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)

    # Move the legend properly (more space on the right)
    plt.legend(
        title="", fontsize=16, loc='center left', bbox_to_anchor=(1.0, 0.05), borderaxespad=0.
    )

    # Tight layout adjustment
    plt.tight_layout(rect=[0, 0, 0.95, 1])

    plt.show()

    return data.drop(columns=['Total'])  # Cleaner output table
"""

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b239d757-10b0-4220-be17-42f029bc9656"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace, when

def Summary_table_data(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort
    # Vaccination status defined if person has taken atleast 1 dose of vaccine
    df = df.withColumn('vaccination_status', when(df.Covid_vaccine_dose >= 1,1).otherwise(0)) 
    # merge mild_liver_disease & severe_liver_disease in to binary columns liver_disease
    df = df.withColumn("liver_disease", when((df.MILD_liver == 1) | (df.Moderat_severe_liver == 1), 1).otherwise(0))

    # merge Malignant_cancer & Metastatic_solid_tumor_cancer in to binary columns cancer
    df = df.withColumn("Cancer", when((df.Malignant_cancer == 1) | (df.Metastatic_solid_tumor_cancer == 1), 1).otherwise(0))

    # merge DMCx_before & Diabetes_uncomplicated in to binary columns Diabetes
    df = df.withColumn("Diabetes", when((df.DMCx_before == 1) | (df.Diabetes_uncomplicated == 1), 1).otherwise(0))

    df = df.withColumn("life_thretening",
                     when(col("Severity_Type")=='Death', 1)
                    .when(col("Severity_Type")=='Severe', 1)
                    .otherwise(0))

    df = df.withColumn("antiviral_treatment",
                     when(col("Paxlovid")== 1, 1)
                    .when(col("Molnupiravir")==1, 1)
                    .otherwise(0))

    # Add a new column 'Age_bin' based on the binning conditions
    df = df.withColumn('Age_bin', 
                    when((df.Age > 1) & (df.Age <= 20), '1-20')
                    .when((df.Age > 20) & (df.Age <= 40), '20-40')
                    .when((df.Age > 40) & (df.Age <= 65), '40-65')
                    .otherwise('>65'))

    return df.select("hospitalized","Age_bin","BMI_category","Gender","Race","Ethnicity","smoking_status","vaccination_status","antiviral_treatment","COVID_reinfection","long_covid","Cancer","Cardiomyophaties","Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes","Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction","Peripheral_vascular_disease","Rheumatologic_disease","rare_disease","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease")  

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f95543b8-6fbf-4bdd-a32f-7b36289cde2d"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace, when

def Summary_table_data_LT(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort
    # Vaccination status defined if person has taken atleast 1 dose of vaccine
    df = df.withColumn('vaccination_status', when(df.Covid_vaccine_dose >= 1,1).otherwise(0)) 
    # merge mild_liver_disease & severe_liver_disease in to binary columns liver_disease
    df = df.withColumn("liver_disease", when((df.MILD_liver == 1) | (df.Moderat_severe_liver == 1), 1).otherwise(0))

    # merge Malignant_cancer & Metastatic_solid_tumor_cancer in to binary columns cancer
    df = df.withColumn("Cancer", when((df.Malignant_cancer == 1) | (df.Metastatic_solid_tumor_cancer == 1), 1).otherwise(0))

    # merge DMCx_before & Diabetes_uncomplicated in to binary columns Diabetes
    df = df.withColumn("Diabetes", when((df.DMCx_before == 1) | (df.Diabetes_uncomplicated == 1), 1).otherwise(0))

    df = df.withColumn("life_thretening",
                     when(col("Severity_Type")=='Death', 1)
                    .when(col("Severity_Type")=='Severe', 1)
                    .otherwise(0))

    df = df.withColumn("antiviral_treatment",
                     when(col("Paxlovid")== 1, 1)
                    .when(col("Molnupiravir")==1, 1)
                    .otherwise(0)) 

    # Add a new column 'Age_bin' based on the binning conditions
    df = df.withColumn('Age_bin', 
                    when((df.Age > 1) & (df.Age <= 20), '1-20')
                    .when((df.Age > 20) & (df.Age <= 40), '20-40')
                    .when((df.Age > 40) & (df.Age <= 65), '40-65')
                    .otherwise('>65'))

    return df.select("life_thretening","Age_bin","BMI_category","Gender","Race","Ethnicity","smoking_status","vaccination_status","antiviral_treatment","COVID_reinfection","long_covid","Cancer","Cardiomyophaties","Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes","Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction","Peripheral_vascular_disease","Rheumatologic_disease","rare_disease","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease")  

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.da5ffbf3-8c22-4251-a22c-dcbda7e84a8f"),
    rd_representation_investigation=Input(rid="ri.foundry.main.dataset.b17fd6fa-fee3-4424-844f-6298ebf774b3")
)

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace, when
def Vaccination_antiviral_cohort_data(rd_representation_investigation):
    df = rd_representation_investigation
    # patients considered if their post covid observation more than 60 days
    df = df.where(df.observation_period_post_covid > 60)
     # Subcohort build from 23rd December 2021, because antiviral treatment approval
    df_omicron = df.where(df.COVID_first_poslab_or_diagnosis_date >= '2021-12-23') 
    # Better vaccination and anitivital treatment sites
    l = ['526','726','793','294','406','439','819','507','569','207','828','806','399','688','23','134','183','524','77','362','798'] 

    # filter out records by data prtner ids by list l
    df = df_omicron.filter(df_omicron.data_partner_id.isin(l))

    return df.select( "rare_abdominal_surgical_diseases","rare_bone_diseases","rare_cardiac_diseases","rare_circulatory_system_disease","rare_developmental_defect_during_embryogenesis","rare_disorder_due_to_toxic_effects","rare_endocrine_disease","rare_gastroenterologic_disease","rare_gynecologic_or_obstetric_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_maxillo_facial_surgical_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_odontologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_surgical_thoracic_disease","rare_systemic_or_rheumatologic_disease","rare_transplantation_disease","rare_urogenital_disease","hospitalized","life_thretening", "death", "Covid_vaccine_dose", "COVID_reinfection", "long_covid", "Paxlovid", "Molnupiravir", "Bebtelovimab", "selected_antiviral_treatment")

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ec5e20c1-fecf-4a65-8727-bef46134232e"),
    Rd_class_with_covid=Input(rid="ri.foundry.main.dataset.ed69046c-c721-4711-a24d-9b9bc5decd01")
)

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import pandas as pd
import numpy as np

def covid_cci_score_with_binning(Rd_class_with_covid):
    df = Rd_class_with_covid.select(
        'person_id',
        "MYOCARDIALINFARCTION_before_covid_indicator",
        "CONGESTIVEHEARTFAILURE_before_covid_indicator",
        "PERIPHERALVASCULARDISEASE_before_covid_indicator",
        "CEREBROVASCULARDISEASE_before_covid_indicator",
        "DEMENTIA_before_covid_indicator",
        "CHRONICLUNGDISEASE_before_covid_indicator",
        "RHEUMATOLOGICDISEASE_before_covid_indicator",
        "PEPTICULCER_before_covid_indicator",
        "MILDLIVERDISEASE_before_covid_indicator",
        "MODERATESEVERELIVERDISEASE_before_covid_indicator",
        "DIABETESCOMPLICATED_before_covid_indicator",
        "DIABETESUNCOMPLICATED_before_covid_indicator",
        "HEMIPLEGIAORPARAPLEGIA_before_covid_indicator",
        "KIDNEYDISEASE_before_covid_indicator",
        "METASTATICSOLIDTUMORCANCERS_before_covid_indicator",
        "HIVINFECTION_before_covid_indicator"
    ).withColumnRenamed('MYOCARDIALINFARCTION_before_covid_indicator','MYOCARDIALINFARCTION')\
     .withColumnRenamed('CONGESTIVEHEARTFAILURE_before_covid_indicator','CONGESTIVEHEARTFAILURE')\
     .withColumnRenamed('PERIPHERALVASCULARDISEASE_before_covid_indicator', 'PERIPHERALVASCULARDISEASE')\
     .withColumnRenamed('CEREBROVASCULARDISEASE_before_covid_indicator','CEREBROVASCULARDISEASE')\
     .withColumnRenamed('DEMENTIA_before_covid_indicator','DEMENTIA')\
     .withColumnRenamed('CHRONICLUNGDISEASE_before_covid_indicator','Chronic_lung_disease')\
     .withColumnRenamed('RHEUMATOLOGICDISEASE_before_covid_indicator','Rheumatologic_disease')\
     .withColumnRenamed('PEPTICULCER_before_covid_indicator','PEPTICULCER')\
     .withColumnRenamed('MILDLIVERDISEASE_before_covid_indicator','MILD_liver')\
     .withColumnRenamed('MODERATESEVERELIVERDISEASE_before_covid_indicator','Moderat_severe_liver')\
     .withColumnRenamed('DIABETESCOMPLICATED_before_covid_indicator','DMCx_before')\
     .withColumnRenamed("DIABETESUNCOMPLICATED_before_covid_indicator",'Diabetes_uncomplicated')\
     .withColumnRenamed('HEMIPLEGIAORPARAPLEGIA_before_covid_indicator','Hemiplegia_or_paraplegia')\
     .withColumnRenamed('KIDNEYDISEASE_before_covid_indicator','Kidney_disease')\
     .withColumnRenamed('METASTATICSOLIDTUMORCANCERS_before_covid_indicator','Metastatic_solid_tumor_cancer')\
     .withColumnRenamed('HIVINFECTION_before_covid_indicator','HIV_infection')

    # Score calculation
    df = df.withColumn('CCI_score', (
        F.col('MYOCARDIALINFARCTION') * 1 +
        F.col('CONGESTIVEHEARTFAILURE') * 1 +
        F.col('PERIPHERALVASCULARDISEASE') * 1 +
        F.col('CEREBROVASCULARDISEASE') * 1 +
        F.col('DEMENTIA') * 1 +
        F.col('Chronic_lung_disease') * 1 +
        F.col('Rheumatologic_disease') * 1 +
        F.col('PEPTICULCER') * 1 +
        F.when(F.col('Moderat_severe_liver') == 1, 3).otherwise(F.col('MILD_liver') * 1) +
        F.when(F.col('DMCx_before') == 1, 2).otherwise(F.col('Diabetes_uncomplicated') * 1) +
        F.col('Hemiplegia_or_paraplegia') * 2 +
        F.col('Kidney_disease') * 2 +
        F.when(F.col('Metastatic_solid_tumor_cancer') == 1, 6).otherwise(0) +
        F.col('HIV_infection') * 6
    ).cast(IntegerType()))

    # Convert to pandas to bin CCI score
    df_pd = df.select('person_id', 'CCI_score').toPandas()

    conditions = [
        df_pd['CCI_score'] == 0,
        df_pd['CCI_score'].between(1, 2),
        df_pd['CCI_score'].between(3, 4),
        df_pd['CCI_score'] >= 5
    ]
    choices = ['no_comorbidities', 'mild', 'moderate', 'severe']
    df_pd['CCI'] = np.select(conditions, choices, default='Unknown')

    return df_pd

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3afa9443-5f2b-4827-b7df-db12dde270d7"),
    Enriched_Death_Table=Input(rid="ri.foundry.main.dataset.b07b1c8f-97a7-43b4-826a-299e456c6e85")
)

def death_data_table(Enriched_Death_Table):
    df =Enriched_Death_Table.select("person_id", "death_date")
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bd9f57b4-c9e5-48d6-ad2b-f3dcdb2f6099"),
    death_data_table=Input(rid="ri.foundry.main.dataset.3afa9443-5f2b-4827-b7df-db12dde270d7"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)
from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff, row_number
from pyspark.sql.window import Window

def long_covid_dataframe_partial_vacc_new( death_data_table, partial_fully_vaccinated_data):
    df1 = partial_fully_vaccinated_data
    df2 = death_data_table

    # Join death data
    df = df1.join(df2, on=['person_id'], how='left')
    df = df.where(df.observation_period_post_covid > 90) # replacing 60 days

    # Calculate days between COVID diagnosis and death
    df = df.withColumn("date_diff", datediff(col("death_date"), col("COVID_first_poslab_or_diagnosis_date")))

    # Filter those who died within 90 days of COVID diagnosis (CDC condition after 90 days)
    filtered_df = df.filter(col("date_diff") <= 90)

    # Remove people who died within 45 days
    df = df.join(filtered_df, on=['person_id'], how='left_anti')

    # Define window to keep only the earliest COVID diagnosis per person
    window_spec = Window.partitionBy("person_id").orderBy("COVID_first_poslab_or_diagnosis_date")

    # Assign row number based on earliest diagnosis date
    df = df.withColumn("row_num", row_number().over(window_spec))

    # Keep only the first row per person_id
    df = df.filter(col("row_num") == 1).drop("row_num")

    return df

"""
from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff
def long_covid_dataframe_partial_vacc_new(partial_fully_vaccinated_data, death_data_table):
    df1 = partial_fully_vaccinated_data
    df2 = death_data_table
    # join dataframe df2 on df1
    df = df1.join(df2, on=['person_id'], how='left')

    # Calculate date difference
    df = df.withColumn("date_diff", datediff(col("death_date"), col("COVID_first_poslab_or_diagnosis_date")))

    # Filter rows where date difference is <= 45 days
    filtered_df = df.filter(col("date_diff") <= 45)

    # drop filtered_df person id (which is person died within 45 days to covid-19 diagnosis date)
    df = df.join(filtered_df, on=['person_id'], how='left_anti')

    return df
    """

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8efc36cf-404e-457d-bca0-78b17d49f71c"),
    test_multiVar_rds_models_hosp_new=Input(rid="ri.vector.main.execute.df5f8eb9-8b4e-43b7-8a57-4291e165535b"),
    test_multiVar_rds_models_lt_new=Input(rid="ri.vector.main.execute.1bc5c860-a948-4057-85ec-8428aac19443")
)
## THis dataframe is combining output of rare disease (life-threatening + Hospitalization )

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, round, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def multivariate_for_radial_plot_spark_test(test_multiVar_rds_models_lt_new, test_multiVar_rds_models_hosp_new):
    df1 = test_multiVar_rds_models_lt_new.select('rare_disease', 'odds_ratio', 'ci_lower', 'ci_upper')
    df2 = test_multiVar_rds_models_hosp_new.select('rare_disease', 'odds_ratio', 'ci_lower', 'ci_upper')

    # Add 'Severity' column
    df_lt = df1.withColumn("Severity", lit("life_threatening"))
    df_hosp = df2.withColumn("Severity", lit("hospitalized"))

    # Marker rows as a Spark DataFrame
    marker_data = [
        ("odds_mark", 1.0, 1.0, 1.0, "life_threatening"),
        ("odds_mark", 1.0, 1.0, 1.0, "hospitalized")
    ]
    
    marker_schema = StructType([
        StructField("rare_disease", StringType(), True),
        StructField("odds_ratio", DoubleType(), True),
        StructField("ci_lower", DoubleType(), True),
        StructField("ci_upper", DoubleType(), True),
        StructField("Severity", StringType(), True)
    ])
    
    marker_df = spark.createDataFrame(marker_data, schema=marker_schema)

    # Combine all data
    combined_df = df_hosp.unionByName(df_lt).unionByName(marker_df)

    # Replacement dictionary
    replacement_dict = {
        "odds_mark": "OR",
        "rare_bone_diseases": "RBD",
        "rare_cardiac_diseases": "RCD",
        "rare_developmental_defect_during_embryogenesis": "RDDDE",
        "rare_endocrine_disease": "RED",
        "rare_gastroenterologic_disease": "RGD",
        "rare_hematologic_disease": "RHD",
        "rare_hepatic_disease": "RHEPD",
        "rare_immune_disease": "RID",
        "rare_inborn_errors_of_metabolism": "RIEM",
        "rare_infectious_disease": "RINFD",
        "rare_neoplastic_disease": "RND",
        "rare_neurologic_disease": "RNUED",
        "rare_ophthalmic_disorder": "ROD",
        "rare_otorhinolaryngologic_disease": "ROTORD",
        "rare_renal_disease": "RRD",
        "rare_respiratory_disease": "RRESD",
        "rare_skin_disease": "RSD",
        "rare_systemic_or_rheumatologic_disease": "RSRD"
    }

    # Apply replacements using when-otherwise
    rare_disease_col = col("rare_disease")
    replacement_expr = when(rare_disease_col == list(replacement_dict.keys())[0], replacement_dict[list(replacement_dict.keys())[0]])
    for k, v in list(replacement_dict.items())[1:]:
        replacement_expr = replacement_expr.when(rare_disease_col == k, v)
    replacement_expr = replacement_expr.otherwise(rare_disease_col)

    # Apply updated column and rounding
    final_df = combined_df \
        .withColumn("rare_disease", replacement_expr) \
        .withColumn("odds_ratio", round(col("odds_ratio"), 2)) \
        .withColumn("ci_lower", round(col("ci_lower"), 2)) \
        .withColumn("ci_upper", round(col("ci_upper"), 2))

    return final_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4b0ef14f-2863-4691-9715-eb0e27db3e6c"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace, when
def new_cohort_for_sensiity_analysis(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort
    # Here cutoff data considered 23rd December 2021 because FDA/EUA drugs approval time
    df_omicron = df.where(df.COVID_first_poslab_or_diagnosis_date >= '2021-01-01') 
#    df = df_omicron
    # better vaccine & antiviral sites (site with minimum 20 % vaccination & minimum 10 antiviral)
    l = ['362','183','406','688','806','507','207','819','23','524','77','569','793','526','798','828','75','726','198','294','399','134']

    # filter out records by data prtner ids by list l
    df = df_omicron.filter(df_omicron.data_partner_id.isin(l))
    
    # Encoding object type features into binary features
    bmi_categories = df.select('BMI_category').distinct().rdd.flatMap(lambda x: x).collect()
    gender_categories = df.select('Gender').distinct().rdd.flatMap(lambda x: x).collect()
    ethnicity_categories = df.select('Ethnicity').distinct().rdd.flatMap(lambda x: x).collect()
    race_categories = df.select('Race').distinct().rdd.flatMap(lambda x: x).collect()
    smoking_status_categories = df.select('smoking_status').distinct().rdd.flatMap(lambda x: x).collect()

    bmi_exprs = [F.when(F.col('BMI_category') == cat, 1).otherwise(0).alias(f"{cat}") for cat in bmi_categories]
    gender_exprs = [F.when(F.col('Gender') == cat, 1).otherwise(0).alias(f"gender_{cat}") for cat in gender_categories]
    ethnicity_exprs = [F.when(F.col('Ethnicity') == cat, 1).otherwise(0).alias(f"ethnicity_{cat}") for cat in ethnicity_categories]
    race_exprs = [F.when(F.col('Race') == cat, 1).otherwise(0).alias(f"race_{cat}") for cat in race_categories]
    smoking_status_exprs = [F.when(F.col('smoking_status') == cat, 1).otherwise(0).alias(f"smoking_status_{cat}") for cat in smoking_status_categories]

    df2 = df.select(bmi_exprs + gender_exprs + race_exprs + ethnicity_exprs + smoking_status_exprs + df.columns)
#    cols_to_drop = ["Gender", "Race", "Ethnicity","smoking_status"]
#    df2 = df.drop(*cols_to_drop)

    # merge mild_liver_disease & severe_liver_disease in to binary columns liver_disease
    df2 = df2.withColumn("liver_disease", when((df2.MILD_liver == 1) | (df2.Moderat_severe_liver == 1), 1).otherwise(0))

    # merge Malignant_cancer & Metastatic_solid_tumor_cancer in to binary columns cancer
    df2 = df2.withColumn("Cancer", when((df2.Malignant_cancer == 1) | (df2.Metastatic_solid_tumor_cancer == 1), 1).otherwise(0))

    # merge DMCx_before & Diabetes_uncomplicated in to binary columns Diabetes
    df2 = df2.withColumn("Diabetes", when((df2.DMCx_before == 1) | (df2.Diabetes_uncomplicated == 1), 1).otherwise(0))

    df2 = df2.withColumn("life_thretening",
                     when(col("Severity_Type")=='Death', 1)
                    .when(col("Severity_Type")=='Severe', 1)
                    .otherwise(0))

    df2 = df2.withColumn("antiviral_treatment",
                     when(col("Paxlovid")== 1, 1)
                    .when(col("Molnupiravir")==1, 1)
                    .otherwise(0))

    df2 = df2.withColumn('Age_bin', 
                    when((df2.Age > 1) & (df2.Age <= 20), '1-20')
                    .when((df2.Age > 20) & (df2.Age <= 40), '20-40')
                    .when((df2.Age > 40) & (df2.Age <= 65), '40-65')
                    .otherwise('>65'))

    df2 = df2.drop("MILD_liver","Moderat_severe_liver", "Malignant_cancer", "Metastatic_solid_tumor_cancer", "DMCx_before", "Diabetes_uncomplicated","Severity_Type")

    return df2

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8b3fcb9f-22e9-4d5d-a984-2927911b78cd"),
    death_data_table=Input(rid="ri.foundry.main.dataset.3afa9443-5f2b-4827-b7df-db12dde270d7"),
    new_partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.f157a6ca-e647-431b-bafe-3bf59471fc8a")
)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff, row_number
from pyspark.sql.window import Window

def new_covid_reinfection_partial_vacc_antiv_data(new_partial_fully_vaccinated_data, death_data_table):
    df1 = new_partial_fully_vaccinated_data
    df2 = death_data_table
    # join dataframe df2 on df1
    df = df1.join(df2, on=['person_id'], how='left')
    df = df.where(df.observation_period_post_covid > 60) # replacing 60 days
    # Calculate date difference
    df = df.withColumn("date_diff", datediff(col("death_date"), col("COVID_first_poslab_or_diagnosis_date")))

    # Filter rows where date difference is <= 60 days
    filtered_df = df.filter(col("date_diff") <= 60)

    # drop filtered_df person id (which is person died within 60 days to covid-19 diagnosis date)
    df = df.join(filtered_df, on=['person_id'], how='left_anti')

    # Define window to keep only the earliest COVID diagnosis per person
    window_spec = Window.partitionBy("person_id").orderBy("COVID_first_poslab_or_diagnosis_date")

    # Assign row number based on earliest diagnosis date
    df = df.withColumn("row_num", row_number().over(window_spec))

    # Keep only the first row per person_id
    df = df.filter(col("row_num") == 1).drop("row_num")

    return df    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f157a6ca-e647-431b-bafe-3bf59471fc8a"),
    new_two_dose_vaccine=Input(rid="ri.foundry.main.dataset.425c0af2-13bb-4ea0-938f-3afd7bde6984"),
    new_unvaccinated_subcohort=Input(rid="ri.foundry.main.dataset.6e55ac70-b994-4167-ac7c-ef54258e18c7")
)

"""
Here partially vaccinated dataframe means single vaccine dose before 14 days to COVID-19 diagosis date and dropping rows if concomitant drug treatment happen during antiviral treatment
"""
from pyspark.sql import functions as F
def new_partial_fully_vaccinated_data(new_unvaccinated_subcohort, new_two_dose_vaccine):
    one_dose_vaccine = new_two_dose_vaccine
    concatenated_df = new_unvaccinated_subcohort.unionAll(one_dose_vaccine)
#    # Drop rows where person_id is in the Concomitant_treatment DataFrame
#    filtered_df = concatenated_df.join(
#        Concomitant_treatment.select("person_id"), 
#        on="person_id", 
#        how="left_anti"
#    )   
    return concatenated_df 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.425c0af2-13bb-4ea0-938f-3afd7bde6984"),
    Vaccine_fact_lds=Input(rid="ri.foundry.main.dataset.7482e426-55a2-4a0b-9976-4cb3aa35788d"),
    new_cohort_for_sensiity_analysis=Input(rid="ri.foundry.main.dataset.4b0ef14f-2863-4691-9715-eb0e27db3e6c")
)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff
from pyspark.sql.functions import lit

def new_two_dose_vaccine(Vaccine_fact_lds, new_cohort_for_sensiity_analysis):
    df1 = new_cohort_for_sensiity_analysis
    df2 = Vaccine_fact_lds.select("person_id","vaccine_txn","first_vax_date","2_vax_date")
    df = df1.join(df2, on=['person_id'], how='left')
    df = df.filter(df.vaccine_txn >=2)
    df = df.filter(df["2_vax_date"].isNotNull())
    # Filter the DataFrame for vaccination 14 days prior to covid-19 diagnosis date 
    df_filtered = df.filter(datediff(col("COVID_first_poslab_or_diagnosis_date"), col("2_vax_date")) >= 14)
    df_filtered = df_filtered.withColumn("vaccination_status", lit(1))
    return df_filtered.select("person_id","observation_period_post_covid","COVID_first_poslab_or_diagnosis_date","life_thretening","death","hospitalized","vaccination_status","Paxlovid","Molnupiravir","antiviral_treatment","COVID_reinfection","long_covid","age_1_20","age_20_40","age_40_65","age_65_above","Age_bin","BMI_category","BMI_obese","BMI_over_weight","BMI_normal","BMI_under_weight","Gender", "Race", "Ethnicity","smoking_status","gender_MALE","gender_FEMALE","race_Black_or_African_American","race_Unknown","race_White","race_Asian","ethnicity_Not_Hispanic_or_Latino","ethnicity_Hispanic_or_Latino","ethnicity_Unknown","smoking_status_Non_smoker","smoking_status_Current_or_Former","Cancer","Cardiomyophaties","Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes","Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction","Peripheral_vascular_disease","Rheumatologic_disease","Systemic_corticosteroids","rare_disease","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease")

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6e55ac70-b994-4167-ac7c-ef54258e18c7"),
    Vaccine_fact_lds=Input(rid="ri.foundry.main.dataset.7482e426-55a2-4a0b-9976-4cb3aa35788d"),
    new_cohort_for_sensiity_analysis=Input(rid="ri.foundry.main.dataset.4b0ef14f-2863-4691-9715-eb0e27db3e6c")
)

from pyspark.sql import functions as F
from pyspark.sql.functions import lit

def new_unvaccinated_subcohort(new_cohort_for_sensiity_analysis, Vaccine_fact_lds):
    df1 = new_cohort_for_sensiity_analysis
    df1 = df1.filter(df1.Covid_vaccine_dose == 0)
    df2 = Vaccine_fact_lds.select("person_id")
    # Perform a left anti join to filter out the common person_id of vaccinated from df1 dataframe to have unvaccinated (control)
    df1_filtered = df1.join(df2, df1.person_id == df2.person_id, "left_anti")
    df1_filtered = df1_filtered.withColumn("vaccination_status", lit(0))
    return df1_filtered.select("person_id","observation_period_post_covid","COVID_first_poslab_or_diagnosis_date","life_thretening","death","hospitalized","vaccination_status","Paxlovid","Molnupiravir","antiviral_treatment","COVID_reinfection","long_covid","age_1_20","age_20_40","age_40_65","age_65_above","Age_bin","BMI_category","BMI_obese","BMI_over_weight","BMI_normal","BMI_under_weight","Gender", "Race", "Ethnicity","smoking_status","gender_MALE","gender_FEMALE","race_Black_or_African_American","race_Unknown","race_White","race_Asian","ethnicity_Not_Hispanic_or_Latino","ethnicity_Hispanic_or_Latino","ethnicity_Unknown","smoking_status_Non_smoker","smoking_status_Current_or_Former","Cancer","Cardiomyophaties","Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes","Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction","Peripheral_vascular_disease","Rheumatologic_disease","Systemic_corticosteroids","rare_disease","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease")

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311"),
    two_dose_vaccine=Input(rid="ri.foundry.main.dataset.59023e70-4a20-4dcd-a704-be55e3005924"),
    unvaccinated_subcohort=Input(rid="ri.foundry.main.dataset.19f41c07-8ccb-4736-b517-e12b83f94d45")
)

"""
Here partially vaccinated dataframe means single vaccine dose before 14 days to COVID-19 diagosis date and dropping rows if concomitant drug treatment happen during antiviral treatment
"""
from pyspark.sql import functions as F
def partial_fully_vaccinated_data(unvaccinated_subcohort, two_dose_vaccine):
    one_dose_vaccine = two_dose_vaccine
    concatenated_df = unvaccinated_subcohort.unionAll(two_dose_vaccine)
#    # Drop rows where person_id is in the Concomitant_treatment DataFrame
#    filtered_df = concatenated_df.join(
#        Concomitant_treatment.select("person_id"), 
#        on="person_id", 
#        how="left_anti"
#    )   
    return concatenated_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.56a84162-780d-4199-a416-28f41120e969"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace, when

def rd_cohort(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort
    # Vaccination status defined if person has taken atleast 1 dose of vaccine
    df = df.withColumn('vaccination_status', when(df.Covid_vaccine_dose >= 1,1).otherwise(0))
    
    # Encoding object type features into binary features
    bmi_categories = df.select('BMI_category').distinct().rdd.flatMap(lambda x: x).collect()
    gender_categories = df.select('Gender').distinct().rdd.flatMap(lambda x: x).collect()
    ethnicity_categories = df.select('Ethnicity').distinct().rdd.flatMap(lambda x: x).collect()
    race_categories = df.select('Race').distinct().rdd.flatMap(lambda x: x).collect()
    smoking_status_categories = df.select('smoking_status').distinct().rdd.flatMap(lambda x: x).collect()
    cci_categories = df.select("CCI").distinct().rdd.flatMap(lambda x: x).collect()

    bmi_exprs = [F.when(F.col('BMI_category') == cat, 1).otherwise(0).alias(f"{cat}") for cat in bmi_categories]
    gender_exprs = [F.when(F.col('Gender') == cat, 1).otherwise(0).alias(f"gender_{cat}") for cat in gender_categories]
    ethnicity_exprs = [F.when(F.col('Ethnicity') == cat, 1).otherwise(0).alias(f"ethnicity_{cat}") for cat in ethnicity_categories]
    race_exprs = [F.when(F.col('Race') == cat, 1).otherwise(0).alias(f"race_{cat}") for cat in race_categories]
    smoking_status_exprs = [F.when(F.col('smoking_status') == cat, 1).otherwise(0).alias(f"smoking_status_{cat}") for cat in smoking_status_categories]
    cci_exprs = [F.when(F.col('CCI') == cat, 1).otherwise(0).alias(f"CCI_{cat}") for cat in cci_categories]    

    df = df.select(bmi_exprs + gender_exprs + race_exprs + ethnicity_exprs + smoking_status_exprs + cci_exprs + df.columns)
    cols_to_drop = ["BMI_category","Gender", "Race", "Ethnicity","smoking_status","CCI"]
    df2 = df.drop(*cols_to_drop)

    # merge mild_liver_disease & severe_liver_disease in to binary columns liver_disease
    df2 = df2.withColumn("liver_disease", when((df2.MILD_liver == 1) | (df2.Moderat_severe_liver == 1), 1).otherwise(0))

    # merge Malignant_cancer & Metastatic_solid_tumor_cancer in to binary columns cancer
    df2 = df2.withColumn("Cancer", when((df2.Malignant_cancer == 1) | (df2.Metastatic_solid_tumor_cancer == 1), 1).otherwise(0))

    # merge DMCx_before & Diabetes_uncomplicated in to binary columns Diabetes
    df2 = df2.withColumn("Diabetes", when((df2.DMCx_before == 1) | (df2.Diabetes_uncomplicated == 1), 1).otherwise(0))

    df2 = df2.withColumn("life_threatening",
                     when(col("Severity_Type")=='Death', 1)
                    .when(col("Severity_Type")=='Severe', 1)
                    .otherwise(0))

    df2 = df2.withColumn("antiviral_treatment",
                     when(col("Paxlovid")== 1, 1)
                    .when(col("Molnupiravir")==1, 1)
                    .when(col("Bebtelovimab")==1, 1)
                    .otherwise(0))

    df2 = df2.drop("MILD_liver","Moderat_severe_liver", "Malignant_cancer", "Metastatic_solid_tumor_cancer", "DMCx_before", "Diabetes_uncomplicated","Severity_Type")

    return df2.select("life_threatening","death","hospitalized","age_1_20","age_20_40","age_40_65","age_65_above","BMI_obese","BMI_over_weight","BMI_normal","BMI_under_weight","gender_MALE","gender_FEMALE","race_Black_or_African_American","race_Unknown","race_White","race_Asian","ethnicity_Not_Hispanic_or_Latino","ethnicity_Hispanic_or_Latino","ethnicity_Unknown","smoking_status_Non_smoker","smoking_status_Current_or_Former","vaccination_status","antiviral_treatment","COVID_reinfection","long_covid","Cancer","Cardiomyophaties","Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes","Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction","Peripheral_vascular_disease","Rheumatologic_disease","rare_disease","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease","Paxlovid","Molnupiravir","Bebtelovimab","CCI_no_comorbidities","CCI_mild","CCI_moderate","CCI_severe") 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6"),
    Covid_drug_treatment=Input(rid="ri.foundry.main.dataset.9b49d62c-05a8-4bfc-8e62-cca5c09b6291"),
    Long_covid_table=Input(rid="ri.foundry.main.dataset.afbeb9be-ec92-4aec-a405-e0969ce20a22"),
    Rd_class_with_covid=Input(rid="ri.foundry.main.dataset.ed69046c-c721-4711-a24d-9b9bc5decd01"),
    covid_cci_score_with_binning=Input(rid="ri.foundry.main.dataset.ec5e20c1-fecf-4a65-8727-bef46134232e")
)

import pandas as pd
import numpy as np
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import mean
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.window import Window
from pyspark.sql.functions import isnan, when, count, col, lit, regexp_replace,mean,desc,row_number, array, array_contains

def rd_covid_lcovid_drugs_cohort(Covid_drug_treatment, Long_covid_table, Rd_class_with_covid, covid_cci_score_with_binning):
    df_drug = Covid_drug_treatment
    df_long_covid = Long_covid_table.select("person_id","long_covid_start_date","long_covid")
    df_cci = covid_cci_score_with_binning.select("person_id","CCI")
    df = Rd_class_with_covid.select("person_id","data_partner_id","COVID_first_PCR_or_AG_lab_positive", "COVID_first_poslab_or_diagnosis_date","observation_period_post_covid","age_at_covid","BMI_max_observed_or_calculated_before_covid","COVID_hospitalization_length_of_stay","COVID_associated_hospitalization_indicator" ,"COVID_patient_death_indicator","number_of_COVID_vaccine_doses_before_covid","had_at_least_one_reinfection_post_covid_indicator","gender_concept_name","ethnicity_concept_name","race_concept_name","smoking_status","Severity_Type", "postal_code","TUBERCULOSIS_before_covid_indicator","MILDLIVERDISEASE_before_covid_indicator","MODERATESEVERELIVERDISEASE_before_covid_indicator","RHEUMATOLOGICDISEASE_before_covid_indicator","DEMENTIA_before_covid_indicator","CONGESTIVEHEARTFAILURE_before_covid_indicator","KIDNEYDISEASE_before_covid_indicator","MALIGNANTCANCER_before_covid_indicator","DIABETESCOMPLICATED_before_covid_indicator","CEREBROVASCULARDISEASE_before_covid_indicator","PERIPHERALVASCULARDISEASE_before_covid_indicator","HEARTFAILURE_before_covid_indicator","HEMIPLEGIAORPARAPLEGIA_before_covid_indicator",        
"PSYCHOSIS_before_covid_indicator","OBESITY_before_covid_indicator","CORONARYARTERYDISEASE_before_covid_indicator","SYSTEMICCORTICOSTEROIDS_before_covid_indicator","DEPRESSION_before_covid_indicator",   
"METASTATICSOLIDTUMORCANCERS_before_covid_indicator","HIVINFECTION_before_covid_indicator","CHRONICLUNGDISEASE_before_covid_indicator","PEPTICULCER_before_covid_indicator","MYOCARDIALINFARCTION_before_covid_indicator","DIABETESUNCOMPLICATED_before_covid_indicator","CARDIOMYOPATHIES_before_covid_indicator", "HYPERTENSION_before_covid_indicator","TOBACCOSMOKER_before_covid_indicator","REMDISIVIR_during_covid_hospitalization_indicator",'rare_abdominal_surgical_diseases','rare_bone_diseases','rare_cardiac_diseases','rare_circulatory_system_disease','rare_developmental_defect_during_embryogenesis','rare_disorder_due_to_toxic_effects','rare_endocrine_disease','rare_gastroenterologic_disease','rare_gynecologic_or_obstetric_disease','rare_hematologic_disease','rare_hepatic_disease','rare_immune_disease','rare_inborn_errors_of_metabolism','rare_infectious_disease','rare_maxillo_facial_surgical_disease','rare_neoplastic_disease','rare_neurologic_disease','rare_odontologic_disease','rare_ophthalmic_disorder','rare_otorhinolaryngologic_disease','rare_renal_disease','rare_respiratory_disease','rare_skin_disease','rare_surgical_thoracic_disease','rare_systemic_or_rheumatologic_disease','rare_transplantation_disease','rare_urogenital_disease')

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

    binary_cols = [
                    "rare_urogenital_disease",
                    "rare_maxillo_facial_surgical_disease",
                    "rare_surgical_thoracic_disease",
                    "rare_odontologic_disease",
                    "rare_abdominal_surgical_diseases",
                    "rare_disorder_due_to_toxic_effects",
                    "rare_gynecologic_or_obstetric_disease",
                    "rare_circulatory_system_disease",
                    "rare_transplantation_disease"] 

    # Create a new column that is an array of the binary columns
    df = df.withColumn("binary_array", array(*binary_cols))

    # Create a new column that is 1 if the array contains 1, and 0 otherwise
    df = df.withColumn("other_rare_diseases", array_contains("binary_array", 1).cast("int"))

    # Now drop patients with following 9 rare disease classes
    df = df.where(F.col('other_rare_diseases') == 0)

    # Drop the intermediate column 
    df = df.drop(
                 "rare_urogenital_disease",
                 "rare_maxillo_facial_surgical_disease",
                 "rare_surgical_thoracic_disease",
                 "rare_odontologic_disease",
                 "rare_abdominal_surgical_diseases",
                 "rare_disorder_due_to_toxic_effects",
                 "rare_gynecologic_or_obstetric_disease",
                 "rare_circulatory_system_disease",
                 "rare_transplantation_disease")

    df1 = df.join(df_long_covid, on = ['person_id'], how = 'left')
    df1 = df1.join(df_cci, on = ['person_id'], how = 'left')
    df_final = df1.join(df_drug, on = ['person_id'], how = 'left')
    # replace Nan values with 0 ("Bebtelovimab",)
    columns_to_fill = ["long_covid", "Paxlovid", "Molnupiravir", "selected_antiviral_treatment"]
    df_final = df_final.na.fill(0, subset=columns_to_fill)

    # Create a window specification for ordering by date
    window_spec = Window.partitionBy("person_id").orderBy(desc("COVID_first_poslab_or_diagnosis_date"))

    # Add a rank column to identify the latest date per person_id
    df_ranked = df_final.withColumn("rank", row_number().over(window_spec))

    # Select rows with rank = 1 (latest date)
    result_df = df_ranked.where(col("rank") == 1).drop("rank")

    # Define a function to fill null values with the column mean
    def fillna_mean(df, include=set()):
        means = df.agg(*(mean(x).alias(x) for x in df.columns if x in include))
        return df.na.fill(means.first().asDict())

    # Specify the columns you want to impute (e.g., "BMI")
    columns_to_impute = ["BMI"]

    # Apply the imputation function to the DataFrame
    result_df = fillna_mean(result_df, include=columns_to_impute)

    # Define BMI categories
    result_df = result_df.withColumn("BMI_category", when(col("BMI") < 18.5, "BMI_under_weight")
                        .when((col("BMI") >= 18.5) & (col("BMI") < 25), "BMI_normal")
                        .when((col("BMI") >= 25) & (col("BMI") < 30), "BMI_over_weight")
                        .otherwise("BMI_obese"))
    
    return result_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b17fd6fa-fee3-4424-844f-6298ebf774b3"),
    Covid_drug_treatment=Input(rid="ri.foundry.main.dataset.9b49d62c-05a8-4bfc-8e62-cca5c09b6291"),
    Long_covid_table=Input(rid="ri.foundry.main.dataset.afbeb9be-ec92-4aec-a405-e0969ce20a22"),
    Rd_class_with_covid=Input(rid="ri.foundry.main.dataset.ed69046c-c721-4711-a24d-9b9bc5decd01")
)

import pandas as pd
import numpy as np
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import mean
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.window import Window
from pyspark.sql.functions import isnan, when, count, col, lit, regexp_replace,mean,desc,row_number, array, array_contains

def rd_representation_investigation(Covid_drug_treatment, Long_covid_table, Rd_class_with_covid):
    df_drug = Covid_drug_treatment
    df_long_covid = Long_covid_table.select("person_id","long_covid_start_date","long_covid")
    df = Rd_class_with_covid.select("person_id","data_partner_id","COVID_first_PCR_or_AG_lab_positive", "COVID_first_poslab_or_diagnosis_date","observation_period_post_covid","age_at_covid","BMI_max_observed_or_calculated_before_covid","COVID_hospitalization_length_of_stay","COVID_associated_hospitalization_indicator" ,"COVID_patient_death_indicator","number_of_COVID_vaccine_doses_before_covid","had_at_least_one_reinfection_post_covid_indicator","gender_concept_name","ethnicity_concept_name","race_concept_name","smoking_status","Severity_Type", "postal_code","TUBERCULOSIS_before_covid_indicator","MILDLIVERDISEASE_before_covid_indicator","MODERATESEVERELIVERDISEASE_before_covid_indicator","RHEUMATOLOGICDISEASE_before_covid_indicator","DEMENTIA_before_covid_indicator","CONGESTIVEHEARTFAILURE_before_covid_indicator","KIDNEYDISEASE_before_covid_indicator","MALIGNANTCANCER_before_covid_indicator","DIABETESCOMPLICATED_before_covid_indicator","CEREBROVASCULARDISEASE_before_covid_indicator","PERIPHERALVASCULARDISEASE_before_covid_indicator","HEARTFAILURE_before_covid_indicator","HEMIPLEGIAORPARAPLEGIA_before_covid_indicator",        
"PSYCHOSIS_before_covid_indicator","OBESITY_before_covid_indicator","CORONARYARTERYDISEASE_before_covid_indicator","SYSTEMICCORTICOSTEROIDS_before_covid_indicator","DEPRESSION_before_covid_indicator",   
"METASTATICSOLIDTUMORCANCERS_before_covid_indicator","HIVINFECTION_before_covid_indicator","CHRONICLUNGDISEASE_before_covid_indicator","PEPTICULCER_before_covid_indicator","MYOCARDIALINFARCTION_before_covid_indicator","DIABETESUNCOMPLICATED_before_covid_indicator","CARDIOMYOPATHIES_before_covid_indicator", "HYPERTENSION_before_covid_indicator","TOBACCOSMOKER_before_covid_indicator","REMDISIVIR_during_covid_hospitalization_indicator",'rare_abdominal_surgical_diseases','rare_bone_diseases','rare_cardiac_diseases','rare_circulatory_system_disease','rare_developmental_defect_during_embryogenesis','rare_disorder_due_to_toxic_effects','rare_endocrine_disease','rare_gastroenterologic_disease','rare_gynecologic_or_obstetric_disease','rare_hematologic_disease','rare_hepatic_disease','rare_immune_disease','rare_inborn_errors_of_metabolism','rare_infectious_disease','rare_maxillo_facial_surgical_disease','rare_neoplastic_disease','rare_neurologic_disease','rare_odontologic_disease','rare_ophthalmic_disorder','rare_otorhinolaryngologic_disease','rare_renal_disease','rare_respiratory_disease','rare_skin_disease','rare_surgical_thoracic_disease','rare_systemic_or_rheumatologic_disease','rare_transplantation_disease','rare_urogenital_disease')

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

    df1 = df.join(df_long_covid, on = ['person_id'], how = 'left')
    df_final = df1.join(df_drug, on = ['person_id'], how = 'left')
    # replace Nan values with 0
    columns_to_fill = ["long_covid", "Paxlovid", "Molnupiravir", "Bebtelovimab", "selected_antiviral_treatment"]
    df_final = df_final.na.fill(0, subset=columns_to_fill)

    # Create a window specification for ordering by date
    window_spec = Window.partitionBy("person_id").orderBy(desc("COVID_first_poslab_or_diagnosis_date"))

    # Add a rank column to identify the latest date per person_id
    df_ranked = df_final.withColumn("rank", row_number().over(window_spec))

    # Select rows with rank = 1 (latest date)
    result_df = df_ranked.where(col("rank") == 1).drop("rank")

    # Define a function to fill null values with the column mean
    def fillna_mean(df, include=set()):
        means = df.agg(*(mean(x).alias(x) for x in df.columns if x in include))
        return df.na.fill(means.first().asDict())

    # Specify the columns you want to impute (e.g., "BMI")
    columns_to_impute = ["BMI"]

    # Apply the imputation function to the DataFrame
    result_df = fillna_mean(result_df, include=columns_to_impute)

    # Define BMI categories
    result_df = result_df.withColumn("BMI_category", when(col("BMI") < 18.5, "BMI_under_weight")
                        .when((col("BMI") >= 18.5) & (col("BMI") < 25), "BMI_normal")
                        .when((col("BMI") >= 25) & (col("BMI") < 30), "BMI_over_weight")
                        .otherwise("BMI_obese"))
                        
    result_df = result_df.withColumn("life_thretening",
                     when(col("Severity_Type")=='Death', 1)
                    .when(col("Severity_Type")=='Severe', 1)
                    .otherwise(0))
    
    return result_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.feea9a2d-0f2b-4b46-bc5c-6ffaeef42f45"),
    rd_representation_investigation=Input(rid="ri.foundry.main.dataset.b17fd6fa-fee3-4424-844f-6298ebf774b3")
)
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

def rd_severity_frequencies(rd_representation_investigation):
    # Select the relevant columns from the original dataframe
    df = rd_representation_investigation.select(
        "rare_abdominal_surgical_diseases","rare_bone_diseases","rare_cardiac_diseases","rare_circulatory_system_disease", "rare_developmental_defect_during_embryogenesis", "rare_disorder_due_to_toxic_effects","rare_endocrine_disease","rare_gastroenterologic_disease","rare_gynecologic_or_obstetric_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_maxillo_facial_surgical_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_odontologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_surgical_thoracic_disease","rare_systemic_or_rheumatologic_disease","rare_transplantation_disease","rare_urogenital_disease","hospitalized", "life_thretening","death", "Covid_vaccine_dose", "COVID_reinfection", "long_covid", "Paxlovid", "Molnupiravir", "Bebtelovimab", "selected_antiviral_treatment" )

    # Add the vaccination_status column
    df = df.withColumn('vaccination_status', when(df.Covid_vaccine_dose >= 1, 1).otherwise(0))

    # List of rare diseases columns
    rare_diseases_columns = [
        "rare_abdominal_surgical_diseases","rare_bone_diseases","rare_cardiac_diseases","rare_circulatory_system_disease",                     
        "rare_developmental_defect_during_embryogenesis","rare_disorder_due_to_toxic_effects","rare_endocrine_disease","rare_gastroenterologic_disease",
        "rare_gynecologic_or_obstetric_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease",
        "rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_maxillo_facial_surgical_disease","rare_neoplastic_disease",
        "rare_neurologic_disease","rare_odontologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease",
        "rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_surgical_thoracic_disease",
        "rare_systemic_or_rheumatologic_disease","rare_transplantation_disease","rare_urogenital_disease"
    ]

    # Prepare an empty DataFrame for the final result
    df1 = None

    # Loop through each disease column and calculate the sum for the relevant columns
    for rare_disease in rare_diseases_columns:
        # Aggregate values for the current rare disease
        aggregated_df = df.select(
            F.lit(rare_disease).alias("rare_disease"),
            F.sum(when(col(rare_disease) == 1, 1).otherwise(0)).alias("total_patients"),
            F.sum(when(col(rare_disease) == 1, col("hospitalized")).otherwise(0)).alias("hospitalized"),
            F.sum(when(col(rare_disease) == 1, col("life_thretening")).otherwise(0)).alias("life_thretening"),
            F.sum(when(col(rare_disease) == 1, col("death")).otherwise(0)).alias("death"),
            F.sum(when(col(rare_disease) == 1, col("vaccination_status")).otherwise(0)).alias("vaccination_status"),
            F.sum(when(col(rare_disease) == 1, col("COVID_reinfection")).otherwise(0)).alias("COVID_reinfection"),
            F.sum(when(col(rare_disease) == 1, col("long_covid")).otherwise(0)).alias("long_covid"),
            F.sum(when(col(rare_disease) == 1, col("selected_antiviral_treatment")).otherwise(0)).alias("selected_antiviral_treatment"),
            F.sum(when(col(rare_disease) == 1, col("Paxlovid")).otherwise(0)).alias("Paxlovid"),
            F.sum(when(col(rare_disease) == 1, col("Molnupiravir")).otherwise(0)).alias("Molnupiravir"),
            F.sum(when(col(rare_disease) == 1, col("Bebtelovimab")).otherwise(0)).alias("Bebtelovimab")
        )

        # Union the result to the final dataframe
        if df1 is None:
            df1 = aggregated_df
        else:
            df1 = df1.union(aggregated_df)

    # Return the final reorganized DataFrame
    return df1

   

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.70726e63-d349-4f2d-b93f-4e8b0088dd76"),
    Vaccination_antiviral_cohort_data=Input(rid="ri.foundry.main.dataset.da5ffbf3-8c22-4251-a22c-dcbda7e84a8f")
)
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

def rd_severity_frequencies_omicron(Vaccination_antiviral_cohort_data):
    # Select the relevant columns from the original dataframe
    df = Vaccination_antiviral_cohort_data.select( "rare_abdominal_surgical_diseases","rare_bone_diseases","rare_cardiac_diseases","rare_circulatory_system_disease", "rare_developmental_defect_during_embryogenesis", "rare_disorder_due_to_toxic_effects","rare_endocrine_disease","rare_gastroenterologic_disease","rare_gynecologic_or_obstetric_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_maxillo_facial_surgical_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_odontologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_surgical_thoracic_disease","rare_systemic_or_rheumatologic_disease","rare_transplantation_disease","rare_urogenital_disease","hospitalized", "life_thretening","death", "Covid_vaccine_dose", "COVID_reinfection", "long_covid", "Paxlovid", "Molnupiravir", "Bebtelovimab", "selected_antiviral_treatment" )

    # Add the vaccination_status column
    df = df.withColumn('vaccination_status', when(df.Covid_vaccine_dose >= 1, 1).otherwise(0))

    # List of rare diseases columns
    rare_diseases_columns = [
        "rare_abdominal_surgical_diseases","rare_bone_diseases","rare_cardiac_diseases","rare_circulatory_system_disease",                     
        "rare_developmental_defect_during_embryogenesis","rare_disorder_due_to_toxic_effects","rare_endocrine_disease","rare_gastroenterologic_disease",
        "rare_gynecologic_or_obstetric_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease",
        "rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_maxillo_facial_surgical_disease","rare_neoplastic_disease",
        "rare_neurologic_disease","rare_odontologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease",
        "rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_surgical_thoracic_disease",
        "rare_systemic_or_rheumatologic_disease","rare_transplantation_disease","rare_urogenital_disease"
    ]

    # Prepare an empty DataFrame for the final result
    df1 = None

    # Loop through each disease column and calculate the sum for the relevant columns
    for rare_disease in rare_diseases_columns:
        # Aggregate values for the current rare disease
        aggregated_df = df.select(
            F.lit(rare_disease).alias("rare_disease"),
            F.sum(when(col(rare_disease) == 1, 1).otherwise(0)).alias("total_patients"),
            F.sum(when(col(rare_disease) == 1, col("life_thretening")).otherwise(0)).alias("life_thretening"),
            F.sum(when(col(rare_disease) == 1, col("hospitalized")).otherwise(0)).alias("hospitalized"),
            F.sum(when(col(rare_disease) == 1, col("death")).otherwise(0)).alias("death"),
            F.sum(when(col(rare_disease) == 1, col("vaccination_status")).otherwise(0)).alias("vaccination_status"),
            F.sum(when(col(rare_disease) == 1, col("COVID_reinfection")).otherwise(0)).alias("COVID_reinfection"),
            F.sum(when(col(rare_disease) == 1, col("long_covid")).otherwise(0)).alias("long_covid"),
            F.sum(when(col(rare_disease) == 1, col("selected_antiviral_treatment")).otherwise(0)).alias("selected_antiviral_treatment"),
            F.sum(when(col(rare_disease) == 1, col("Paxlovid")).otherwise(0)).alias("Paxlovid"),
            F.sum(when(col(rare_disease) == 1, col("Molnupiravir")).otherwise(0)).alias("Molnupiravir"),
            F.sum(when(col(rare_disease) == 1, col("Bebtelovimab")).otherwise(0)).alias("Bebtelovimab")

        )

        # Union the result to the final dataframe
        if df1 is None:
            df1 = aggregated_df
        else:
            df1 = df1.union(aggregated_df)

    # Return the final reorganized DataFrame
    return df1

@transform_pandas(
    Output(rid="ri.vector.main.execute.df5f8eb9-8b4e-43b7-8a57-4291e165535b"),
    rd_cohort=Input(rid="ri.foundry.main.dataset.56a84162-780d-4199-a416-28f41120e969")
)

import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression

def test_multiVar_rds_models_hosp_new( rd_cohort, outcome_var="hospitalized"):
    df = rd_cohort.toPandas()

    rare_diseases = [
        "rare_bone_diseases", "rare_cardiac_diseases", "rare_developmental_defect_during_embryogenesis",    
        "rare_endocrine_disease", "rare_gastroenterologic_disease", "rare_hematologic_disease", 
        "rare_hepatic_disease", "rare_immune_disease", "rare_inborn_errors_of_metabolism",  
        "rare_infectious_disease", "rare_neoplastic_disease", "rare_neurologic_disease",
        "rare_ophthalmic_disorder", "rare_otorhinolaryngologic_disease", "rare_renal_disease",
        "rare_respiratory_disease", "rare_skin_disease", "rare_systemic_or_rheumatologic_disease"
    ]
# "age_20_40","BMI_normal","gender_FEMALE", "race_White", "ethnicity_Not_Hispanic_or_Latino","smoking_status_Non_smoker",
    base_covariates = [
"age_1_20",  "age_40_65", "age_65_above", "BMI_obese", "BMI_over_weight","BMI_under_weight", "gender_MALE", "race_Black_or_African_American", "race_Unknown","race_Asian", "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown", "smoking_status_Current_or_Former", "vaccination_status","Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease", "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes", "Heart_failure","HIV_infection", "HTN", "Kidney_disease", "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
    ]

    # Dictionary to define which covariates to drop for specific rare diseases
    drop_covariates = {
        "rare_cardiac_diseases": ["Cardiomyophaties", "Coronary_artery_disease", "Heart_failure", "Myocardial_infraction"],
        "rare_immune_disease": ["Cancer", "HIV_infection", "Diabetes", "Rheumatologic_disease"],
        "rare_renal_disease": ["Kidney_disease"],
        "rare_systemic_or_rheumatologic_disease" : ["Rheumatologic_disease", "HIV_infection", "Diabetes", "Rheumatologic_disease"]
    }

    results = []
    rare_disease_sums = df[rare_diseases].sum(axis=1)

    for disease in rare_diseases:
        mask = (df[disease] == 1) | (rare_disease_sums == 0)

        # Determine covariates for this disease
        covariates = base_covariates.copy()
        if disease in drop_covariates:
            covariates = [cov for cov in covariates if cov not in drop_covariates[disease]]

        data = df.loc[mask, covariates + [disease, outcome_var]].copy()
        data["exposure"] = data[disease]

        if data["exposure"].nunique() < 2:
            results.append({"rare_disease": disease, "OR (95% CI)": "NA", "odds_ratio": np.nan,
                            "ci_lower": np.nan, "ci_upper": np.nan, "p_value": np.nan})
            continue

        try:
            # Propensity score model
            ps_model = LogisticRegression(max_iter=1000).fit(data[covariates], data["exposure"])
            ps = ps_model.predict_proba(data[covariates])[:, 1]
            data["weight"] = np.where(data["exposure"] == 1, 1/ps, 1/(1-ps))

            # Weighted logistic regression (doubly robust: exposure + covariates)
            data["intercept"] = 1.0
            model_vars = ["intercept", "exposure"] + covariates
            logit_model = sm.Logit(data[outcome_var], data[model_vars],
                                   freq_weights=data["weight"])
            result = logit_model.fit(disp=0)

            or_val = np.exp(result.params["exposure"])
            ci = result.conf_int().loc["exposure"]
            ci_lower, ci_upper = np.exp(ci[0]), np.exp(ci[1])
            pval = result.pvalues["exposure"]
            formatted = f"{or_val:.2f} ({ci_lower:.2f}-{ci_upper:.2f})"
        except Exception:
            or_val = ci_lower = ci_upper = pval = np.nan
            formatted = "NA"

        results.append({
            "rare_disease": disease,
            "OR (95% CI)": formatted,
            "odds_ratio": or_val,
            "ci_lower": ci_lower,
            "ci_upper": ci_upper,
            "p_value": pval
        })

    return pd.DataFrame(results)

@transform_pandas(
    Output(rid="ri.vector.main.execute.1bc5c860-a948-4057-85ec-8428aac19443"),
    rd_cohort=Input(rid="ri.foundry.main.dataset.56a84162-780d-4199-a416-28f41120e969")
)

import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression

def test_multiVar_rds_models_lt_new( rd_cohort, outcome_var="life_threatening"):
    df = rd_cohort.toPandas()

    rare_diseases = [
        "rare_bone_diseases", "rare_cardiac_diseases", "rare_developmental_defect_during_embryogenesis",    
        "rare_endocrine_disease", "rare_gastroenterologic_disease", "rare_hematologic_disease", 
        "rare_hepatic_disease", "rare_immune_disease", "rare_inborn_errors_of_metabolism",  
        "rare_infectious_disease", "rare_neoplastic_disease", "rare_neurologic_disease",
        "rare_ophthalmic_disorder", "rare_otorhinolaryngologic_disease", "rare_renal_disease",
        "rare_respiratory_disease", "rare_skin_disease", "rare_systemic_or_rheumatologic_disease"
    ]
# "age_20_40","BMI_normal","gender_FEMALE", "race_White", "ethnicity_Not_Hispanic_or_Latino","smoking_status_Non_smoker",
    base_covariates = [
"age_1_20",  "age_40_65", "age_65_above", "BMI_obese", "BMI_over_weight","BMI_under_weight", "gender_MALE", "race_Black_or_African_American", "race_Unknown","race_Asian", "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown", "smoking_status_Current_or_Former", "vaccination_status","Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease", "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes", "Heart_failure","HIV_infection", "HTN", "Kidney_disease", "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
    ]

    # Dictionary to define which covariates to drop for specific rare diseases
    drop_covariates = {
        "rare_cardiac_diseases": ["Cardiomyophaties", "Coronary_artery_disease", "Heart_failure", "Myocardial_infraction"],
        "rare_immune_disease": ["Cancer", "HIV_infection", "Diabetes", "Rheumatologic_disease"],
        "rare_renal_disease": ["Kidney_disease"],
        "rare_systemic_or_rheumatologic_disease" : ["Rheumatologic_disease", "HIV_infection", "Diabetes", "Rheumatologic_disease"]
    }

    results = []
    rare_disease_sums = df[rare_diseases].sum(axis=1)

    for disease in rare_diseases:
        mask = (df[disease] == 1) | (rare_disease_sums == 0)

        # Determine covariates for this disease
        covariates = base_covariates.copy()
        if disease in drop_covariates:
            covariates = [cov for cov in covariates if cov not in drop_covariates[disease]]

        data = df.loc[mask, covariates + [disease, outcome_var]].copy()
        data["exposure"] = data[disease]

        if data["exposure"].nunique() < 2:
            results.append({"rare_disease": disease, "OR (95% CI)": "NA", "odds_ratio": np.nan,
                            "ci_lower": np.nan, "ci_upper": np.nan, "p_value": np.nan})
            continue

        try:
            # Propensity score model
            ps_model = LogisticRegression(max_iter=1000).fit(data[covariates], data["exposure"])
            ps = ps_model.predict_proba(data[covariates])[:, 1]
            data["weight"] = np.where(data["exposure"] == 1, 1/ps, 1/(1-ps))

            # Weighted logistic regression (doubly robust: exposure + covariates)
            data["intercept"] = 1.0
            model_vars = ["intercept", "exposure"] + covariates
            logit_model = sm.Logit(data[outcome_var], data[model_vars],
                                   freq_weights=data["weight"])
            result = logit_model.fit(disp=0)

            or_val = np.exp(result.params["exposure"])
            ci = result.conf_int().loc["exposure"]
            ci_lower, ci_upper = np.exp(ci[0]), np.exp(ci[1])
            pval = result.pvalues["exposure"]
            formatted = f"{or_val:.2f} ({ci_lower:.2f}-{ci_upper:.2f})"
        except Exception:
            or_val = ci_lower = ci_upper = pval = np.nan
            formatted = "NA"

        results.append({
            "rare_disease": disease,
            "OR (95% CI)": formatted,
            "odds_ratio": or_val,
            "ci_lower": ci_lower,
            "ci_upper": ci_upper,
            "p_value": pval
        })

    return pd.DataFrame(results)

@transform_pandas(
    Output(rid="ri.vector.main.execute.a538ae52-e380-40fc-bb02-acde536096c5"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)

from matplotlib import pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

print('numpy version is:', np.__version__)
print('pandas version is:', pd.__version__)
print('matplotlib version is:', mpl.__version__)
print('Seaborn version is:', sns.__version__)

def top_rd_class_figure(rd_covid_lcovid_drugs_cohort):
    df = rd_covid_lcovid_drugs_cohort.toPandas()

    # Select relevant rare disease columns
    columns = [
        "rare_bone_diseases", "rare_cardiac_diseases", "rare_developmental_defect_during_embryogenesis",
        "rare_endocrine_disease", "rare_gastroenterologic_disease", "rare_hematologic_disease",
        "rare_hepatic_disease", "rare_immune_disease", "rare_inborn_errors_of_metabolism",
        "rare_infectious_disease", "rare_neoplastic_disease", "rare_neurologic_disease",
        "rare_ophthalmic_disorder", "rare_otorhinolaryngologic_disease", "rare_renal_disease",
        "rare_respiratory_disease", "rare_skin_disease", "rare_systemic_or_rheumatologic_disease"
    ]
    df = df[columns]

    # Summarize and sort
    dd = pd.DataFrame({
        'Rare disease class': columns,
        'Frequency': df[columns].sum().values
    })
    df_sorted = dd.sort_values(by='Frequency', ascending=False)
    ranking = df_sorted.head(33).sort_values(by='Frequency', ascending=True)

    # Prepare for plotting
    index = [x.replace("_", " ").capitalize() for x in ranking['Rare disease class']]
    values = ranking['Frequency']

    # Set plotting style
    sns.set(style="white")
    fig, ax = plt.subplots(figsize=(12, 14), facecolor='white', dpi=600)

    # Use a clean blue color
    colors = ["#0072B2"] * len(values)  # Blue color for all bars

    # Plot bars
    bars = ax.barh(index, values, color=colors, log=True, align='center')

    # Set axis format
    ax.xaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.yaxis.set_ticks_position('left')
    ax.xaxis.set_ticks_position('bottom')
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)

    # Add count labels
    for rect in bars:
        width = rect.get_width()
        y = rect.get_y() + rect.get_height() / 2
        if width > 0:
            label = '{:,.0f}'.format(width)
            ax.annotate(
                label,
                (width, y),
                xytext=(4, 0),
                textcoords='offset points',
                va='center',
                ha='left',
                fontsize=15,
                color='black'
            )

    # Remove spines
    for pos in ['right', 'top', 'bottom', 'left']:
        plt.gca().spines[pos].set_visible(False)

    # Titles and labels
    plt.title("Distribution of Rare Disease", fontsize=24, pad=1, weight='bold')
    ax.set_xlabel('# of Patients (log scale)', color='k', fontsize=22, labelpad=15)
    ax.set_ylabel('')

    plt.tight_layout(rect=[0, 0, 0.95, 1])
    plt.show()

    return df_sorted

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.59023e70-4a20-4dcd-a704-be55e3005924"),
    Cohort_for_sensiity_analysis=Input(rid="ri.foundry.main.dataset.5879be37-dfde-472f-85a3-cf9174996907"),
    Vaccine_fact_lds=Input(rid="ri.foundry.main.dataset.7482e426-55a2-4a0b-9976-4cb3aa35788d")
)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff
from pyspark.sql.functions import lit

def two_dose_vaccine(Vaccine_fact_lds, Cohort_for_sensiity_analysis):
    df1 = Cohort_for_sensiity_analysis
    df2 = Vaccine_fact_lds.select("person_id","vaccine_txn","first_vax_date","2_vax_date")
    df = df1.join(df2, on=['person_id'], how='left')
    df = df.filter(df.vaccine_txn >=2)
    df = df.filter(df["2_vax_date"].isNotNull())
    # Filter the DataFrame for vaccination 14 days prior to covid-19 diagnosis date  "Bebtelovimab",
    df_filtered = df.filter(datediff(col("COVID_first_poslab_or_diagnosis_date"), col("2_vax_date")) >= 14)
    df_filtered = df_filtered.withColumn("vaccination_status", lit(1))
    return df_filtered.select("person_id","observation_period_post_covid","COVID_first_poslab_or_diagnosis_date","life_thretening","death","hospitalized","vaccination_status","Paxlovid","Molnupiravir","antiviral_treatment","COVID_reinfection","long_covid","age_1_20","age_20_40","age_40_65","age_65_above","Age_bin","BMI_category","BMI_obese","BMI_over_weight","BMI_normal","BMI_under_weight","Gender", "Race", "Ethnicity","smoking_status","gender_MALE","gender_FEMALE","race_Black_or_African_American","race_Unknown","race_White","race_Asian","ethnicity_Not_Hispanic_or_Latino","ethnicity_Hispanic_or_Latino","ethnicity_Unknown","smoking_status_Non_smoker","smoking_status_Current_or_Former","Cancer","Cardiomyophaties","Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes","Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction","Peripheral_vascular_disease","Rheumatologic_disease","rare_disease","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease")

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.19f41c07-8ccb-4736-b517-e12b83f94d45"),
    Cohort_for_sensiity_analysis=Input(rid="ri.foundry.main.dataset.5879be37-dfde-472f-85a3-cf9174996907"),
    Vaccine_fact_lds=Input(rid="ri.foundry.main.dataset.7482e426-55a2-4a0b-9976-4cb3aa35788d")
)

from pyspark.sql import functions as F
from pyspark.sql.functions import lit

def unvaccinated_subcohort(Cohort_for_sensiity_analysis, Vaccine_fact_lds):
    df1 = Cohort_for_sensiity_analysis
    df1 = df1.filter(df1.Covid_vaccine_dose == 0)
    df2 = Vaccine_fact_lds.select("person_id")
    # Perform a left anti join to filter out the common person_id of vaccinated from df1 dataframe to have unvaccinated (control)
    df1_filtered = df1.join(df2, df1.person_id == df2.person_id, "left_anti")
    df1_filtered = df1_filtered.withColumn("vaccination_status", lit(0))
    return df1_filtered.select("person_id","observation_period_post_covid","COVID_first_poslab_or_diagnosis_date","life_thretening","death","hospitalized","vaccination_status","Paxlovid","Molnupiravir","antiviral_treatment","COVID_reinfection","long_covid","age_1_20","age_20_40","age_40_65","age_65_above","Age_bin","BMI_category","BMI_obese","BMI_over_weight","BMI_normal","BMI_under_weight","Gender", "Race", "Ethnicity","smoking_status","gender_MALE","gender_FEMALE","race_Black_or_African_American","race_Unknown","race_White","race_Asian","ethnicity_Not_Hispanic_or_Latino","ethnicity_Hispanic_or_Latino","ethnicity_Unknown","smoking_status_Non_smoker","smoking_status_Current_or_Former","Cancer","Cardiomyophaties","Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes","Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction","Peripheral_vascular_disease","Rheumatologic_disease","rare_disease","rare_bone_diseases","rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease","rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease","rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease","rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease","rare_skin_disease","rare_systemic_or_rheumatologic_disease") # "Bebtelovimab",

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4cd7e3c5-d08f-428d-a5e7-ab044cb0200d"),
    rd_covid_lcovid_drugs_cohort=Input(rid="ri.foundry.main.dataset.a7bb73b7-d78f-4ea0-b584-fec6c99310e6")
)
from pyspark.sql.functions import col, countDistinct, sum as spark_sum, when, round

def vaccine_antiviral_sites(rd_covid_lcovid_drugs_cohort):
    
    df = rd_covid_lcovid_drugs_cohort
    df = df.where(df.COVID_first_poslab_or_diagnosis_date >= '2021-12-23')
    # Calculate counts and percentages per data_partner_id
    summary = df.groupBy("data_partner_id").agg(
        countDistinct("person_id").alias("total_patients"),
        spark_sum((col("Covid_vaccine_dose") >= 2).cast("int")).alias("num_vaccine_dose_2plus"),
        spark_sum((col("selected_antiviral_treatment") == 1).cast("int")).alias("num_antiviral_treated")
    ).withColumn(
        "pct_vaccine_dose_2plus", round(col("num_vaccine_dose_2plus") / col("total_patients") * 100, 1)
    ).withColumn(
        "pct_antiviral_treated", round(col("num_antiviral_treated") / col("total_patients") * 100, 1)
    ).orderBy(col("total_patients").desc())
    return summary

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.64305590-2d6d-4535-93fa-082865130233"),
    Cohort_for_sensiity_analysis=Input(rid="ri.foundry.main.dataset.5879be37-dfde-472f-85a3-cf9174996907")
)

from pyspark.sql.functions import col, countDistinct, sum as spark_sum, when, round

def vaccine_antiviral_sites_omicron(Cohort_for_sensiity_analysis):
    # Load your DataFrame
    df = Cohort_for_sensiity_analysis

    # Calculate counts and percentages per data_partner_id
    df_summary = df.groupBy("data_partner_id").agg(
        countDistinct("person_id").alias("total_patients"),
        spark_sum((col("Covid_vaccine_dose") >= 2).cast("int")).alias("num_vaccine_dose_2plus"),
        spark_sum((col("antiviral_treatment") == 1).cast("int")).alias("num_antiviral_treated")
    ).withColumn(
        "pct_vaccine_dose_2plus", round(col("num_vaccine_dose_2plus") / col("total_patients") * 100, 1)
    ).withColumn(
        "pct_antiviral_treated", round(col("num_antiviral_treated") / col("total_patients") * 100, 1)
    )
        # Filter partners with at least 20% vaccinated and 10% antiviral
    df_final = df_summary[(df_summary['pct_vaccine_dose_2plus'] >= 20) & (df_summary['pct_antiviral_treated'] >= 10)]
    
    return df_final
  

