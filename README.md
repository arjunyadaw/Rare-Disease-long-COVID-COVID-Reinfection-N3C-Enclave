# Impact of Preexisting Rare Diseases on COVID-19 Severity, Reinfection, and Long COVID: A Retrospective Study Using the National Covid Cohort Collaborative (N3C) Data
Arjun S. Yadaw, David K Sahner, Emily Y. Chew, Eric Sid, Dominique Pichard, Ewy A. Mathé, On behalf the N3C Consortium

## Project Summary: 
This study leverages data from over 21.7 million individuals in the National COVID Cohort Collaborative (N3C), including 4.8 million COVID-19-positive cases and more than 300,000 individuals with rare diseases, defined across 12,003 clinically curated conditions. We present the first large-scale, systematically classified analysis of COVID-19 severity, long COVID, and reinfection risk in rare disease populations. We demonstrate that patients with rare diseases face substantially higher odds of life-threatening illness and hospitalization compared to those without rare diseases. Specific rare disease categories, particularly otorhinolaryngologic, developmental, cardiac, and hematologic conditions are linked to disproportionately elevated risk. Importantly, we show that vaccination and antiviral treatment, especially in combination, significantly reduce the risk of severe disease and reinfection. However, these interventions offer limited protection against long COVID in rare disease populations, in contrast to their benefit in the general population.

We employed both univariate and multivariable logistic regression models to assess the association between RD categories and COVID-19 severity outcomes, adjusting for the full set of covariates described above. To evaluate the effects of vaccination and antiviral treatment among individuals with preexisting rare diseases compared to those without, we grouped all RD categories into a single "rare disease" group due to limited sample sizes within individual RD classes. To address baseline differences across treatment groups defined by vaccination status and/or antiviral use, we applied inverse probability of treatment weighting (IPTW) based on propensity scores, estimating the average treatment effect (ATE). Propensity scores were calculated using logistic regression models that included demographic, clinical, and comorbidity variables. Stabilized weights were used to enhance robustness, and extreme weights were trimmed at the 99th percentile. Covariate balance between treatment groups was evaluated using standardized mean differences (SMD), with values below 0.1 indicating adequate balance. Finally, we used doubly robust logistic regression models incorporating both the trimmed IPTW weights and covariates to estimate adjusted odds ratios (ORs) for the outcomes of interest. 

## Purpose of this code: 
This code is designed to identify rare disease patients, long COVID, COVID reinfection, antiviral treatment cohort in electronic health record (EHR) system. One can use this code to create indepth analysis of rare disease patients acute severity of COVID-19 positive patients and also investigate post covid outcomes (long COVID and COVID reinfection). Further, investigation of prevention (vaccination) and intervention affect (antiviral treatment) on these outcomes.  

## Python Libraries Used:
The following Python version and packages are required to execute this in palantir foundary:

* Python (3.10.16)
* Numpy (1.26.4)
* Pandas (1.5.3)
* Statsmodel (0.14.4)
* Patsy (1.0.1)
* Matplotlib (3.9.4)
* Seaborn (0.13.2)
* Spark SQL (3.4.1.34)

## R Libraries Used:
* WeightIt(1.4.0)
* cobalt(4.6.0)
* ggpubr(0.6.0)
* survey(3.8_3)
* dplyr(1.1.4)
* broom (1.0.8)
* gtsummary (2.1.0)
* tidyverse (2.0.0)
* gt (0.11.1)
* ggplot2 (3.5.2)

# 🧬 Rare Disease COVID-19 Outcomes Analysis

This repository contains code to identify and analyze rare disease patients within an Electronic Health Record (EHR) system, with a focus on COVID-19-related outcomes and the impact of preventive and therapeutic interventions.

---

## 🎯 Key Features

### ✅ Cohort Identification
- Identifies patients with **rare diseases**
- Detects **COVID-19 positive** cases
- Flags cases of **long COVID** and **COVID-19 reinfection**
- Captures **antiviral treatments**:
  - Paxlovid
  - Molnupiravir
 

### 🏥 Acute COVID-19 Severity Analysis
- Measures severity of acute infection:
  - Hospitalization
  - Life-threatening illness

### 📈 Post-COVID Outcome Investigation
- Analyzes long-term outcomes among rare disease patients:
  - **Long COVID**
  - **COVID-19 reinfection**
- Assesses associations with patient demographics and comorbidities

### 💉 Prevention & Intervention Effects
- Evaluates the **effectiveness of COVID-19 vaccination** in rare disease (RD) vs. control (without RDs) populations
- Estimates the **treatment effects of antivirals** on:
  - Acute severity
  - Long COVID
  - Reinfection risk

---

## 🔬 Use Cases

This codebase enables advanced EHR-based observational analyses, including:

- Propensity score modeling
- Inverse probability of treatment weighting (IPTW)
- Multivariable logistic regression
- Stratified or subgroup analyses for specific rare diseases
- Sensitivity analyses and visualizations for publication

## Running our model

- To reproduce the results, one has to create rare disease cohort based ICD-10 and SNOMED-CT codes which is shared in our previous paper (MedRxiv doi: https://doi.org/10.1101/2025.05.02.25325348). The cohort construction code is organized into folders named according to each cohort. The final modeling code is located in the Rare-Disease-Analysis-N3C-Enclave folder. All simulations were conducted within the Palantir Foundry platform.




