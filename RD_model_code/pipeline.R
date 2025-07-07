

@transform_pandas(
    Output(rid="ri.vector.main.execute.2d54d359-011a-46d2-81d9-ce64f2ebde87"),
    Summary_table_data_LT=Input(rid="ri.foundry.main.dataset.f95543b8-6fbf-4bdd-a32f-7b36289cde2d")
)

LT_demographic <- function(Summary_table_data_LT) {

    library(gtsummary)
    library(tidyverse)
    library(gt)

    trial = as.data.frame(Summary_table_data_LT)
    trial %>% 
    tbl_summary(by = "life_thretening") %>% # stratify by Severity_Type
    add_p() %>% # format p-values with two digits
    add_overall() %>%
    print()
    
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.49783058-5fd9-4e22-bca7-5153aad9035e"),
    rd_cohort=Input(rid="ri.foundry.main.dataset.56a84162-780d-4199-a416-28f41120e969")
)
Life_threatening_demographic <- function(rd_cohort) {
  
  library(gtsummary)
  library(tidyverse)
  library(gt)

  selected_columns <- c(
    "life_threatening", "age_1_20", "age_20_40", "age_40_65", "age_65_above",
    "BMI_under_weight", "BMI_normal", "BMI_over_weight", "BMI_obese",
    "gender_MALE", "gender_FEMALE", "race_Asian", "race_Black_or_African_American",
    "race_Unknown", "race_White", "ethnicity_Hispanic_or_Latino",
    "ethnicity_Not_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former", "smoking_status_Non_smoker",
    "vaccination_status", "antiviral_treatment", "COVID_reinfection", "long_covid",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease", "liver_disease",
    "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease",
    "rare_disease", "rare_bone_diseases", "rare_cardiac_diseases",
    "rare_developmental_defect_during_embryogenesis", "rare_endocrine_disease",
    "rare_gastroenterologic_disease", "rare_hematologic_disease",
    "rare_hepatic_disease", "rare_immune_disease", "rare_inborn_errors_of_metabolism",
    "rare_infectious_disease", "rare_neoplastic_disease", "rare_neurologic_disease",
    "rare_ophthalmic_disorder", "rare_otorhinolaryngologic_disease",
    "rare_renal_disease", "rare_respiratory_disease", "rare_skin_disease",
    "rare_systemic_or_rheumatologic_disease"
  )

  # Subset and convert to data frame
  trial <- rd_cohort %>%
    dplyr::select(all_of(selected_columns)) %>%
    as.data.frame()

  trial %>%
    tbl_summary(by = "life_threatening") %>%  # stratify by life_threatening
    add_p() %>%                               # add p-values
    add_overall() %>%
    print()
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.bcf002cb-a849-4d91-b2cc-937e6ca75c3e"),
    Summary_table_data=Input(rid="ri.foundry.main.dataset.b239d757-10b0-4220-be17-42f029bc9656")
)
hosp_demographic <- function(Summary_table_data) {

    library(gtsummary)
    library(tidyverse)
    library(gt)

    trial = as.data.frame(Summary_table_data)
    trial %>% 
    tbl_summary(by = "hospitalized") %>% # stratify by Severity_Type
    add_p() %>% # format p-values with two digits
    add_overall() %>%
    print()

}

@transform_pandas(
    Output(rid="ri.vector.main.execute.af71a9df-f395-4a8d-98fb-1552bc8ac6af"),
    rd_cohort=Input(rid="ri.foundry.main.dataset.56a84162-780d-4199-a416-28f41120e969")
)
hospitalized_demographic <- function(rd_cohort) {
  
  library(gtsummary)
  library(tidyverse)
  library(gt)

  selected_columns <- c(
    "hospitalized", "age_1_20", "age_20_40", "age_40_65", "age_65_above",
    "BMI_under_weight", "BMI_normal", "BMI_over_weight", "BMI_obese",
    "gender_MALE", "gender_FEMALE", "race_Asian", "race_Black_or_African_American",
    "race_Unknown", "race_White", "ethnicity_Hispanic_or_Latino",
    "ethnicity_Not_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former", "smoking_status_Non_smoker",
    "vaccination_status", "antiviral_treatment", "COVID_reinfection", "long_covid",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease", "liver_disease",
    "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease",
    "rare_disease", "rare_bone_diseases", "rare_cardiac_diseases",
    "rare_developmental_defect_during_embryogenesis", "rare_endocrine_disease",
    "rare_gastroenterologic_disease", "rare_hematologic_disease",
    "rare_hepatic_disease", "rare_immune_disease", "rare_inborn_errors_of_metabolism",
    "rare_infectious_disease", "rare_neoplastic_disease", "rare_neurologic_disease",
    "rare_ophthalmic_disorder", "rare_otorhinolaryngologic_disease",
    "rare_renal_disease", "rare_respiratory_disease", "rare_skin_disease",
    "rare_systemic_or_rheumatologic_disease"
  )

  # Subset and convert to data frame
  trial <- rd_cohort %>%
    dplyr::select(all_of(selected_columns)) %>%
    as.data.frame()

  trial %>%
    tbl_summary(by = "hospitalized") %>%  # stratify by life_threatening
    add_p() %>%                               # add p-values
    add_overall() %>%
    print()
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.6250be9f-e2d9-4854-ab0e-03045bb0411e"),
    multivariate_for_radial_plot_spark_test=Input(rid="ri.foundry.main.dataset.8efc36cf-404e-457d-bca0-78b17d49f71c")
)
# radial plot of multivariate model
multi_var_radial_plot <- function(multivariate_for_radial_plot_spark_test) {
  library(ggplot2)

  # Transform Odds_ratio to log scale
  multivariate_for_radial_plot_spark_test$Log_Odds_Ratio <- log(multivariate_for_radial_plot_spark_test$odds_ratio)
  multivariate_for_radial_plot_spark_test$Log_CI_lower <- log(multivariate_for_radial_plot_spark_test$ci_lower)
  multivariate_for_radial_plot_spark_test$Log_CI_upper <- log(multivariate_for_radial_plot_spark_test$ci_upper)

  # Reorder the levels of the 'rds' factor based on your desired order  ("RGOD",)
  multivariate_for_radial_plot_spark_test$rare_disease <- factor(multivariate_for_radial_plot_spark_test$rare_disease, levels = c(
        "ROTORD","RDDDE","RCD","RRESD","RBD","RED","RHD",
        "RND","RHEPD","RNUED","RID","RRD","RIEM","ROD",
        "RINFD","RGD","RSRD","OR","RSD"))

  # Calculate the error bars using CI_lower and CI_upper
  multivariate_for_radial_plot_spark_test$error_lower <- multivariate_for_radial_plot_spark_test$Log_Odds_Ratio - multivariate_for_radial_plot_spark_test$Log_CI_lower
  multivariate_for_radial_plot_spark_test$error_upper <- multivariate_for_radial_plot_spark_test$Log_CI_upper - multivariate_for_radial_plot_spark_test$Log_Odds_Ratio

  # Plot with centered error bars in black
  gg <- ggplot(data = multivariate_for_radial_plot_spark_test, aes(rare_disease, Log_Odds_Ratio, fill = Severity)) +
    geom_bar(stat = "identity", color = "black", position = position_dodge(width = 0.5), width = 0.5) +
    geom_errorbar(aes(ymin = Log_Odds_Ratio - error_lower, ymax = Log_Odds_Ratio + error_upper),
                  width = 0.2, position = position_dodge(0.5), color = "black") +
    coord_polar(theta = "x", start = 0) +
    ylim(c(-0.5, 1.8)) +
    scale_y_continuous(limits = c(-1.0, 1.8), breaks = seq(-1.0, 1.8, by = 0.5)) +
    scale_fill_brewer(palette = "Set1") +
    theme_bw() +
    theme(axis.text.y = element_text(size = 16, colour = "black"),
          axis.text.x = element_text(size = 16, colour = "black"),
          legend.text = element_text(size = 14, colour = "black"),
          legend.title = element_blank(),
          axis.title = element_text(size = 14, face = "bold"),
          axis.text = element_text(size = 12)
    )
  print(gg)
  return(multivariate_for_radial_plot_spark_test)  # Return the modified dataframe
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.fa78eb22-fafb-4690-abe7-3035638c4889"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

no_rd_antiviral_only_vs_control_hosp <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 0 & antiviral_treatment == 1) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 0 & antiviral_treatment == 1, 1, 0),
      hospitalized = as.integer(hospitalized),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total no rare disease patients (antiviral only vs control):", nrow(df)))
  print(table(df$hospitalized))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("hospitalized ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.e91d6383-bdf4-4cd8-af3c-a26a17252c9d"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

no_rd_antiviral_only_vs_control_lt <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 0 & antiviral_treatment == 1) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 0 & antiviral_treatment == 1, 1, 0),
      life_thretening = as.integer(life_thretening),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total no rare disease patients (antiviral only vs control):", nrow(df)))
  print(table(df$life_thretening))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("life_thretening ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.66517936-6be5-46c1-ac5f-c1634706e651"),
    long_covid_dataframe_partial_vacc_new=Input(rid="ri.foundry.main.dataset.bd9f57b4-c9e5-48d6-ad2b-f3dcdb2f6099")
)

no_rd_antiviral_vs_control_lc <- function(long_covid_dataframe_partial_vacc_new) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- long_covid_dataframe_partial_vacc_new %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 0 & antiviral_treatment == 1) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 0 & antiviral_treatment == 1, 1, 0),
      long_covid = as.integer(long_covid),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total without rare disease patients (antiviral only vs control):", nrow(df)))
  print(table(df$long_covid))
  print(table(df$trt_combine))

  # 2. Propensity score weighting 
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Antiviral vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model   
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("long_covid ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.11e738ac-6e6c-4f6e-863f-903861040954"),
    long_covid_dataframe_partial_vacc_new=Input(rid="ri.foundry.main.dataset.bd9f57b4-c9e5-48d6-ad2b-f3dcdb2f6099")
)

no_rd_lc_vacc_antiviral_vs_control <- function(long_covid_dataframe_partial_vacc_new) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)
  library(forcats)

  # 1. Subset data: Rare disease, vaccinated only vs. no treatment
  df <- long_covid_dataframe_partial_vacc_new %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 1) | 
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = factor(ifelse(vaccination_status == 1 & antiviral_treatment == 1, 1, 0), 
                           labels = c("No Treatment", "Vaccination & Antiviral Only")),
      long_covid = as.integer(long_covid)
    )

    # Print counts
    print(paste("Total rare disease patients (included in analysis):", nrow(df)))
    print(paste("Number of long COVID cases:", sum(df$long_covid, na.rm = TRUE)))
    print(paste("Number in Vaccination & Antiviral group:", sum(df$trt_combine == "Vaccination & Antiviral Only", na.rm = TRUE)))
    print(paste("Number in No Treatment group:", sum(df$trt_combine == "No Treatment", na.rm = TRUE)))

  # Basic checks
  print(paste("Total rare disease patients (vaccination & antiviral only vs control):", nrow(df)))
  print(table(df$trt_combine))
  print(table(df$long_covid))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights
  cap_val <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, cap_val)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Balance & PS plots
  love_plot <- love.plot(balance,
                         binary = "std",
                         var.order = "unadjusted",
                         abs = TRUE,
                         sample.names = c("Unweighted", "Weighted")) +
               labs(title = "Covariate Balance: Vaccination vs Control") +
               theme_minimal(base_size = 14)

  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score") +
             theme_minimal(base_size = 14)

  print(ggarrange(love_plot, ps_plot, nrow = 2, heights = c(1.2, 1)))

  # 6. Weighted logistic regression
  covariates <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE", "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown", "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  model_formula <- as.formula(
    paste("long_covid ~ trt_combine +", paste(covariates, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 7. Tidy and return model results
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      Covariate = fct_reorder(term, estimate),
      p_value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate, Odds_Ratio = estimate, CI_95 = OR_CI, p_value)

  print(tidy_model)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.0093e29d-6d3f-4a1b-a798-4c248f207645"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

no_rd_trt_combine_Weighting_output_hosp <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep
  df <- partial_fully_vaccinated_data %>% filter(rare_disease == 0)

  # Print dataset size
  cat("Size of non rare disease cohort:", nrow(df), "patients\n")

  # Print number of hospitalized vs not hospitalized
  cat("Hospitalized patients:", sum(df$hospitalized == 1, na.rm = TRUE), "\n")
  cat("Non-hospitalized patients:", sum(df$hospitalized == 0, na.rm = TRUE), "\n\n")

  # Define combined treatment variable
  df <- df %>%
  filter((vaccination_status == 1 & antiviral_treatment == 1) | 
         (vaccination_status == 0 & antiviral_treatment == 0)) %>%
  mutate(
    trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 1, 1, 0)
  )

  df$trt_combine <- factor(df$trt_combine)
  df$hospitalized <- as.integer(df$hospitalized)

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (above 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Balance assessment
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance,
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance for trt_combine")

  # 6. Propensity score distribution plot
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  # 7. Display both plots
  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("hospitalized ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Format output
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.221b4eb2-3615-4f11-8c55-0573a78124b6"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

    
no_rd_trt_combine_Weighting_output_lt <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep
  df <- partial_fully_vaccinated_data %>% filter(rare_disease == 0)

  # Print dataset size
  cat("Size of rare disease cohort:", nrow(df), "patients\n")

  # Print number of life_thretening vs not life_thretening
  cat("life_thretening patients:", sum(df$life_thretening == 1, na.rm = TRUE), "\n")
  cat("Non-life_thretening patients:", sum(df$life_thretening == 0, na.rm = TRUE), "\n\n")

  # Define combined treatment variable

  df <- df %>%
  filter((vaccination_status == 1 & antiviral_treatment == 1) | 
         (vaccination_status == 0 & antiviral_treatment == 0)) %>%
  mutate(
    trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 1, 1, 0)
  )

  df$trt_combine <- factor(df$trt_combine)
  df$life_thretening <- as.integer(df$life_thretening)

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (above 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Balance assessment
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance,
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance for trt_combine")

  # 6. Propensity score distribution plot
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  # 7. Display both plots
  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("life_thretening ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Format output
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.5af4f632-7afc-4358-b71b-e763633b8356"),
    long_covid_dataframe_partial_vacc_new=Input(rid="ri.foundry.main.dataset.bd9f57b4-c9e5-48d6-ad2b-f3dcdb2f6099")
)
 
no_rd_vacc_vs_control_lc <- function(long_covid_dataframe_partial_vacc_new) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Patients without rare disease, Vaccination only vs Control (no treatment)
  df <- long_covid_dataframe_partial_vacc_new %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      long_covid = as.integer(long_covid),
      trt_combine = factor(trt_combine)
    )

  # 2. Print summary counts
  print(paste("Total patients included (no rare disease):", nrow(df)))
  print(paste("Number of long COVID cases:", sum(df$long_covid, na.rm = TRUE)))
  print(paste("Number in vaccination group:", sum(df$trt_combine == 1, na.rm = TRUE))) 
  print(paste("Number in no vaccination group:", sum(df$trt_combine == 0, na.rm = TRUE)))

  # 3. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 4. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 5. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 6. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccination Only vs Control (No Rare Disease)")

  # 7. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("long_covid ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.650ba337-6265-45b1-9cd2-2903ab29b2ec"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

no_rd_vaccination_only_vs_control_hosp <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Without Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      hospitalized = as.integer(hospitalized),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total rare disease patients (vaccination only vs control):", nrow(df)))
  print(table(df$hospitalized))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("hospitalized ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.2effbb2a-48f8-4fad-a314-3dd23e4d59a6"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)
  

no_rd_vaccination_only_vs_control_lt <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      life_thretening = as.integer(life_thretening),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total rare disease patients (vaccination only vs control):", nrow(df)))
  print(table(df$life_thretening))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("life_thretening ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.2adbe32b-c539-47fb-904a-e502921f7ac4"),
    new_covid_reinfection_partial_vacc_antiv_data=Input(rid="ri.foundry.main.dataset.8b3fcb9f-22e9-4d5d-a984-2927911b78cd")
)
    
no_rd_vaccination_vs_control_reinfec_new <- function(new_covid_reinfection_partial_vacc_antiv_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Patients with rare disease, Vaccination only vs Control (no treatment)
  df <- new_covid_reinfection_partial_vacc_antiv_data %>%
    filter(rare_disease == 0) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      COVID_reinfection = as.integer(COVID_reinfection),
      trt_combine = factor(trt_combine)
    )

  # 2. Print summary counts
  print(paste("Total patients included (rare disease):", nrow(df)))
  print(paste("Number of COVID reinfection cases:", sum(df$COVID_reinfection, na.rm = TRUE)))
  print(paste("Number in vaccination group:", sum(df$trt_combine == 1, na.rm = TRUE))) 
  print(paste("Number in no vaccination group:", sum(df$trt_combine == 0, na.rm = TRUE)))

  # 3. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 4. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 5. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 6. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccination Only vs Control (Rare Disease)")

  # 7. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("COVID_reinfection ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.28c3e62d-34c3-487a-aeb6-de844a53a2fd"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

rd_antiviral_only_vs_control_hosp <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 0 & antiviral_treatment == 1) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 0 & antiviral_treatment == 1, 1, 0),
      hospitalized = as.integer(hospitalized),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total rare disease patients (antiviral only vs control):", nrow(df)))
  print(table(df$hospitalized))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("hospitalized ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.5064708d-27ad-4cc2-8d29-2b7eb739f39d"),
    long_covid_dataframe_partial_vacc_new=Input(rid="ri.foundry.main.dataset.bd9f57b4-c9e5-48d6-ad2b-f3dcdb2f6099")
)
rd_antiviral_only_vs_control_lc <- function(long_covid_dataframe_partial_vacc_new) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Patients WITHOUT rare disease, Antiviral only vs Control (No treatment) Paxlovid,antiviral_treatment
  df <- long_covid_dataframe_partial_vacc_new %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 0 & antiviral_treatment == 1) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 0 & antiviral_treatment == 1, 1, 0),
      long_covid = as.integer(long_covid),
      trt_combine = factor(trt_combine)
    )

  # 2. Print summary counts
  print(paste("Total patients included (no rare disease):", nrow(df)))
  print(paste("Number of long COVID cases:", sum(df$long_covid, na.rm = TRUE)))
  print(paste("Number in Antiviral group:", sum(df$trt_combine == 1, na.rm = TRUE)))
  print(paste("Number in No Treatment group:", sum(df$trt_combine == 0, na.rm = TRUE)))

  print(table(df$long_covid))
  print(table(df$trt_combine))

  # 3. Propensity score weighting 
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 4. Trim extreme weights
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 5. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 6. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Antiviral Only vs Control")

  # 7. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates for outcome model 
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  model_formula <- as.formula(
    paste("long_covid ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  # 9. Weighted logistic regression
  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Tidy output
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.c5600932-dc57-435f-9ac7-a9ce1b124c69"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

rd_antiviral_only_vs_control_lt <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 0 & antiviral_treatment == 1) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 0 & antiviral_treatment == 1, 1, 0),
      life_thretening = as.integer(life_thretening),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total rare disease patients (antiviral only vs control):", nrow(df)))
  print(table(df$life_thretening))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("life_thretening ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.d3ee136f-e3b6-48d1-bcda-bcf029433c93"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

rd_demographic_updated <- function(partial_fully_vaccinated_data) {
  
  library(gtsummary)
  library(tidyverse)
  library(gt)

  selected_columns <- c(
    "Age_bin","BMI_category","Gender", "Race", "Ethnicity","smoking_status","life_thretening", "hospitalized",   
    "vaccination_status","antiviral_treatment","COVID_reinfection","long_covid","Cancer","Cardiomyophaties",
    "Cerebro_vascular_disease","Chronic_lung_disease","Coronary_artery_disease","Dementia_before","Depression","Diabetes",
    "Heart_failure","HIV_infection","HTN","Kidney_disease","liver_disease","Myocardial_infraction",
    "Peripheral_vascular_disease","Rheumatologic_disease","Systemic_corticosteroids","rare_disease","rare_bone_diseases",
    "rare_cardiac_diseases","rare_developmental_defect_during_embryogenesis","rare_endocrine_disease",
    "rare_gastroenterologic_disease","rare_hematologic_disease","rare_hepatic_disease","rare_immune_disease",
    "rare_inborn_errors_of_metabolism","rare_infectious_disease","rare_neoplastic_disease","rare_neurologic_disease",
    "rare_ophthalmic_disorder","rare_otorhinolaryngologic_disease","rare_renal_disease","rare_respiratory_disease",
    "rare_skin_disease","rare_systemic_or_rheumatologic_disease"
  )

  # Subset and convert to data frame
  trial <- partial_fully_vaccinated_data %>%
    dplyr::select(all_of(selected_columns)) %>%
    mutate(
      prevention_intervention = case_when(
        vaccination_status == 1 & antiviral_treatment == 0 ~ "vaccination",
        vaccination_status == 0 & antiviral_treatment == 1 ~ "antiviral",
        vaccination_status == 1 & antiviral_treatment == 1 ~ "vacc_antiviral",
        TRUE ~ "control"
      )
    ) %>%
    as.data.frame()

  trial %>%
    tbl_summary(by = "rare_disease") %>%  # stratify by rare_disease
    add_p() %>%                           # add p-values
    add_overall() %>%
    print()
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.fc20a250-e6dd-4af9-b31f-0bad06311e49"),
    long_covid_dataframe_partial_vacc_new=Input(rid="ri.foundry.main.dataset.bd9f57b4-c9e5-48d6-ad2b-f3dcdb2f6099")
)

rd_lc_vaccination_vs_control_all_patients <- function(long_covid_dataframe_partial_vacc_new) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)
  library(forcats)

  # 1. Subset data: Rare disease, vaccinated only vs. no treatment
  df <- long_covid_dataframe_partial_vacc_new %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 1) | 
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = factor(ifelse(vaccination_status == 1 & antiviral_treatment == 1, 1, 0), 
                           labels = c("No Treatment", "Vaccination & Antiviral Only")),
      long_covid = as.integer(long_covid)
    )

    # Print counts
    print(paste("Total rare disease patients (included in analysis):", nrow(df)))
    print(paste("Number of long COVID cases:", sum(df$long_covid, na.rm = TRUE)))
    print(paste("Number in Vaccination & Antiviral group:", sum(df$trt_combine == "Vaccination & Antiviral Only", na.rm = TRUE)))
    print(paste("Number in No Treatment group:", sum(df$trt_combine == "No Treatment", na.rm = TRUE)))

  # Basic checks
  print(paste("Total rare disease patients (vaccination & antiviral only vs control):", nrow(df)))
  print(table(df$trt_combine))
  print(table(df$long_covid))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights
  cap_val <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, cap_val)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Balance & PS plots
  love_plot <- love.plot(balance,
                         binary = "std",
                         var.order = "unadjusted",
                         abs = TRUE,
                         sample.names = c("Unweighted", "Weighted")) +
               labs(title = "Covariate Balance: Vaccination vs Control") +
               theme_minimal(base_size = 14)

  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score") +
             theme_minimal(base_size = 14)

  print(ggarrange(love_plot, ps_plot, nrow = 2, heights = c(1.2, 1)))

  # 6. Weighted logistic regression
  covariates <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE", "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown", "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  model_formula <- as.formula(
    paste("long_covid ~ trt_combine +", paste(covariates, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 7. Tidy and return model results
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      Covariate = fct_reorder(term, estimate),
      p_value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate, Odds_Ratio = estimate, CI_95 = OR_CI, p_value)

  print(tidy_model)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.963076b2-2355-4cfa-8190-ddc70286fe70"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)
rd_trt_combine_Weighting_output_hosp <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep
  df <- partial_fully_vaccinated_data %>% filter(rare_disease == 1)

  # Print dataset size
  cat("Size of rare disease cohort:", nrow(df), "patients\n")

  # Print number of hospitalized vs not hospitalized
  cat("Hospitalized patients:", sum(df$hospitalized == 1, na.rm = TRUE), "\n")
  cat("Non-hospitalized patients:", sum(df$hospitalized == 0, na.rm = TRUE), "\n\n")

  # Define combined treatment variable

  df <- df %>%
  filter((vaccination_status == 1 & antiviral_treatment == 1) | 
         (vaccination_status == 0 & antiviral_treatment == 0)) %>%
  mutate(
    trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 1, 1, 0)
  )

  df$trt_combine <- factor(df$trt_combine)
  df$hospitalized <- as.integer(df$hospitalized)

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (above 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Balance assessment
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance,
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance for trt_combine")

  # 6. Propensity score distribution plot
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  # 7. Display both plots
  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("hospitalized ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Format output
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.568966af-0e71-450c-b493-8b5f8316fef2"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

    
rd_trt_combine_Weighting_output_lt <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep
  df <- partial_fully_vaccinated_data %>% filter(rare_disease == 1)

  # Print dataset size
  cat("Size of rare disease cohort:", nrow(df), "patients\n")

  # Print number of life_thretening vs not life_thretening
  cat("life_thretening patients:", sum(df$life_thretening == 1, na.rm = TRUE), "\n")
  cat("Non-life_thretening patients:", sum(df$life_thretening == 0, na.rm = TRUE), "\n\n")

  # Define combined treatment variable

  df <- df %>%
  filter((vaccination_status == 1 & antiviral_treatment == 1) | 
         (vaccination_status == 0 & antiviral_treatment == 0)) %>%
  mutate(
    trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 1, 1, 0)
  )

  df$trt_combine <- factor(df$trt_combine)
  df$life_thretening <- as.integer(df$life_thretening)

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (above 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Balance assessment
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance,
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance for trt_combine")

  # 6. Propensity score distribution plot
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  # 7. Display both plots
  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("life_thretening ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Format output
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.51938b22-f688-4382-9261-81fcde961187"),
    new_covid_reinfection_partial_vacc_antiv_data=Input(rid="ri.foundry.main.dataset.8b3fcb9f-22e9-4d5d-a984-2927911b78cd")
)
    
rd_vacc_only_vs_control_reinfec_new <- function(new_covid_reinfection_partial_vacc_antiv_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Patients with rare disease, Vaccination only vs Control (no treatment)
  df <- new_covid_reinfection_partial_vacc_antiv_data %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      COVID_reinfection = as.integer(COVID_reinfection),
      trt_combine = factor(trt_combine)
    )

  # 2. Print summary counts
  print(paste("Total patients included (rare disease):", nrow(df)))
  print(paste("Number of COVID reinfection cases:", sum(df$COVID_reinfection, na.rm = TRUE)))
  print(paste("Number in vaccination group:", sum(df$trt_combine == 1, na.rm = TRUE))) 
  print(paste("Number in no vaccination group:", sum(df$trt_combine == 0, na.rm = TRUE)))

  # 3. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 4. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 5. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 6. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccination Only vs Control (Rare Disease)")

  # 7. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("COVID_reinfection ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.5415f23a-6169-4046-8c89-74b4807797b9"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)
rd_vaccination_only_vs_control_hosp <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      hospitalized = as.integer(hospitalized),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total rare disease patients (vaccination only vs control):", nrow(df)))
  print(table(df$hospitalized))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("hospitalized ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.8cdcad97-9891-4e4c-a3bb-d6ec5a9e2e5e"),
    long_covid_dataframe_partial_vacc_new=Input(rid="ri.foundry.main.dataset.bd9f57b4-c9e5-48d6-ad2b-f3dcdb2f6099")
)
rd_vaccination_only_vs_control_lc <- function(long_covid_dataframe_partial_vacc_new) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Patients without rare disease, Vaccination only vs Control (no treatment)
  df <- long_covid_dataframe_partial_vacc_new %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      long_covid = as.integer(long_covid),
      trt_combine = factor(trt_combine)
    )

  # 2. Print summary counts
  print(paste("Total patients included (rare disease):", nrow(df)))
  print(paste("Number of long COVID cases:", sum(df$long_covid, na.rm = TRUE)))
  print(paste("Number in vaccination group:", sum(df$trt_combine == 1, na.rm = TRUE))) 
  print(paste("Number in no vaccination group:", sum(df$trt_combine == 0, na.rm = TRUE)))

  # 3. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 4. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 5. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 6. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccination Only vs Control (No Rare Disease)")

  # 7. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 8. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 9. Weighted logistic regression model
  model_formula <- as.formula(
    paste("long_covid ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 10. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.08a5e984-4a17-4f32-9a20-5b06e2dc05ab"),
    partial_fully_vaccinated_data=Input(rid="ri.foundry.main.dataset.6ad156f2-d074-4362-a242-9021fe7d3311")
)

rd_vaccination_only_vs_control_lt <- function(partial_fully_vaccinated_data) {
  library(WeightIt)
  library(cobalt)
  library(ggplot2)
  library(ggpubr)
  library(survey)
  library(dplyr)
  library(broom)

  # 1. Data prep: Rare disease patients, comparing vaccinated only vs control
  df <- partial_fully_vaccinated_data %>%
    filter(rare_disease == 1) %>%
    filter((vaccination_status == 1 & antiviral_treatment == 0) |
           (vaccination_status == 0 & antiviral_treatment == 0)) %>%
    mutate(
      trt_combine = ifelse(vaccination_status == 1 & antiviral_treatment == 0, 1, 0),
      life_thretening = as.integer(life_thretening),
      trt_combine = factor(trt_combine)
    )

  # Summary stats
  print(paste("Total rare disease patients (vaccination only vs control):", nrow(df)))
  print(table(df$life_thretening))
  print(table(df$trt_combine))

  # 2. Propensity score weighting
  weight_model <- weightit(trt_combine ~ age_1_20 + age_20_40 + age_40_65 + age_65_above +
                             BMI_obese + BMI_normal + BMI_over_weight + BMI_under_weight +
                             gender_MALE + gender_FEMALE + race_White +
                             race_Black_or_African_American + race_Unknown + race_Asian +
                             ethnicity_Hispanic_or_Latino + ethnicity_Unknown + ethnicity_Not_Hispanic_or_Latino +
                             smoking_status_Current_or_Former + smoking_status_Non_smoker +
                             Cancer + Cardiomyophaties + Cerebro_vascular_disease + Chronic_lung_disease +
                             Coronary_artery_disease + Dementia_before + Depression + Diabetes +
                             Heart_failure + HIV_infection + HTN + Kidney_disease +
                             liver_disease + Myocardial_infraction + Peripheral_vascular_disease + Rheumatologic_disease,
                           data = df,
                           method = "ps", estimand = "ATE", stabilize = TRUE)

  df$weights <- weight_model$weights
  df$pscore <- weight_model$ps

  # 3. Trim extreme weights (cap at 99th percentile)
  weight_cap <- quantile(df$weights, 0.99)
  df$weights_trimmed <- pmin(df$weights, weight_cap)

  # 4. Covariate balance
  balance <- bal.tab(weight_model, un = TRUE, thresholds = c(m = .1, v = 2))
  print(balance)

  # 5. Love plot
  love_plot <- love.plot(balance, 
                         var.names = var.names(balance, type = "vec"),
                         binary = "std",
                         drop.distance = TRUE,
                         var.order = "unadjusted",
                         grid = TRUE,
                         abs = TRUE,
                         line = TRUE,
                         sample.names = c("Unweighted", "Weighted"),
                         thresholds = c(m = .1)) +
               labs(title = "Covariate Balance: Vaccinated vs Control")

  # 6. Propensity score distribution
  ps_plot <- bal.plot(weight_model, var.name = "prop.score", which = "both",
                      type = "density", mirror = TRUE,
                      sample.names = c("Unweighted", "Weighted")) +
             labs(title = "Propensity Score Distribution", x = "Propensity Score")

  combined_plots <- ggarrange(love_plot, ps_plot, ncol = 1, nrow = 2, heights = c(12, 6)) +
                    theme(text = element_text(size = 16),
                          axis.text = element_text(size = 14),
                          plot.title = element_text(size = 18))
  plot(combined_plots)

  # 7. Covariates to include in model
  covariates_for_model <- c(
    "age_1_20", "age_40_65", "age_65_above",
    "BMI_obese", "BMI_over_weight", "BMI_under_weight",
    "gender_MALE",
    "race_Black_or_African_American", "race_Unknown", "race_Asian",
    "ethnicity_Hispanic_or_Latino", "ethnicity_Unknown",
    "smoking_status_Current_or_Former",
    "Cancer", "Cardiomyophaties", "Cerebro_vascular_disease", "Chronic_lung_disease",
    "Coronary_artery_disease", "Dementia_before", "Depression", "Diabetes",
    "Heart_failure", "HIV_infection", "HTN", "Kidney_disease",
    "liver_disease", "Myocardial_infraction", "Peripheral_vascular_disease", "Rheumatologic_disease"
  )

  # 8. Weighted logistic regression model
  model_formula <- as.formula(
    paste("life_thretening ~ trt_combine +", paste(covariates_for_model, collapse = " + "))
  )

  design <- svydesign(ids = ~1, data = df, weights = ~weights_trimmed)
  model_fit <- svyglm(model_formula, design = design, family = quasibinomial())

  # 9. Tidy result
  tidy_model <- tidy(model_fit, conf.int = TRUE, conf.level = 0.95, exponentiate = TRUE) %>%
    mutate(
      term = ifelse(term == "(Intercept)", "Intercept", term),
      OR_CI = paste0(round(estimate, 2), " (", round(conf.low, 2), " - ", round(conf.high, 2), ")"),
      p.value = format.pval(p.value, digits = 3, eps = .Machine$double.eps)
    ) %>%
    select(Covariate = term, Odds_Ratio = estimate, CI_95 = OR_CI, p_value = p.value)

  return(tidy_model)
}

