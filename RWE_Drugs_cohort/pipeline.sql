

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bf3aa1d9-b9e5-4a71-bdd0-441f9a724c5c"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
-- Bebtelovimab drug concept IDs 
SELECT *
FROM concept_set_members
where concept_id in (726330,
726665,
727154,
727611,
1759073,
1759074,
1759075,
1759076,
1759077,
1759259,
1999375,
42632508);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.728c5752-e7bf-4e36-a8e5-bf6dfad92daf"),
    Bebtelovimab_concept_ids=Input(rid="ri.foundry.main.dataset.bf3aa1d9-b9e5-4a71-bdd0-441f9a724c5c"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
-- Map concept ids to drug_exposure_data table

SELECT A.concept_id, B.*, 1 as Bebtelovimab 
FROM Bebtelovimab_concept_ids A
inner join drug_exposure B
on A.concept_id = B.drug_concept_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.97cc7743-32d5-4743-9e53-4f1b342a7180"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
-- Molnupiravir drug concept IDs based on codeset id

SELECT *
FROM concept_set_members
where codeset_id = 156103933;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a73b02f7-99bf-4ba4-bb90-08b83ed53faa"),
    Molnupiravir_concept_ids=Input(rid="ri.foundry.main.dataset.97cc7743-32d5-4743-9e53-4f1b342a7180"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
-- Map concept ids to drug_exposure_data table

SELECT A.concept_id, B.* , 1 as Molnupiravir
FROM Molnupiravir_concept_ids A
inner join drug_exposure B
on A.concept_id = B.drug_concept_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8e94d028-1aa7-44b1-a381-72449464f9fc"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
-- Paxlovi drug concept IDs based on codeset id
SELECT *
FROM concept_set_members
where codeset_id = 776778942;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.25bf6f1f-8a7b-497e-9bb4-c25d0d82f28e"),
    Paxlovid_Nirmatrelvir_concept_ids=Input(rid="ri.foundry.main.dataset.8e94d028-1aa7-44b1-a381-72449464f9fc"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
-- Map concept ids to drug_exposure_data table

SELECT A.concept_id, B.* , 1 as Paxlovid
FROM Paxlovid_Nirmatrelvir_concept_ids A
inner join drug_exposure B
on A.concept_id = B.drug_concept_id;

