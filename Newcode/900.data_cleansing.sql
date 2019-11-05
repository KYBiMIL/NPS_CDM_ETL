/******************************************************
Data Cleansing
JM Park
@NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
@NHISNSC_database : DB for NHIS-NSC in CDM format
@NHIS_JK: JK table in NHIS NSC
@NHIS_20T: 20 table in NHIS NSC
@NHIS_30T: 30 table in NHIS NSC
@NHIS_40T: 40 table in NHIS NSC
@NHIS_60T: 60 table in NHIS NSC
@NHIS_GJ: GJ table in NHIS NSC
******************************************************/

/********************************************************
		Delete cases outside of Observation_period
********************************************************/
delete from cohort_cdm.VISIT_OCCURRENCE
where visit_occurrence_id not in (
							select visit_occurrence_id
							from cohort_cdm.VISIT_OCCURRENCE a, cohort_cdm.OBSERVATION_PERIOD b
							where a.person_id=b.person_id
								and (visit_start_date >= observation_period_start_date and observation_period_end_date >= visit_end_date) 
							)

delete from cohort_cdm.CONDITION_OCCURRENCE
where condition_occurrence_id not in (
						select condition_occurrence_id
						from cohort_cdm.CONDITION_OCCURRENCE a, cohort_cdm.OBSERVATION_PERIOD b
						where a.person_id=b.person_id
							and (a.condition_start_date >= b.observation_period_start_date and a.condition_end_date <= b.observation_period_end_date)
							)

delete from cohort_cdm.DRUG_EXPOSURE
where drug_exposure_id not in (
							select drug_exposure_id
							from cohort_cdm.DRUG_EXPOSURE a, cohort_cdm.OBSERVATION_PERIOD b
							where a.person_id=b.person_id
								and (a.drug_exposure_start_date >= b.observation_period_start_date and a.drug_exposure_end_date <= b.observation_period_end_date)
								)

delete from cohort_cdm.PROCEDURE_OCCURRENCE
where procedure_occurrence_id not in (
									select procedure_occurrence_id
									from cohort_cdm.PROCEDURE_OCCURRENCE a, cohort_cdm.OBSERVATION_PERIOD b
									where a.person_id=b.person_id
										and (procedure_date >= observation_period_start_date and procedure_date <= observation_period_end_date)
										)

delete from cohort_cdm.DEVICE_EXPOSURE
where device_exposure_id not in (
							select device_exposure_id
							from cohort_cdm.DEVICE_EXPOSURE a, cohort_cdm.observation_period b
							where a.person_id=b.person_id
								and (a.device_exposure_start_date >= b.observation_period_start_date and a.device_exposure_end_date <= b.observation_period_end_date)
								)

delete from cohort_cdm.MEASUREMENT
where measurement_id not in (
						select measurement_id
						from cohort_cdm.MEASUREMENT a, cohort_cdm.OBSERVATION_PERIOD b
						where a.person_id=b.person_id
							and (a.measurement_date >= b.observation_period_start_date and a.measurement_date <= b.observation_period_end_date)
							)

delete from NHIS_NSC_2019.dbo.PAYER_PLAN_PERIOD
where payer_plan_period_id not in (
							select payer_plan_period_id
							from NHIS_NSC_2019.dbo.PAYER_PLAN_PERIOD a, NHIS_NSC_2019.dbo.observation_period b
							where a.person_id=b.person_id
								and (a.payer_plan_period_start_date >= b.observation_period_start_date and a.payer_plan_period_end_date <= b.observation_period_end_date)
							) 


/********************************************************
Update isuues of Person table of which originated from source data
********************************************************/
-- Change the gender_concept_id and gender_source_value from female to male
update cohort_cdm.PERSON
set gender_concept_id='8507', gender_source_value=1
where person_id = 95292839

/********************************************************
Update quantity from 0 to 1
********************************************************/
update cohort_cdm.DEVICE_EXPOSURE
set quantity = 1
where quantity = 0
;
