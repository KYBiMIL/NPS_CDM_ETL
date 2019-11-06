/**************************************
 --encoding : UTF-8
 --Author: JH Cho
 --Date: 2018.09.15
 
 @NHIDNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHIDNSC_database : DB for NHIS-NSC in CDM format
 @NHID_JK: JK table in NHIS NSC
 @NHID_20T: 20 table in NHIS NSC
 @NHID_30T: 30 table in NHIS NSC
 @NHID_40T: 40 table in NHIS NSC
 @NHID_60T: 60 table in NHIS NSC
 @NHID_GJ: GJ table in NHIS NSC
 @CONDITION_MAPPINGTABLE : mapping table between KCD and OMOP vocabulary
 @DRUG_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 @PROCEDURE_MAPPINGTABLE : mapping table between Korean procedure and OMOP vocabulary
 @DEVICE_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
 
 --Description: Create PAYER_PLAN_PERIOD table
			   1) payer_plan_period_id = Define person_id as person_id + year
			   2) payer_plan_period_start_date = Define as the 01 Jan of the year 
			   3) payer_plan_period_end_date = Define as the 31 Dec of the year or the death date
 --Generating Table: PAYER_PLAN_PERIOD
***************************************/

/**************************************
 1. Create table
***************************************/ 
/*
CREATE TABLE cohort_cdm.PAYER_PLAN_PERIOD
    (
     payer_plan_period_id				NUMBER						NOT NULL , 
     person_id							INTEGER						NOT NULL ,
     payer_plan_period_start_date		DATE						NOT NULL ,
     payer_plan_period_end_date			DATE						NOT NULL ,
     payer_source_value					VARCHAR(50) 				NULL,  
     plan_source_value					VARCHAR(50) 				NULL,  
	 family_source_value				VARCHAR(50) 				NULL   
	)
 ; -- DROP TABLE cohort_cdm.PAYER_PLAN_PERIOD
*/ 
 
/**************************************
 2. Insert data 
***************************************/  

INSERT INTO cohort_cdm.PAYER_PLAN_PERIOD (payer_plan_period_id, person_id, payer_plan_period_start_date, payer_plan_period_end_date, payer_source_value, plan_source_value, family_source_value)
	SELECT	a.person_id+STND_Y as payer_plan_period_id,
			a.person_id as person_id,
			to_date(STND_Y || '0101' ,'yyyymmdd') as payer_plan_period_start_date,
			case when year < death_date then a.year
			when year > death_date then death_date
			else a.year
			end as payer_plan_period_end_date,
			payer_source_value = 'National Health Insurance Service',
			IPSN_TYPE_CD as plan_source_value,
			family_source_value = null
	FROM 
			(select person_id, STND_Y, IPSN_TYPE_CD, to_date(STND_Y as varchar) || '1231' ,'yyyymmdd') as year from cohort_cdm.NHID_JK) a left join cohort_cdm.DEATH b
	  		on a.person_id=b.person_id
