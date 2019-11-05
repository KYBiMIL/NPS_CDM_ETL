/**************************************
 --encoding : UTF-8
 --Author: JH Cho, JM Park
 --Date: 2018.09.10
 
 @NHIDNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHIDNSC_database : DB for NHIS-NSC in CDM format
 @Mapping_database : DB for mapping table
 @NHID_JK: JK table in NHIS NSC
 @NHID_20T: 20 table in NHIS NSC
 @NHID_30T: 30 table in NHIS NSC
 @NHID_40T: 40 table in NHIS NSC
 @NHID_60T: 60 table in NHIS NSC
 @NHID_GJ: GJ table in NHIS NSC
 --Description: Create Death table
 				1) In sample cohort DB, death dates are recorded with year and month, not date, therefore, define death date as last day of death month
				2) Consider the cases with clinical diagnosis after death
			   	3) A00-A15, J46 and other unmapped codes need to be inserted to mapping table(#death mapping)
 --Generating Table: DEATH
***************************************/

/**************************************
 1. Create table
***************************************/  
/*
-- death table
CREATE TABLE cohort_cdm.DEATH
(
    person_id							INTEGER			NOT NULL , 
    death_date							DATE			NOT NULL , 
    death_type_concept_id				INTEGER			NOT NULL , 
    cause_concept_id					INTEGER			NULL , 
    cause_source_value					VARCHAR(500)	NULL,
	cause_source_concept_id				INTEGER			NULL,
	primary key (person_id)
);
*/
create global temporary table death_mapping
(
KCDCODE VARCHAR(20),
NAME varchar(255),
CONCEPT_ID INTEGER, 
CONCEPT_NAME varchar(255)
)
on commit preserve rows;


-- temp death mapping table  
 SELECT	source_code, source_code_description, target_concept_id
		INTO #DEATH_MAPPINGTABLE
from cohort_cdm.source_to_concept_map a join cohort_cdm.CONCEPT b on a.target_concept_id=b.concept_id
where a.domain_id='condition' and b.domain_id='condition'
	and a.target_concept_id=b.concept_id
	and a.invalid_reason is null and b.invalid_reason is null;

--Insert additional death data to temp death mapping table
INSERT all
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('A00-A09', 4134887, 'Infectious disease of digestive tract') -- 104180 적용됨, 나머지는 1행씩 적용됨
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('A15-A19', 434557, 'Tuberculosis')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('A30-A49', 432545, 'Bacterial infectious disease')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('A50-A64', 440647, 'Sexually transmitted infectious disease')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('A75-A79', 432545, 'Bacterial infectious disease')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('A80-A89', 4028070, 'Infectious disease of central nervous system')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('A90-A99', 4347554, 'Viral hemorrhagic fever')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('B00-B09', 440029, 'Viral disease')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('B15-B19', 4291005, 'Viral hepatitis')
insert INTO death_mapping VALUES(source_code,  target_concept_id, source_code_description) values ('B20-B24', 4221489, 'AIDS-associated disorder')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('B25-B34', 440029, 'Viral disease')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('B35-B49', 433701, 'Mycosis')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('B50-B64', 442176, 'Protozoan infection')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('B65-B83', 432251, 'Disease caused by parasite')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('B90-B94', 444201, 'Post-infectious disorder')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F00-F09', 374009, 'Organic mental disorder')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F10-F19', 40483111, 'Mental disorder due to drug')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F20-F29', 436073, 'Psychotic disorder')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F30-F39', 444100, 'Mood disorder')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F40-F48', 444243, 'Neurosis')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F50-F59', 4333000, 'Behavioral syndrome associated with physiological disturbance and physical factors')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F70-F79', 440389, 'Mental retardation')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F80-F89', 435244, 'Developmental disorder')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('F99-F99', 432586, 'Mental disorder')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('J46', 4145356, 'Severe persistent asthma')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S00-S09', 375415, 'Injury of head')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S10-S19', 24818, 'Injury of neck')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S20-S29', 4094683, 'Chest injury')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S30-S39', 200588, 'Injury of abdomen')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S40-S49', 4130851, 'Injury of upper extremity')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S50-S59', 136779, 'Disorder of forearm')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S60-S69', 80004, 'Injury of hand')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S70-S79', 4130852, 'Injury of lower extremity')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('S80-S89', 444131, 'Injury of lower leg')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T00-T07', 440921, 'Traumatic injury')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T08-T14', 4022201, 'Injury of musculoskeletal system')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T15-T19', 4053838, 'Foreign body')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T20-T25', 4123196, 'Burn of skin of body region')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T26-T28', 198030, 'Burn of internal organ')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T29-T32', 442013, 'Burn')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T33-T35', 441487, 'Frostbite')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T36-T50', 438028, 'Poisoning by drug AND/OR medicinal substance')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T51-T65', 40481346, 'Poisoning due to chemical substance')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T66-T78', 4167864, 'Effect of exposure to physical force')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T79-T79', 4211546, 'Traumatic complication of injury')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T80-T88', 442019, 'Complication of procedure')
insert INTO death_mapping VALUES (source_code, target_concept_id, source_code_description) values ('T90-T98', 443403, 'Sequela')
select 1 from dual;

select * from death_mapping;

/**************************************
 2. Insert data
***************************************/  
-- Define the last date of death month as death date
INSERT INTO cohort_cdm.DEATH (person_id, death_date, death_type_concept_id, cause_concept_id, 
cause_source_value, cause_source_concept_id)
SELECT a.person_id AS PERSON_ID,
	to_char(last_day(to_date(dth_ym||'01','yyyymmdd')),'yyyymmdd') as death_date,
	38003618 as death_type_concept_id,
	b.target_concept_id as cause_concept_id,
	dth_code1 as cause_source_value,
	NULL as cause_source_concept_id
FROM cohort_cdm.NHID_JK a left join DEATH_MAPPINGTABLE b
on a.dth_code1=b.source_code
WHERE a.dth_ym IS NOT NULL and a.dth_ym != ''
;

-- If there is no death month, define 12.31 as death month and date 
INSERT INTO cohort_cdm.DEATH (person_id, death_date, death_type_concept_id, cause_concept_id, 
cause_source_value, cause_source_concept_id)
SELECT a.person_id AS PERSON_ID,
	STND_Y || '1231' ,'yyyymmdd') AS DEATH_DATE,
	38003618 as death_type_concept_id,
	b.target_concept_id as cause_concept_id,
	dth_code1 as cause_source_value,
	NULL as cause_source_concept_id
FROM cohort_cdm.NHID_JK a left join DEATH_MAPPINGTABLE b
on a.dth_code1=b.source_code
WHERE a.dth_ym = '' and a.DTH_CODE1 != ''
;

--Delete temp death mapping table
drop table #DEATH_MAPPINGTABLE;
