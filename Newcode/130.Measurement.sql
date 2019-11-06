/**************************************
 --encoding : UTF-8
 --Author: SC You
 --Date: 2018.09.15
 
 @NHISNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHID_JK: JK table in NHIS NSC
 @NHID_20T: 20 table in NHIS NSC
 @NHID_30T: 30 table in NHIS NSC
 @NHID_40T: 40 table in NHIS NSC
 @NHID_60T: 60 table in NHIS NSC
 @NHID_GJ: GJ table in NHIS NSC
 --Description: Create MEASUREMENT table
 --Generating Table: MEASUREMENT
***************************************/

/**************************************
 0. Create table
***************************************/ 
/*
IF OBJECT_ID('NHIDNSC_database.MEASUREMENT', 'U') IS NULL
CREATE TABLE NHIDNSC_database.MEASUREMENT
    (
     measurement_id						NUMBER						NOT NULL , 
     person_id							INTEGER						NOT NULL ,
     measurement_concept_id				INTEGER						NOT NULL ,
     measurement_date					DATE						NOT NULL ,
     measurement_time					TIME						NULL,  
     measurement_type_concept_id		integer		 				NULL,  
	 operator_concept_id				integer		 				NULL,  
	 value_as_number					float		 				NULL,
	 value_as_concept_id				integer		 				NULL,
	 unit_concept_id					integer						NULL,
	 range_low							float						NULL,
	 range_high							float						NULL,
	 provider_id						integer						NULL,
	 visit_occurrence_id				NUMBER						NULL,
	 measurement_source_value			VARCHAR(50) 				NULL,
	 measurement_source_concept_id		integer						NULL,
	 unit_source_value					VARCHAR(50) 				NULL,
	 value_source_value					VARCHAR(50)					NULL
	);
*/

-- measurement mapping table(temp)

CREATE TABLE measurement_mapping
    (
     meas_type						varchar(50)					NULL , 
     id_value						varchar(50)					NULL ,
     answer							NUMBER						NULL ,
     measurement_concept_id			NUMBER						NULL ,
	 measurement_type_concept_id	NUMBER						NULL ,
	 measurement_unit_concept_id	NUMBER						NULL ,
	 value_as_concept_id			bigint						NULL ,
	 value_as_number				float						NULL 
	)
;

	

INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HEIGHT',			'01',	0,	3036277,	44818701,	4122378,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('WEIGHT',			'02',	0,	3025315,	44818701,	4122383,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('WAIST',				'03',	0,	3016258,	44818701,	4122378,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BP_HIGH',			'04',	0,	3028737,	44818701,	4118323,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BP_LWST',			'05',	0,	3012888,	44818701,	4118323,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BLDS',				'06',	0,	46235168,	44818702,	4121396,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('TOT_CHOLE',			'07',	0,	3027114,	44818702,	4121396,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('TRIGLYCERIDE',		'08',	0,	3022038,	44818702,	4121396,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HDL_CHOLE',			'09',	0,	3023752,	44818702,	4121396,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('LDL_CHOLE',			'10',	0,	3028437,	44818702,	4121396,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HMG',				'11',	0,	3000963,	44818702,	4121395,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	1,	3009261,	44818702,	NULL,		9189,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	2,	3009261,	44818702,	NULL,		4127785,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	3,	3009261,	44818702,	NULL,		4123508,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	4,	3009261,	44818702,	NULL,		4126673,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	5,	3009261,	44818702,	NULL,		4125547,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	6,	3009261,	44818702,	NULL,		4126674,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	1,	437038,		44818702,	NULL,		9189,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	2,	437038,		44818702,	NULL,		4127785,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	3,	437038,		44818702,	NULL,		4123508,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	4,	437038,		44818702,	NULL,		4126673,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	5,	437038,		44818702,	NULL,		4125547,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	6,	437038,		44818702,	NULL,		4126674,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PH',			'14',	0,	3015736,	44818702,	8482,		NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	1,	3014051,	44818702,	NULL,		9189,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	2,	3014051,	44818702,	NULL,		4127785,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	3,	3014051,	44818702,	NULL,		4123508,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	4,	3014051,	44818702,	NULL,		4126673,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	5,	3014051,	44818702,	NULL,		4125547,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	6,	3014051,	44818702,	NULL,		4126674,	NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('CREATININE',		'16',	0,	2212294,	44818702,	4121396,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('SGOT_AST',			'17',	0,	2212597,	44818702,	4118000,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('SGPT_ALT',			'18',	0,	2212598,	44818702,	4118000,	NULL,		NULL)
INTO measurement_mapping VALUES (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GAMMA_GTP',			'19',	0,	4289475,	44818702,	4118000,	NULL,		NULL)
																																																																				

/**************************************																																							   
 1. Rotate rows to columns 
***************************************/ 
/*
select hchk_year, person_id, ykiho_gubun_cd, meas_type, meas_value into @NHISNSC_database.GJ_VERTICAL
from @NHISNSC_rawdata.@NHIS_GJ
unpivot (meas_value for meas_type in ( -- 47 검진 항목
	height, weight, waist, bp_high, bp_lwst,
	blds, tot_chole, triglyceride, hdl_chole, ldl_chole,
	hmg, gly_cd, olig_occu_cd, olig_ph, olig_prote_cd,
	creatinine, sgot_ast, sgpt_alt, gamma_gtp, hchk_pmh_cd1,
	hchk_pmh_cd2, hchk_pmh_cd3, hchk_apop_pmh_yn, hchk_hdise_pmh_yn, hchk_hprts_pmh_yn,
	hchk_diabml_pmh_yn, hchk_hplpdm_pmh_yn, hchk_etcdse_pmh_yn, hchk_phss_pmh_yn, fmly_liver_dise_patien_yn, 
	fmly_hprts_patien_yn, fmly_apop_patien_yn, fmly_hdise_patien_yn, fmly_diabml_patien_yn, fmly_cancer_patien_yn, 
	smk_stat_type_rsps_cd, smk_term_rsps_cd, cur_smk_term_rsps_cd, cur_dsqty_rsps_cd, past_smk_term_rsps_cd, 
	past_dsqty_rsps_cd, dsqty_rsps_cd, drnk_habit_rsps_Cd, tm1_drkqty_rsps_cd, exerci_freq_rsps_cd, 
	mov20_wek_freq_id, mov30_wek_freq_id, wlk30_wek_freq_id
)) as unpivortn
;
*/

/**************************************
 2. Insert continuous data
***************************************/ 
INSERT INTO cohort_cdm.MEASUREMENT (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_time, measurement_type_concept_id, operator_concept_id, value_as_number, value_as_concept_id,			
											unit_concept_id, range_low, range_high, provider_id, visit_occurrence_id, measurement_source_value, measurement_source_concept_id, unit_source_value, value_source_value)


	select	to_date(c.master_seq, b.id_value) as measurement_id,
			a.person_id as person_id,
			b.measurement_concept_id as measurement_concept_id,
			to_date(a.hchk_year||'0101', 'yyyymmdd') as measurement_date,
			measurement_time = null,
			b.measurement_type_concept_id as measurement_type_concept_id,
			operator_concept_id = null,
			b.value_as_number as value_as_number,
			b.value_as_concept_id as value_as_concept_id,
			b.measurement_unit_concept_id as unit_concept_id ,
			range_low = null,
			range_high = null,
			provider_id = null,
			c.master_seq as visit_occurrence_id,
			a.meas_value as measurement_source_value,
			measurement_source_concept_id =null,
			unit_source_value = null,
			a.meas_value as value_source_value

	from (select hchk_year, person_id, ykiho_gubun_cd, meas_type, meas_value 			
			from cohort_cdm.GJ_VERTICAL) a
		JOIN measurement_mapping b 
		on isnull(a.meas_type,'') = isnull(b.meas_type,'') 
			and isnull(a.meas_value,'0') >= isnull(cast(b.answer as char),'0')
		JOIN cohort_cdm.SEQ_MASTER c
		on a.person_id = cast(c.person_id as char)
			and a.hchk_year = c.hchk_year
	where (a.meas_value != '' and substring(a.meas_type, 1, 30) in ('HEIGHT', 'WEIGHT',	'WAIST', 'BP_HIGH', 'BP_LWST', 'BLDS', 'TOT_CHOLE', 'TRIGLYCERIDE',	'HDL_CHOLE',		
																	'LDL_CHOLE', 'HMG', 'OLIG_PH', 'CREATININE', 'SGOT_AST', 'SGPT_ALT', 'GAMMA_GTP')
			and c.source_table like 'GJT')
;

	

/**************************************
 2. Insert categorical data
***************************************/ 
INSERT INTO cohort_cdm.MEASUREMENT (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_time, measurement_type_concept_id, operator_concept_id, value_as_number, value_as_concept_id,			
											unit_concept_id, range_low, range_high, provider_id, visit_occurrence_id, measurement_source_value, measurement_source_concept_id, unit_source_value, value_source_value)


	select	cast(concat(c.master_seq, b.id_value) as bigint) as measurement_id,
			a.person_id as person_id,
			b.measurement_concept_id as measurement_concept_id,
			to_date(a.hchk_year||'0101', 'yyyymmdd') as measurement_date,
			measurement_time = null,
			b.measurement_type_concept_id as measurement_type_concept_id,
			operator_concept_id = null,
			b.value_as_number as value_as_number,
			b.value_as_concept_id as value_as_concept_id,
			b.measurement_unit_concept_id as unit_concept_id ,
			range_low = null,
			range_high = null,
			provider_id = null,
			c.master_seq as visit_occurrence_id,
			a.meas_value as measurement_source_value,
			measurement_source_concept_id =null,
			unit_source_value = null,
			a.meas_value as value_source_value

	from (select hchk_year, person_id, ykiho_gubun_cd, meas_type, meas_value 			
			from cohort_cdm.GJ_VERTICAL) a
		JOIN #measurement_mapping b 
		on isnull(a.meas_type,'') = isnull(b.meas_type,'') 
			and isnull(a.meas_value,'0') = isnull(cast(b.answer as char),'0')
		JOIN cohort_cdm.SEQ_MASTER c
		on a.person_id = cast(c.person_id as char)
			and a.hchk_year = c.hchk_year
	where (a.meas_value != '' and substring(a.meas_type, 1, 30) in ('GLY_CD', 'OLIG_OCCU_CD', 'OLIG_PROTE_CD')
			and c.source_table like 'GJT')
;

/**************************************
 3.Insert source_value into vlaue_as_number
***************************************/
/* 
UPDATE @NHISNSC_database.MEASUREMENT
SET value_as_number = measurement_source_value
where measurement_source_value is not null
;
*/
