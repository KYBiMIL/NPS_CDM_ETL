/**************************************
 --encoding : UTF-8
 --Author: 이성원
 --Date: 2017.02.13
 
cohort_cdm : DB containing NHIS National Sample cohort DB
cohort_cdm : DB for NHIS-NSC in CDM format
NHID_JK: JK table in NHIS NSC
NHID_20T: 20 table in NHIS NSC
NHID_30T: 30 table in NHIS NSC
NHID_40T: 40 table in NHIS NSC
NHID_60T: 60 table in NHIS NSC
NHID_GJ: GJ table in NHIS NSC
CONDITION_MAPPINGTABLE : mapping table between KCD and OMOP vocabulary
DRUG_MAPPINGTABLE : mapping table between EDI and OMOP vocabulary
PROCEDURE_MAPPINGTABLE : mapping table between Korean procedure and OMOP vocabulary
 
 --Description: Procedure_occurrence 테이블 생성
			   * 30T(진료), 60T(처방전) 테이블에서 각각 ETL을 수행해야 함
 --Generating Table: PROCEDURE_OCCURRENCE
***************************************/

/**************************************
 1. 변환 데이터 건수 파악
***************************************/ 
-- 30T 변환 예상 건수(1:N 매핑 허용)
select count(a.key_seq)
from cohort_cdm.NHID_30T a, procedure_edi_mapped_20161007 b, cohort_cdm.NHID_20T c
where a.div_cd=b.sourcecode
and a.key_seq=c.key_seq

-- 참고) 30T 변환 예상 건수 (distinct 용어만 카운트)
select count(a.key_seq)
from cohort_cdm.NHID_30T a, cohort_cdm.NHID_20T b
where a.key_seq=b.key_seq
and a.div_cd in (select distinct sourcecode
	from procedure_edi_mapped_20161007)
	
-- 참고) 30T 중 1:N 매핑 중복 건수
select count(a.key_seq), sum(cnt)
from cohort_cdm.NHID_30T a, 
	(select sourcecode, count(sourcecode)-1 as cnt 
	from procedure_edi_mapped_20161007 
	group by sourcecode 
	having count(sourcecode) > 1) b
where a.div_cd=b.sourcecode
-- 1,168,437

----------------------------------------
-- 60T 변환 예상 건수(1:N 매핑 허용)
select count(a.key_seq)
from cohort_cdm.[drug02_13_60T] a, procedure_edi_mapped_20161007 b, cohort_cdm.NHID_20T c
where a.div_cd=b.sourcecode
and a.key_seq=c.key_seq

-- 참고) 60T 변환 예상 건수 (distinct 용어만 카운트)
select count(a.key_seq)
from cohort_cdm.[drug02_13_60T] a, cohort_cdm.NHID_20T b
where a.key_seq=b.key_seq
and a.div_cd in (select distinct sourcecode
	from procedure_edi_mapped_20161007)

-- 참고) 60T 중 1:N 매핑 중복 건수
select count(a.key_seq), sum(cnt)
from cohort_cdm.[drug02_13_60T] a, 
	(select sourcecode, count(sourcecode)-1 as cnt 
	from procedure_edi_mapped_20161007 
	group by sourcecode 
	having count(sourcecode) > 1) b,
	cohort_cdm.NHID_20T c
where a.div_cd=b.sourcecode
and a.key_seq=c.key_seq
-- 1건


/**************************************
 2. 테이블 생성
***************************************/ 
CREATE TABLE cohort_cdm.PROCEDURE_OCCURRENCE ( 
     procedure_occurrence_id		BIGINT			PRIMARY KEY, 
     person_id						INTEGER			NOT NULL, 
     procedure_concept_id			INTEGER			NOT NULL, 
     procedure_date					DATE			NOT NULL, 
     procedure_type_concept_id		INTEGER			NOT NULL,
	 modifier_concept_id			INTEGER			NULL,
	 quantity						INTEGER			NULL, 
     provider_id					INTEGER			NULL, 
     visit_occurrence_id			BIGINT			NULL, 
     procedure_source_value			VARCHAR(50)		NULL,
	 procedure_source_concept_id	INTEGER			NULL,
	 qualifier_source_value			VARCHAR(50)		NULL
    )
;

CREATE TABLE cohort_cdm.PROCEDURE_OCCURRENCE ( 
     procedure_occurrence_id		NUMBER			PRIMARY KEY, 
     person_id						INTEGER			NOT NULL, 
     procedure_concept_id			INTEGER			NOT NULL, 
     procedure_date					DATE			NOT NULL, 
     procedure_type_concept_id		INTEGER			NOT NULL,
	 modifier_concept_id			INTEGER			NULL,
	 quantity						INTEGER			NULL, 
     provider_id					INTEGER			NULL, 
     visit_occurrence_id			NUMBER			NULL, 
     procedure_source_value			VARCHAR(50)		NULL,
	 procedure_source_concept_id	INTEGER			NULL,
	 qualifier_source_value			VARCHAR(50)		NULL
    );

/**************************************
 3. 30T를 이용하여 데이터 입력
***************************************/
INSERT INTO cohort_cdm.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id, qualifier_source_value)
SELECT 
	to_number(to_char(a.master_seq) || to_char(row_number() over (partition by a.key_seq, a.seq_no order by b.concept_id))) as procedure_occurrence_id,
	a.person_id as person_id,
	CASE WHEN b.concept_id IS NOT NULL THEN b.concept_id ELSE 0 END as procedure_concept_id,
	to_char(a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	to_float(a.dd_mqty_exec_freq) * to_float(a.mdcn_exec_freq) * to_float(a.dd_mqty_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id,
	null as qualifier_source_value
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_mqty_exec_freq is not null and isnumeric(x.dd_mqty_exec_freq)=1 and cast(x.dd_mqty_exec_freq as float) > '0' then cast(x.dd_mqty_exec_freq as float) else 1 end as dd_mqty_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM cohort_cdm.NHID_30T x, 
		 (select master_seq, key_seq, seq_no, person_id from seq_master where source_table='130') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a, procedure_EDI_mapped_20161007 b
WHERE a.div_cd=b.sourcecode


/**************************************
 4. 60T를 이용하여 데이터 입력
***************************************/
INSERT INTO cohort_cdm.PROCEDURE_OCCURRENCE 
	(procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, 
	modifier_concept_id, quantity, provider_id, visit_occurrence_id, procedure_source_value, 
	procedure_source_concept_id, qualifier_source_value)
SELECT 
	to_number( to_char(a.master_seq) || to_char(row_number() over (partition by a.key_seq, a.seq_no order by b.concept_id))) as procedure_occurrence_id,
	a.person_id as person_id,
	CASE WHEN b.concept_id IS NOT NULL THEN b.concept_id ELSE 0 END as procedure_concept_id,
	to_char(a.recu_fr_dt, 112) as procedure_date,
	45756900 as procedure_type_concept_id,
	NULL as modifier_concept_id,
	to_float(a.dd_mqty_freq) * to_float(a.dd_exec_freq) * to_float(a.mdcn_exec_freq) as quantity,
	NULL as provider_id,
	a.key_seq as visit_occurrence_id,
	a.div_cd as procedure_source_value,
	null as procedure_source_concept_id,
	null as qualifier_source_value
FROM (SELECt x.key_seq, x.seq_no, x.recu_fr_dt, x.div_cd, 
			case when x.mdcn_exec_freq is not null and isnumeric(x.mdcn_exec_freq)=1 and cast(x.mdcn_exec_freq as float) > '0' then cast(x.mdcn_exec_freq as float) else 1 end as mdcn_exec_freq,
			case when x.dd_exec_freq is not null and isnumeric(x.dd_exec_freq)=1 and cast(x.dd_exec_freq as float) > '0' then cast(x.dd_exec_freq as float) else 1 end as dd_exec_freq,
			case when x.dd_mqty_freq is not null and isnumeric(x.dd_mqty_freq)=1 and cast(x.dd_mqty_freq as float) > '0' then cast(x.dd_mqty_freq as float) else 1 end as dd_mqty_freq,
			y.master_seq, y.person_id
	FROM cohort_cdm.NHID_60T x, 
		 (select master_seq, key_seq, seq_no, person_id from seq_master where source_table='160') y
	WHERE x.key_seq=y.key_seq
	AND x.seq_no=y.seq_no) a, cohort_cdm.PROCEDURE_MAPPINGTABLE b
WHERE a.div_cd=b.sourcecode
