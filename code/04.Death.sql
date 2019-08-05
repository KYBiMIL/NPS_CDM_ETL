/**************************************
 --encoding : UTF-8
 --Author: 조재형
 --Date: 2017.02.06
 
 cohort_cdm : DB containing NHIS National Sample cohort DB
 NHID_JK: JK table in NHIS NSC
 NHID_20T: 20 table in NHIS NSC
 NHID_30T: 30 table in NHIS NSC
 NHID_40T: 40 table in NHIS NSC
 NHID_60T: 60 table in NHIS NSC
 NHID_GJ: GJ table in NHIS NSC
 --Description: DEATH 테이블 생성
			   1) 표본코호트DB에는 사망한 날짜가 년도, 월까지 표시가 되기 때문에 해당 월의 1일로 사망일 정의
			   2) 표본코호트DB는 사망한 후에도 진료기록이 있는 경우가 있음을 고려
			   3) 범위(A00-A15), J46 등 매핑 안되는 code들 insert(#death_mapping)
 --Generating Table: DEATH
***************************************/

/**************************************
 1. 테이블 생성
***************************************/  

-- death table 생성
CREATE TABLE  cohort_cdm.DEATH
(
    person_id							INTEGER			NOT NULL , 
    death_date							DATE			NOT NULL , 
    death_type_concept_id				INTEGER			NOT NULL , 
    cause_concept_id					INTEGER			NULL , 
    cause_source_value					VARCHAR(500)	NULL,
	cause_source_concept_id				INTEGER			NULL,
	primary key (person_id)
);

-- 임시 death mapping table
create global temporary table death_mapping
(
KCDCODE VARCHAR(20),
NAME varchar(255),
CONCEPT_ID INTEGER, 
CONCEPT_NAME varchar(255)
)
on commit preserve rows;

INSERT all
INTO death_mapping VALUES ('A00-A09','infectious enteritis', 4134887, 'Infectious disease of digestive tract')
INTO death_mapping VALUES ('A15-A19','tuberculosis', 434557, 'Tuberculosis')
INTO death_mapping VALUES ('A30-A49','기타 박테리아 감염', 432545, 'Bacterial infectious disease')
INTO death_mapping VALUES ('A50-A64','sexually tranmitted disease', 440647, 'Sexually transmitted infectious disease')
INTO death_mapping VALUES ('A75-A79','열성질환(typhus, tsutsugamushi, spotted fever....)', 432545, 'Bacterial infectious disease')
INTO death_mapping VALUES ('A80-A89','CNS 감염 질환', 4028070, 'Infectious disease of central nervous system')
INTO death_mapping VALUES ('A90-A99','viral hemorrhagic fever', 4347554, 'Viral hemorrhagic fever')
INTO death_mapping VALUES ('B00-B09','viral infection', 440029, 'Viral disease')
INTO death_mapping VALUES ('B15-B19','viral liver disease', 4291005, 'Viral hepatitis')
INTO death_mapping VALUES ('B20-B24','AIDS-associated disorder', 4221489, 'AIDS-associated disorder')
INTO death_mapping VALUES ('B25-B34','viral infection', 440029, 'Viral disease')
INTO death_mapping VALUES ('B35-B49','진균증', 433701, 'Mycosis')
INTO death_mapping VALUES ('B50-B64','원충증', 442176, 'Protozoan infection')
INTO death_mapping VALUES ('B65-B83','기생충병', 432251, 'Disease caused by parasite')
INTO death_mapping VALUES ('B90-B94','감염질환에 의한 후유증', 444201, 'Post-infectious disorder')
INTO death_mapping VALUES ('F00-F09','정신과 질환 (기질성)', 374009, 'Organic mental disorder')
INTO death_mapping VALUES ('F10-F19','약물 오남용 관련 질환', 40483111, 'Mental disorder due to drug')
INTO death_mapping VALUES ('F20-F29','조현병 관련 질환', 436073, 'Psychotic disorder')
INTO death_mapping VALUES ('F30-F39','우울증/ 조증 관련 질환', 444100, 'Mood disorder')
INTO death_mapping VALUES ('F40-F48','neurosis', 444243, 'Neurosis')
INTO death_mapping VALUES ('F50-F59','행동장애', 4333000, 'Behavioral syndrome associated with physiological disturbance and physical factors')
INTO death_mapping VALUES ('F70-F79','정신 지체', 440389, 'Mental retardation')
INTO death_mapping VALUES ('F80-F89','발달 장애', 435244, 'Developmental disorder')
INTO death_mapping VALUES ('F99-F99','Mental disorder', 432586, 'Mental disorder')
INTO death_mapping VALUES ('J46','Status asthmaticus', 4145356, 'Severe persistent asthma')
INTO death_mapping VALUES ('S00-S09','두부 외상', 375415, 'Injury of head')
INTO death_mapping VALUES ('S10-S19','목부위 외상', 24818, 'Injury of neck')
INTO death_mapping VALUES ('S20-S29','흉부 외상', 4094683, 'Chest injury')
INTO death_mapping VALUES ('S30-S39','복부/골반 외상', 200588, 'Injury of abdomen')
INTO death_mapping VALUES ('S40-S49','위팔/어깨 외상', 4130851, 'Injury of upper extremity')
INTO death_mapping VALUES ('S50-S59','forearm 외상', 136779, 'Disorder of forearm')
INTO death_mapping VALUES ('S60-S69','수부 외상', 80004, 'Injury of hand')
INTO death_mapping VALUES ('S70-S79','hip/thigh 외상', 4130852, 'Injury of lower extremity')
INTO death_mapping VALUES ('S80-S89','무릎/lower leg 외상', 444131, 'Injury of lower leg')
INTO death_mapping VALUES ('T00-T07','다발성 외상', 440921, 'Traumatic injury')
INTO death_mapping VALUES ('T08-T14','척추 및 사지 손상', 4022201, 'Injury of musculoskeletal system')
INTO death_mapping VALUES ('T15-T19','foreign body', 4053838, 'Foreign body')
INTO death_mapping VALUES ('T20-T25','화상 (피부)', 4123196, 'Burn of skin of body region')
INTO death_mapping VALUES ('T26-T28','화상 (내부기관)', 198030, 'Burn of internal organ')
INTO death_mapping VALUES ('T29-T32','기타 화상', 442013, 'Burn')
INTO death_mapping VALUES ('T33-T35','동상', 441487, 'Frostbite')
INTO death_mapping VALUES ('T36-T50','poisoning (마약/약물)', 438028, 'Poisoning by drug AND/OR medicinal substance')
INTO death_mapping VALUES ('T51-T65','기타 중독', 40481346, 'Poisoning due to chemical substance')
INTO death_mapping VALUES ('T66-T78','외부 환경에 의한 영향', 4167864, 'Effect of exposure to physical force')
INTO death_mapping VALUES ('T79-T79','달리 분류되지 않은 외상의 특정 조기합병증', 4211546, 'Traumatic complication of injury')
INTO death_mapping VALUES ('T80-T88','의료 행위에 대한 합병증', 442019, 'Complication of procedure')
INTO death_mapping VALUES ('T90-T98','기타 후유증', 443403, 'Sequela')
select 1 from dual;

select * from death_mapping;
/**************************************
 2. 데이터 입력 및 확인
***************************************/  

--날짜를 해당 월의 말일로 정의
INSERT INTO cohort_cdm.DEATH (person_id, death_date, death_type_concept_id, cause_concept_id, 
cause_source_value, cause_source_concept_id)
SELECT a.person_id AS PERSON_ID,
	to_char(SYSDATE - 'DATEPART' DAY,(SYSDATE - 'DD' MONTH,1,to_char(a.dth_ym || '01',23)),SYSDATE - MONTH,1,to_char(a.dth_ym || '01',23)),23 AS DEATH_DATE,
	38003618 as death_type_concept_id,
	b.concept_id as cause_concept_id,
	dth_code1 as cause_source_value,
	NULL as cause_source_concept_id
FROM cohort_cdm.NHID_JK a left join death_mapping b
on a.dth_code1=b.kcdcode
WHERE a.dth_ym IS NOT NULL and a.dth_ym != '';



--날짜 없는 경우 해당 년의 12월 31일로 death 정의
INSERT INTO cohort_cdm.DEATH (person_id, death_date, death_type_concept_id, cause_concept_id, 
cause_source_value, cause_source_concept_id)
SELECT a.person_id AS PERSON_ID,
	to_char(STND_Y || '1231', 23) AS DEATH_DATE,
	38003618 as death_type_concept_id,
	b.concept_id as cause_concept_id,
	dth_code1 as cause_source_value,
	NULL as cause_source_concept_id
FROM cohort_cdm.NHID_JK a left join death_mapping b
on a.dth_code1=b.kcdcode
WHERE a.dth_ym = '' and a.DTH_CODE1 != '';


--임시매핑테이블 삭제
drop table death_mapping;
