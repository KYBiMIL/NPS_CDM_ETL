/**************************************
 --encoding : UTF-8
 --Author: SW Lee, JM Park
 --Date: 2018.08.21
 
 @NHISNSC_raw : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHIS_JK: JK table in NHIS NSC
 @NHIS_20T: 20 table in NHIS NSC
 @NHIS_30T: 30 table in NHIS NSC
 @NHIS_40T: 40 table in NHIS NSC
 @NHIS_60T: 60 table in NHIS NSC
 @NHIS_GJ: GJ table in NHIS NSC
 --Description: Among T1 tables of sample cohort, keep primary keys of 30T, 40T, 60T, GJ, JK table and create table which has unique serial number.
				The serial number is used as primary key of condition, drug, procedure and device tables, and the serial number of GJ table will be used as priomary key of visit_occurrence table.
			   the serial number of JK table will be used as priomary key of observation table.
			   Those keys is created for tracking sample cogort DB in converted CDM DB
 --Generating Table: SEQ_MASTER
***************************************/

/**************************************
 1. Create table
    : serial number(PK), source table, person_id, primary keys of 30T, 40T, 60T, GJ, JK tables
***************************************/  
CREATE TABLE SEQ_MASTER (
	master_seq		NUMBER PRIMARY KEY,
	source_table	CHAR(3) NULL, -- 30T = 130, 40T = 140, 60T = 160 GJ ='GJT', JK = 'JKT'
	person_id		INTEGER	NULL, 
	key_seq			NUMBER	NULL, -- 30T, 40T, 60T
	seq_no			NUMERIC(4)	NULL, -- 30T, 40T, 60T
	hchk_year		CHAR(4)	NULL, -- GJ
	stnd_y			CHAR(4) NULL -- JK 
);

/**************************************
 2. Insert data of 30T
    : serial number is starting from 3000000001
***************************************/
-- 1) Reset the serial number
create SEQUENCE master_seq START WITH 3000000001 increment by 1;

CREATE OR REPLACE TRIGGER master_bir 
BEFORE INSERT ON SEQ_MASTER 
FOR EACH ROW
BEGIN
  SELECT master_seq.NEXTVAL
  INTO   :new.master_seq
  FROM   dual;
END;
-- 2) Insert data
INSERT INTO cohort_cdm.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '130', b.person_id, a.key_seq, a.seq_no
FROM cohort_cdm.NHID_30T a, cohort_cdm.NHID_20T b
WHERE a.key_seq=b.key_seq
;

/**************************************
 3. Insert data of 40T
    : serial number is starting from 4000000001
***************************************/
-- 1) Reset the serial number
CREATE SEQUENCE master_seq START WITH 4000000001 increment by 1;

-- 2) Insert data
INSERT INTO cohort_cdm.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '140', b.person_id, a.key_seq, a.seq_no
FROM cohort_cdm.NHID_40T a, cohort_cdm.NHID_20T b
WHERE a.key_seq=b.key_seq
;

/**************************************
 4. Insert data of 60T
    : serial number is starting from 6000000001
***************************************/
-- 1) Reset the serial number
CREATE SEQUENCE master_seq START WITH 6000000001 increment by 1;

-- 2) Insert data
INSERT INTO cohort_cdm.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '160', b.person_id, a.key_seq, a.seq_no
FROM cohort_cdm.NHID_60T a, cohort_cdm.NHID_20T b
WHERE a.key_seq=b.key_seq
;

/**************************************
 5. Insert data of GJ table
    : serial number is starting from 800000000001
	: visit_occurrence_id is consisted with 12 numbers, match the numbers
***************************************/
-- 1) Reset the serial number
CREATE SEQUENCE master_seq START WITH 800000000001 increment by 1;

-- 2) Insert data
INSERT INTO cohort_cdm.SEQ_MASTER
	(source_table, person_id, hchk_year)
SELECT 'GJT', person_id, hchk_year
FROM cohort_cdm.NHID_GJ
GROUP BY hchk_year, person_id
;
/**************************************
 6. Insert data of JK table
	: serial number is starting from 900000000001
**************************************/
-- 1) Reset the serial number
CREATE SEQUENCE master_seq START WITH 900000000001 increment by 1;

-- 2) Insert data
INSERT INTO cohort_cdm.SEQ_MASTER
	(source_table, person_id, stnd_y)
SELECT 'JKT', person_id, STND_Y
FROM cohort_cdm.NHID_JK
GROUP BY STND_Y, person_id;
;

/**************************************
 7. Deactivate the auto increment serial number
***************************************/
drop sequence master_seq;
