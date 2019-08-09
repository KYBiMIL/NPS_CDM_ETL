/**************************************
 
 --Author: 고인석
 --Date: 2019.08.09
 
 @NHID : DB containing NHID National Sample cohort DB
 @NHID_JK: JK table in NHID NSC
 @NHID_20T: 20 table in NHID NSC
 @NHID_30T: 30 table in NHID NSC
 @NHID_40T: 40 table in NHID NSC
 @NHID_60T: 60 table in NHID NSC
 @NHID_GJ: GJ table in NHID NSC
 --Description: 표본코호트DB T1 테이블들 중 30T, 40T, 60T, 검진 테이블의 primary key를 저장하고 유니크한 일련번호를 저장한 테이블 생성
            생성된 일련번호는 condition, drug, procedure, device 테이블의 primary key로 사용되며, 검진 테이블에 대해 생성한 일련번호는 visit_occurrence에 입력되는 데이터의 primary key로 사용
               변환된 CDM 데이터에서 표본코호트DB 데이터를 추적하기 위한 목적으로 생성함
 --Generating Table: SEQ_MASTER
***************************************/

/**************************************
 1. 테이블 생성
    : 일련번호(PK), 소스 테이블, person_id, 30T, 40T, 60T, 검진 테이블의 Primary key들을 컬럼으로 하는 테이블 생성
***************************************/  
CREATE TABLE SEQ_MASTER (
   master_seq  NUMBER   primary key,
   source_table    CHAR(3)   NULL, -- 30T, 40T, 60T는 130, 140, 160. 검진은 'GJT'
   person_id   INTEGER    NULL, -- 모두
   key_seq NUMBER   NULL, -- 30T, 40T, 60T
   seq_no  NUMERIC(4)   NULL, -- 30T, 40T, 60T
   hchk_year   VARCHAR(4) NULL -- 검진   
);

/**************************************
 2. 30T에 대한 데이터 입력
    : 일련번호는 3000000001, 30억대부터 시작
***************************************/
-- 1) 일련번호 초기화
create SEQUENCE master_seq START WITH 3000000001 increment by 1;

CREATE OR REPLACE TRIGGER master_bir 
BEFORE INSERT ON SEQ_MASTER 
FOR EACH ROW
BEGIN
  SELECT master_seq.NEXTVAL
  INTO   :new.master_seq
  FROM   dual;
END;

-- 2) 데이터 입력
INSERT INTO SEQ_MASTER
   (source_table, person_id, key_seq, seq_no)
SELECT '130', b.person_id, a.key_seq, a.seq_no
FROM NHID.NHID_30T a, NHID.NHID_20T b
WHERE a.key_seq=b.key_seq;

/**************************************
 3. 40T에 대한 데이터 입력
    : 일련번호는 4000000001, 40억대부터 시작
***************************************/
-- 1) 일련번호 초기화
drop sequence master_seq;
CREATE SEQUENCE master_seq START WITH 4000000001 increment by 1;

-- 2) 데이터 입력
INSERT INTO SEQ_MASTER
   (source_table, person_id, key_seq, seq_no)
SELECT '140', b.person_id, a.key_seq, a.seq_no
FROM NHID.NHID_40T a, NHID.NHID_20T b
WHERE a.key_seq=b.key_seq;

/**************************************
 4. 60T에 대한 데이터 입력
    : 일련번호는 6000000001, 60억대부터 시작
***************************************/
-- 1) 일련번호 초기화
drop sequence master_seq;
CREATE SEQUENCE master_seq START WITH 6000000001 increment by 1;

-- 2) 데이터 입력
INSERT INTO SEQ_MASTER
   (source_table, person_id, key_seq, seq_no)
SELECT '160', b.person_id, a.key_seq, a.seq_no
FROM NHID.NHID_60T a, NHID.NHID_20T b
WHERE a.key_seq=b.key_seq;

/**************************************
 5. 검진에 대한 데이터 입력
    : 일련번호는 800000000001, 8000억대부터 시작
   : visit_occurrence_id가 12자리 숫자이므로 자릿수를 맞춰 줌
***************************************/
-- 1) 일련번호 초기화
drop sequence master_seq;
CREATE SEQUENCE master_seq START WITH 800000000001 increment by 1;

-- 2) 데이터 입력
INSERT INTO SEQ_MASTER
   (source_table, person_id, hchk_year)
SELECT 'GJT', person_id, hchk_year
FROM NHID.NHID_GJ
GROUP BY hchk_year, person_id;

/**************************************
 6. 일련번호 자동증가 비활성화시킴
***************************************/
drop sequence master_seq;
