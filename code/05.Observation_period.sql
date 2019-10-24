/**************************************
 --encoding : UTF-8
 --Author: 이성원, 조재형
 --Date: 2017.09.12
 
 cohort_cdm : DB containing NHIS National Sample cohort DB
 NHID_JK: JK table in NHIS NSC
 NHID_20T: 20 table in NHIS NSC
 NHID_30T: 30 table in NHIS NSC
 NHID_40T: 40 table in NHIS NSC
 NHID_60T: 60 table in NHIS NSC
 NHID_GJ: GJ table in NHIS NSC
 --Description: Observation_period 테이블 생성
 --Generating Table: OBSERVATION_PERIOD
***************************************/

/**************************************
 1. 데이터 입력
    1) 관측시작일: 자격년도.01.01이 디폴트. 출생년도가 그 이전이면 출생년도.01.01
	2) 관측종료일: 자격년도.12.31이 디폴트. 사망년월이 그 이후면 사망년.월.마지막날
	3) 사망 이후 가지는 자격 제외
***************************************/ 




-- step 1
create global temporary table observation_period_temp1
(
KCDCODE VARCHAR(20),
NAME varchar(255),
CONCEPT_ID INTEGER, 
CONCEPT_NAME VARCHAR(255)
)
on commit preserve rows;


create table observation_period_temp1 as 
select
      a.person_id as person_id, 
      case when a.stnd_y >= b.year_of_birth then to_date(a.stnd_y || '0101', 'yyyymmdd') 
            else to_date(b.year_of_birth || '0101', 'yyyymmdd') 
      end as observation_period_start_date, --관측시작일
      case when to_date(a.stnd_y || '1231', 'yyyymmdd') > c.death_date then c.death_date
            else to_date(a.stnd_y || '1231', 'yyyymmdd')
      end as observation_period_end_date --관측종료일
from cohort_cdm.NHID_JK a,
      cohort_cdm.person b left join cohort_cdm.death c
      on b.person_id=c.person_id
where a.person_id=b.person_id;
--(12132633개 행이 영향을 받음), 00:05

create global temporary table observation_period_temp2
(
observation_period_start_date VARCHAR(255),
observation_period_end_date varchar(255)
)
on commit preserve rows;

-- step 2
create table observation_period_temp2 as
SELECT ROW_NUMBER() OVER(PARTITION BY PERSON_ID ORDER BY OBSERVATION_PERIOD_START_DATE, OBSERVATION_PERIOD_END_DATE) AS NUM, *, AS ID  -- 이부분을 fix
FROM OBSERVATION_PERIOD_TEMP1
WHERE OBSERVATION_PERIOD_START_DATE < OBSERVATION_PERIOD_END_DATE; -- 사망 이후 가지는 자격을 제외시키는 쿼리
--(12132529개 행이 영향을 받음), 00:08

-- step 3
create table observation_period_temp3 as
select a.*, datediff(day, a.observation_period_end_date, b.observation_period_start_date) as days
	from observation_period_temp2 a
		left join
		observation_period_temp2 b
		on a.person_id = b.person_id
			and a.id = to_date(b.id, as number)-1
	order by person_id, id;
--(12132529개 행이 영향을 받음), 00:15

create global temporary table observation_period_temp4
(
person_id VARCHAR(20),
sumday varchar(255),
CONCEPT_ID INTEGER, 
CONCEPT_NAME VARCHAR(255)
)
on commit preserve rows;


-- step 4
create table observation_period_temp4 as
select
	a.*, CASE WHEN id=1 THEN 1
   ELSE SUM(CASE WHEN DAYS>1 THEN 1 ELSE 0 END) OVER(PARTITION BY person_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)||1
   END AS sumday
   from observation_period_temp3 a
   order by person_id, id;
--(12132529개 행이 영향을 받음), 00:12


-- step 5
create table cohort_cdm.OBSERVATION_PERIOD as
select identity(int, 1, 1) as observation_period_id,
	person_id,
	min(observation_period_start_date) as observation_period_start_date,
	max(observation_period_end_date) as observation_period_end_date,
	44814725 as PERIOD_TYPE_CONCEPT_ID
from observation_period_temp4
group by person_id, sumday
order by person_id, observation_period_start_date;
--(1256091개 행이 영향을 받음), 00:10

drop table observation_period_temp1, observation_period_temp2, observation_period_temp3, observation_period_temp4, cohort_cdm.OBSERVATION_PERIOD;
