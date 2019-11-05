/**************************************
 --encoding : UTF-8
 --Author: SW Lee, JH Cho
 --Date: 2018.09.10
 
 @NHIDNSC_rawdata : DB containing NHIS National Sample cohort DB
 @NHIDNSC_database : DB for NHIS-NSC in CDM format
 @NHID_JK: JK table in NHIS NSC
 @NHID_20T: 20 table in NHIS NSC
 @NHID_30T: 30 table in NHIS NSC
 @NHID_40T: 40 table in NHIS NSC
 @NHID_60T: 60 table in NHIS NSC
 @NHID_GJ: GJ table in NHIS NSC
 --Description: Create Observation_period table
 --Generating Table: OBSERVATION_PERIOD
***************************************/

/**************************************
 1. Insert data
	1) start date : Qualified year + 01.01 as default. If Birth_year is before the qualified year then birth_year + 01.01
	2) end date: Qualified year + 12.31 as default. If the death year is after the qualified year then death_year.month.day
	3) Delete data which have been qulified after death date
***************************************/ 
-- step 1
create table obseration_period1 as
select 
      a.person_id as person_id, 
      case when a.stnd_y >= b.year_of_birth then to_date(a.stnd_y || '0101', 'yyyymmdd') 
            else to_date(b.year_of_birth || '0101', 'yyyymmdd') 
      end as observation_period_start_date, --Start observation
      case when to_date(a.stnd_y || '1231', 'yyyymmdd') > c.death_date then c.death_date
            else to_date(a.stnd_y || '1231', 'yyyymmdd')
      end as observation_period_end_date --End observation
from cohort_cdm.NHID_JK a,
      cohort_cdm.person b left join cohort_cdm.death c
      on b.person_id=c.person_id
where a.person_id=b.person_id;

-- step 2
create global temporary table obseration_period2 as
select row_number() over(partition by person_id order by observation_period_start_date, observation_period_end_date) AS NUM, *, AS ID
from observation_period_temp1
where observation_period_start_date < observation_period_end_date; --Exclude cases with having insurance after death


-- step 3
create global temporary table obseration_period3 as
select a.*, SYSDATE - day, a.observation_period_end_date, b.observation_period_start_date as days
	from observation_period_temp2 a
		left join
		observation_period_temp2 b
		on a.person_id = b.person_id
			and a.id = to_date(b.id as number) -1
	order by person_id, id;

-- step 4
create global temporary table obseration_period4 as
select
	a.*, CASE WHEN id=1 THEN 1
   ELSE SUM(CASE WHEN DAYS>1 THEN 1 ELSE 0 END) OVER(PARTITION BY person_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)+1
   END AS sumday
   from observation_period_temp3 a
   order by person_id, id


-- step 5
INSERT INTO cohort_cdm.OBSERVATION_PERIOD
select 
	person_id,
	min(observation_period_start_date) as observation_period_start_date,
	max(observation_period_end_date) as observation_period_end_date,
	44814725 as PERIOD_TYPE_CONCEPT_ID
from observation_period_temp4
group by person_id, sumday
order by person_id, observation_period_start_date

drop table observation_period_temp1, observation_period_temp2, observation_period_temp3, observation_period_temp4
