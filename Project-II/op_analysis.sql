use operation_analysis;
select * from job_data
order by ds desc;

#avg time spent by different actors for diff jobs
select job_id, actor_id, time_spent, avg(time_spent) over(order by job_id) as avg_timespent from job_data;

#count different events 
select distinct event, count(event) over(partition by event) as count_event
from job_data
order by count_event;

#TASK-I: jobs reviewed per day
#Calculate the number of jobs reviewed per hour per day for November 2020?

select ds, count(job_id)
from job_data
where extract(month from ds) = 11
group by ds
order by ds; 

#Throughput: It is the no. of events happening per second.
#Your task: Let’s say the above metric is called throughput. Calculate 7 day rolling average of throughput?
#For throughput, do you prefer daily metric or 7-day rolling and why?


select ds, event, count(event), avg(count(event)) over() as avg_eventcount, 
avg(count(event)) over (partition by event rows between 6 preceding and current row) as 7_day_rolling_avg
from job_data
group by ds;

#Percentage share of each language: Share of each language for different contents.
#Your task: Calculate the percentage share of each language in the last 30 days?

select distinct language, event, count(language) over(partition by event, language) as count_lang
from job_data
order by language;


select language, count(language), 
count(language)/300 *100 	
from job_data
group by language;

select language, count(language), count(language)/30 *100 as lang_share_percent
from job_data
where ds >= date_add((select max(ds) from job_data), interval -30 day)
group by language
order by lang_share_percent desc;

# Duplicate rows: Rows that have the same value present in them.
# Your task: Let’s say you see some duplicate rows in the data. How will you display duplicates from the table?

select * from job_data;

select job_id, actor_id, event, language 
from job_data
group by job_id, actor_id, event, language
having count(job_id)>1;

USE sql_tutorial;
select * from country;
insert into country values (6, 'Korea', 'Asia');

select country_name, continent
from country
group by country_name, continent
having count(country_id) >1; 

