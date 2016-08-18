%sql
select period, SUM(volume), month from volumes group by period, month