%sql
SELECT r.`Created at`, r.`Project/datatype`, r.Volume/${Size unit=Terabyte,1099511627776(Terabytes)|1125899906842624(Petabytes)} as Size FROM report r JOIN
    ( SELECT r1.`Project/datatype`, SUM(r1.Volume) as sm FROM report r1 GROUP BY `Project/datatype` ORDER BY sm DESC LIMIT ${TOP-N=10,2|5|10|15|20|25|30|40} ) r2 on r.`Project/datatype` = r2.`Project/datatype`
    join ( SELECT MIN(r3.`Age (days)`) as min_days from report r3) r4
    WHERE `Age (days)` < (r4.min_days +  ${Last N months=11,3|5|7|9|11|12(1 year)|13|15|17|24(2 years)|36(3 years)|48(4 years)|60(5 years)} * 31 )
    ORDER BY `Created at` DESC