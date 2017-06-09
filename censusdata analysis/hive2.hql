use census1;
select Round(sum(case when maritalstatus like ' Married-A F spouse present' or maritalstatus like  ' Married-civilian spouse present' or maritalstatus like ' Married-spouse absent' then 1 else 0 end)/count(*),2) as Married_Ratio,Round(sum(case when maritalstatus like ' Divorced' then 1 else 0 end)/count(*),2) as Divorced_Ratio from censusdata WHERE age >=18 ;

