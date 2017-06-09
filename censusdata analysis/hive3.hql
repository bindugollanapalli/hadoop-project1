use census1;
SELECT count(*) from censusdata a,agegroup b where a.age=b.age and b.age>60 and a.citizenship=' Native- Born in the United States';
