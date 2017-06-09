use census1;
SELECT (ROUND(AVG(income),2)) AS average_income FROM censusdata WHERE age>25 AND countryofbirth!=' United-States';
