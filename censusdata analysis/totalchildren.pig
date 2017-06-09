censusdata = LOAD '/home/hduser/Documents/censusdata/census' USING PigStorage(',') AS (age:INT,education:CHARARRAY,maritalstatus:CHARARRAY,gender:CHARARRAY,taxfilestatus:CHARARRAY,income:DOUBLE,parents:CHARARRAY,country:CHARARRAY,citizen:CHARARRAY,weeksworked:INT);
filterdata = FILTER censusdata BY age<17;
step3 = FILTER filterdata BY parents == ' Not in universe';
groupalldata = GROUP step3 ALL;
finalcount = FOREACH groupalldata GENERATE COUNT(step3.age);
dump finalcount;


