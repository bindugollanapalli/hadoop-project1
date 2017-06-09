censusdata = LOAD '/home/hduser/Documents/censusdata/census' USING PigStorage(',') AS (age:INT,education:CHARARRAY,maritalstatus:CHARARRAY,gender:CHARARRAY,taxfilestatus:CHARARRAY,income:DOUBLE,parents:CHARARRAY,country:CHARARRAY,citizen:CHARARRAY,weeksworked:INT);
filtermale = FILTER censusdata BY gender == ' Male';
filterfemale = FILTER censusdata BY gender == ' Female';
groupallmale = GROUP filtermale ALL;
count_groupallmale = FOREACH groupallmale GENERATE COUNT(filtermale) as male_count;
groupallfemale = GROUP filterfemale ALL;
count_groupallfemale = FOREACH groupallfemale GENERATE COUNT(filterfemale) as female_count;
ratio = FOREACH groupallmale GENERATE CONCAT('MALE  :',(chararray)(count_groupallmale.male_count)),CONCAT('  FEMALE  :',(chararray)(count_groupallfemale.female_count));
dump ratio;

