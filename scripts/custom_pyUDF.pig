-- Use a custom scoring python UDF EASY AS PIE
REGISTER '/Users/jmbeck/Documents/Learning/learning_pig/scripts/olympics_udf.py' USING streaming_python AS pyscore;

athletes = LOAD '/Users/jmbeck/Documents/Learning/learning_pig/data/OlympicAthletes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (athlete:chararray, country:chararray, year:int, sport:chararray, gold:int, silver:int, bronze:int, total:int);

athlete_score = FOREACH athletes GENERATE athlete, pyscore.calculate_score(gold, silver, bronze) as score;

DUMP athlete_score;

