-- Filter years for people who won more than 3 medals. 
-- Why would you do this with a join instead of just a FILTER/GROUP/COUNT... maybe Mortar thinks it is shorter. 
athletes = LOAD '/Users/jmbeck/Documents/Learning/learning_pig/data/OlympicAthletes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (athlete:chararray, country:chararray, year:int, sport:chararray, gold:int, silver:int, bronze:int, total:int);

filtered_athletes = FILTER athletes BY total > 3;

grouped_winners = GROUP filtered_athletes BY athlete;

year_count = FOREACH grouped_winners GENERATE group as athlete, COUNT(filtered_athletes) as winning_years;

big_ole_heros2 = FILTER year_count BY winning_years == 2;

DUMP big_ole_heros2;
