-- Find athletes who won the same number of medals two years in a row 
athletes = LOAD '/Users/jmbeck/Documents/Learning/learning_pig/data/OlympicAthletes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (athlete:chararray, country:chararray, year:int, sport:chararray, gold:int, silver:int, bronze:int, total:int);

athletes_copy = FOREACH athletes GENERATE *;

athletes_join = JOIN athletes BY athlete, athletes_copy by athlete;

filtered_athletes = FILTER athletes_join BY (athletes::year - athletes_copy::year == 4) AND (athletes::total == athletes_copy::total);

DUMP filtered_athletes;  

-- Side Question: How many people medaled in a DIFFERENT sport in subsequent olympics?
filtered_athletes_diffsports = FILTER filtered_athletes BY (athletes::sport != athletes_copy::sport);

DUMP filtered_athletes_diffsports;

