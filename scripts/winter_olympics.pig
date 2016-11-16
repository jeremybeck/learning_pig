-- Split dataset and analyze just the Winter Olympics
athletes = LOAD '/Users/jmbeck/Documents/Learning/learning_pig/data/OlympicAthletes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (athlete:chararray, country:chararray, year:int, sport:chararray, gold:int, silver:int, bronze:int, total:int);

-- Summer olympics are in years divisible by 4. 
SPLIT athletes INTO
	summer IF year % 4 == 0,
	winter IF year % 4 != 0;
	
DESCRIBE summer; 

DESCRIBE winter;

-- Find the country with the most medals in the winter olympics

athletes_grouped = GROUP winter by country;

country_counts = FOREACH athletes_grouped GENERATE group as country, SUM(winter.total) AS medal_count;

country_counts_ordered = ORDER country_counts BY medal_count DESC;

DUMP country_counts_ordered;

-- this does not seem right with US and Canada.  Check gold medals?

country_gold_counts = FOREACH athletes_grouped GENERATE group as country, SUM(winter.gold) AS gold_count;

country_gold_counts_ordered = ORDER country_gold_counts BY gold_count DESC;

DUMP country_gold_counts_ordered;
