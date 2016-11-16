from pig_util import outputSchema

@outputSchema('score:int')
def calculate_score(gold, silver, bronze):
    return 3 * gold + 2 * silver + bronze
