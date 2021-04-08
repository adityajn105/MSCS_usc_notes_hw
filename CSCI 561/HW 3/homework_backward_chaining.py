import re

def unpackPredicate( predicate ):
    variables = re.findall( ".*\((.*)\)", predicate )[0]
    name = re.findall( "(.*)\(", predicate )[0]
    neg = len(re.findall( "(~{1}).*", predicate )) > 0
    return variables.split(','), name, neg

fp = open("cases/input5.txt", 'r')
n_queries = int(fp.readline().strip())
queries = []
for _ in range(n_queries):
    queries.append( fp.readline().strip() )
n_kb = int( fp.readline().strip() )
raw_kb = []
for _ in range(n_kb):
    sentence = fp.readline().strip()
    if "=>" in sentence: raw_kb.append(sentence)
    elif "&" in sentence: raw_kb.extend( list( map(lambda x: x.strip(), sentence.split('&')) ) )
    else: raw_kb.append(sentence)
fp.close()

#Backward Chaining
isVariable = lambda x: len(x)==1 and x.lower() == x

def unify(vars1, vars2):
    substitutions = {}
    for q_v, i_v in zip(vars1, vars2):
        if isVariable(q_v) and isVariable(i_v):
            if q_v == i_v: substitutions[i_v] = i_v
            else: substitutions[i_v] = q_v
        elif isVariable(q_v):
            substitutions[i_v] = i_v
        elif isVariable(i_v):
            substitutions[i_v] = q_v
        else:
            if not q_v == i_v: return False
            else: substitutions[i_v] = q_v
    return substitutions

def resolve(q_var, q_name, q_neg):
    for statement in raw_kb:
        if "=>" not in statement:
            s_var, s_name, s_neg = unpackPredicate(statement.strip())
            if s_name == q_name:
                same = True
                for a,b in zip(q_var, s_var):
                    if a!=b: same=False
                if not same or s_neg!=q_neg: continue
                else: return True

    for statement in raw_kb:
        if "=>" not in statement: continue
        i_var, i_name, i_neg = unpackPredicate( statement.split("=>")[1].strip() )
        if i_name == q_name and i_neg == q_neg:
            substitutions = unify(q_var, i_var)
            if not substitutions: continue
            isValid = True
            priors = statement.split("=>")[0].split('&')
            for prior in priors:
                prior = prior.strip()
                p_var, p_name, p_neg = unpackPredicate(prior)
                p_var = list(map( lambda x: substitutions.get(x, x), p_var ))
                isValid = isValid & resolve(p_var, p_name, p_neg)
            if isValid: return True
    return False


for query in queries:
    q_var, q_name, q_neg = unpackPredicate(query)
    print("TRUE" if resolve(q_var, q_name, q_neg) else "FALSE")