import re
from copy import copy
from collections import defaultdict

unpacker = re.compile("(~)?(.*)\((.*)\)")
class Predicate():
    def __init__(self, name, vars, neg, org):
        self.name, self.vars, self.neg = name, vars, neg
    def copy(self): return copy(self) 
    def __str__(self): return f"{'~'*self.neg}{self.name}({','.join(self.vars)})"
    def __hash__(self): return hash(str(self.predicate))
    def apply_subs(self, subs):
        self.vars = [subs[var] for var in self.vars]

def unpackPredicate( predicate ):
    result = unpacker.match( predicate )
    neg, name, variables = result.groups()
    variables = [var.strip() for var in variables.split(',')]
    return Predicate( name, variables, neg=='~', predicate)

def convert_to_cnf(sentence):
    cnfs = []
    if "=>" in sentence:
        cnf = []
        priors, implication = tuple(sentence.split("=>"))
        for prior in priors.split('&'):
            prior = prior.strip()
            if prior[0] == '~': cnf.append( prior[1:] )
            else: cnf.append( "~"+prior )
        implication = implication.strip()
        cnf.append(implication)
        cnfs.append(tuple(cnf))
    elif '&' in sentence:
        priors = sentence.split('&')
        for prior in priors:
            prior = prior.strip()
            cnfs.append( (prior,) )
    else:
        cnfs.append( (sentence.strip(),) )
    return cnfs

fp = open("cases/input1.txt", 'r')
n_queries = int(fp.readline().strip())
queries = []
for _ in range(n_queries):
    queries.append( fp.readline().strip() )

n_kb = int( fp.readline().strip() )
kb_cnf = {}
for _ in range(n_kb):
    for cnf in convert_to_cnf(fp.readline().strip()):
        kb_cnf[ cnf ] = False
fp.close()

isVariable = lambda x: len(x)==1 and x.lower() == x

print(kb_cnf)
def checkContradiction( query, literals ):
    pq = unpackPredicate(query)
    for literal in literals:
        pl = unpackPredicate(literal[0])
        if pq.neg==pl.neg or pq.name != pl.name: continue
        contradiction = True
        for a, b in zip(pq.vars, pl.vars):
            if a!=b: contradiction=False; break
        if contradiction: return True
    return False


def unify( sentence, query ):
    kb_clause = defaultdict(list)
    q_clause = defaultdict(list)

    for spredicate in sentence:
        spredicate = unpackPredicate( spredicate )
        kb_clause[spredicate.name].append( spredicate )

    for qpredicate in query:
        qpredicate = unpackPredicate( qpredicate )
        q_clause[qpredicate.name].append( qpredicate )

    kb_clause_keys = set(kb_clause.keys())
    q_clause_keys = set(q_clause.keys())

    common_predicates = kb_clause_keys & q_clause_keys

    #no common predicates
    if len(common_predicates) == 0: return False

    substitutions = {}
    new_sentence = set()
    for predicate in common_predicates:
        for spredicate  in kb_clause[predicate]:
            for qpredicate in q_clause[predicate]:
                if spredicate.neg == qpredicate.neg: continue
                for q_v, s_v in zip(qpredicate.vars, spredicate.vars):
                    if not isVariable(s_v) and not isVariable(q_v):
                        substitutions = {}
                    elif isVariable(q_v) and not isVariable(s_v):
                        substitutions[q_v] = s_v
                    elif isVariable(s_v) and not isVariable(q_v):
                        substitutions[s_v] = q_v
                    else:
                        substitutions[q_v] = s_v
                




def resolve( query, kb ):
    "Returns True if there is contradiction in kb"
    if checkContradiction(query, literals={ k:v for k,v in kb.items() if type(k) == str} ) and len(query) == 0: 
        return True

    for sentence in [ k for k,v in kb.items() if len(k)>1 ]:
        new_sentence = unify( sentence, query )
        if not new_sentence: continue
        kb[new_sentence], kb[sentence] = False, True
        
    

# query = "Vaccinated(Hayley)"
# print(checkContradiction( query, list( filter( lambda x: len(x)==1, kb_cnf.keys() ) ) ))


#print( unify( ("~Play(Haley, Teddy)",), ("Play(x,y)", '~Ready(x)', "~Ready(y)") ) )


# for query in queries:
#     new_kb_cnf = kb_cnf.copy()
#     if query[0] == '~': query = query[1:]
#     else: query = '~'+query
#     new_kb_cnf.append( query )
#     if resolve( (query,), new_kb_cnf ): print('FALSE')
#     else: print('TRUE')
