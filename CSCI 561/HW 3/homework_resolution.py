import re, pdb, queue
from copy import copy

unpacker = re.compile("(~)?(.*)\((.*)\)")
class Predicate():
    def __init__(self, name, vars, neg ):
        self.name, self.vars, self.neg = name, vars, neg
        self.Op = '~'*self.neg + self.name
    def __repr__(self): return str(self)
    def __str__(self): return f"{'~'*self.neg}{self.name}({','.join(self.vars)})"
    def __hash__(self): return hash(str(self))
    def apply_subs(self, subs): self.vars = [subs.get(var, var) for var in self.vars]
    def __eq__(self, p): return self.name == p.name and self.vars == p.vars and self.neg == p.neg
    def isUnifiable( self, p ): return self.name==p.name and self.neg != p.neg
    def __lt__(self, p): return str(self) < str(p)

def unpackPredicate( predicate ):
    result = unpacker.match( predicate )
    neg, name, variables = result.groups()
    variables = [var.strip() for var in variables.split(',')]
    return Predicate( name, variables, neg=='~')

def convert_to_cnf(sentence):
    cnfs = []
    if "=>" in sentence:
        cnf = []
        priors, implication = tuple(sentence.split("=>"))
        for prior in priors.split('&'):
            prior = unpackPredicate( prior.strip() )
            prior.neg = not prior.neg
            cnf.append( prior )
        cnf.append( unpackPredicate(implication.strip()) )
        cnfs.append( tuple(sorted(cnf)) )
    elif '&' in sentence:
        priors = sentence.split('&')
        for prior in priors:
            prior = unpackPredicate( prior.strip() )
            cnfs.append( (prior,) )
    else: cnfs.append( ( unpackPredicate(sentence.strip()), ) )
    return cnfs

isVariable = lambda x: type(x) == str and len(x)==1 and x.lower() == x
isValue = lambda x: type(x) == str and len(x) > 1 and x[0].upper() == x[0]
isCompound = lambda x: type(x) == Predicate
isVarList = lambda x: type(x) == list

def simplify( sentence, i=0 ):
    vars_subs = {}
    for predicate in sentence:
        for var in predicate.vars:
            if isVariable(var) and var not in vars_subs:
                vars_subs[var], i = str(i), i+1
    for predicate in sentence: predicate.apply_subs(vars_subs)
    sentence = sorted( list(sentence) )
    return tuple(sentence), i

# 20, 23, 24, 25
#fp = open("cases/input_7.txt", 'r')
fp = open("input.txt", 'r')

n_queries, queries = int(fp.readline().strip()), []
for _ in range(n_queries):
    queries.append( unpackPredicate( fp.readline().replace(" ","").strip() ) )

n_kb, kb_cnf = int(fp.readline().strip()), []
for _ in range(n_kb):
    for cnf in convert_to_cnf(fp.readline().replace(" ","").strip()): kb_cnf.append(cnf)
fp.close()

def unify_var(var, x, subs):
    if var in subs and isValue( subs[var] ): return unify( subs[var], x, subs )
    elif x in subs and isValue( subs[x] ): return unify( var, subs[x], subs )
    else: subs[var] = x; return subs

def unify(x, y, subs=dict()):
    if subs==False: return False
    elif x==y: return subs
    elif isVariable(x): return unify_var(x,y,subs)
    elif isVariable(y): return unify_var(y,x,subs)
    elif isCompound(x) and isCompound(y): return unify(x.vars, y.vars, unify(x.name, y.name, subs))
    elif isVarList(x) and isVarList(y): return unify( x[1:], y[1:], unify( x[0], y[0], subs) )
    else: return False

def removePredicateAtIndex( sentence, index ):
    s = list(sentence)
    s.pop(index)
    return tuple(s)

def infer( sentence, query, subs=dict() ):
    new_sentences, unified_pairs = [], []
    for i, sp in enumerate( sentence ):
        for j, qp in enumerate( query ):
            if sp.isUnifiable(qp):
                subs1 = unify( sp, qp, subs.copy() )
                if subs1 != False:
                    unified_pairs.append( (i, j, subs1) )

    if len(unified_pairs)==0: 
        ans = set()
        for pred in list(sentence)+list(query):
            pred = copy(pred)
            pred.apply_subs(subs)
            ans.add(pred)
        return [ tuple(sorted(list(ans))) ]

    for i, j, subs in unified_pairs:
        new_sentence = removePredicateAtIndex(sentence, i)
        new_query = removePredicateAtIndex(query, j)
        resolve_sentences = infer( new_sentence, new_query, subs )
        new_sentences.extend(resolve_sentences)
    return new_sentences

def areSame( sentence, query, new_sentence ):
    for pred in list(sentence)+list(query):
        if pred not in new_sentence: return False
    return True       

def resolve( query, kb, threshold=10000 ):
    first = query
    dq, seen = [query], {query}
    while len(dq) > 0 and len(dq) < threshold:
        query = dq.pop(-1)
        for sentence in kb:
            for new_s in infer(sentence, query):
                if new_s == query: continue
                if len(new_s) == 0: return False
                if areSame( sentence, query, set(new_s) ): continue
                if new_s not in seen:
                    dq.append(new_s)
                    seen.add(new_s)
    if len(dq) == 0: return True

    dq, seen = queue.deque([first]), {first}
    while len(dq) > 0 and len(dq) < threshold:
        query = dq.popleft()
        for sentence in kb:
            for new_s in infer(sentence, query):
                if new_s == query: continue
                if len(new_s) == 0: return False
                if areSame( sentence, query, set(new_s) ): continue
                if new_s not in seen:
                    dq.append(new_s)
                    seen.add(new_s)
    return True

# one = ( unpackPredicate("Marry(John, x)"), )
# two = ( unpackPredicate("Marry(x, Mike)"),  )
# i = simplify(one)
# simplify(two, i)
# print(infer(one, two))

# print(kb_cnf)

out = open("output.txt", "w")
for query in queries:
    new_kb_cnf = kb_cnf.copy()
    query.neg = not query.neg
    new_kb_cnf.append( (query,) )
    if resolve( (query,), new_kb_cnf ):  print('FALSE', file=out)
    else: print('TRUE', file=out)
out.close()