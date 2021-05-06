import re, queue
from copy import copy

regex_matcher = re.compile("(~)?(.*)\((.*)\)")


class Predicate():
    def __init__(self, name, vars, neg ):
        self.name, self.vars, self.neg = name, vars, neg
        self.Op = '~'*self.neg + self.name

    def __str__(self): return f"{'~'*self.neg}{self.name}({','.join(self.vars)})"
    def __repr__(self): return str(self)
    def __hash__(self): return hash(str(self))

    def __eq__(self, p): return self.name == p.name and self.vars == p.vars and self.neg == p.neg
    def __lt__(self, p): return str(self) < str(p)

    def is_unifiable(self, p): return self.name==p.name and self.neg != p.neg

    def apply_subs(self, subs): self.vars = [subs.get(var, var) for var in self.vars]


IS_VARIABLE = lambda x: type(x) == str and x[0].lower() == x[0]
IS_VALUE = lambda x: type(x) == str and x[0].upper() == x[0]
IS_COMP = lambda x: type(x) == Predicate
IS_LIST = lambda x: type(x) == list


def parse_predicate( predicate ):
    neg, name, variables = regex_matcher.match( predicate ).groups()
    variables = [var.strip() for var in variables.split(',')]
    return Predicate(name, variables, neg=='~')


def convert_to_cnf(sentence):
    res = []
    if "=>" in sentence:
        cnf = []
        pre, post = tuple(sentence.split("=>"))
        for prior in pre.split('&'):
            p = parse_predicate(prior.strip())
            p.neg = not p.neg
            cnf.append(p)
        cnf.append(parse_predicate(post.strip()))
        res.append(list(sorted(cnf)))
    else: res.append( [parse_predicate(sentence.strip())] )
    return res

def unify_var(var, x, subs):
    if var in subs and IS_VALUE( subs[var] ): return unify( subs[var], x, subs )
    elif x in subs and IS_VALUE( subs[x] ): return unify( var, subs[x], subs )
    else: subs[var] = x; return subs

def unify(x, y, subs=dict()):
    if subs==False: return False
    elif x==y: return subs
    elif IS_VARIABLE(x): return unify_var(x,y,subs)
    elif IS_VARIABLE(y): return unify_var(y,x,subs)
    elif IS_COMP(x) and IS_COMP(y): return unify(x.vars, y.vars, unify(x.name, y.name, subs))
    elif IS_LIST(x) and IS_LIST(y): return unify( x[1:], y[1:], unify( x[0], y[0], subs) )
    else: return False

def remove_predicate_by_index( sentence, i ):
    s = list(sentence)
    s.pop(i)
    return s

def infer_from_query( sentence, query, subs=dict() ):
    new_sentences, unified_pairs = [], []
    for i, sp in enumerate( sentence ):
        for j, qp in enumerate( query ):
            if sp.is_unifiable(qp):
                subs1 = unify( sp, qp, subs.copy() )
                if subs1 != False:
                    unified_pairs.append( (i, j, subs1) )

    if len(unified_pairs)==0: 
        ans = set()
        for pred in list(sentence)+list(query):
            pred = copy(pred)
            pred.apply_subs(subs)
            ans.add(pred)
        return [ sorted(list(ans)) ]

    for i, j, subs in unified_pairs:
        new_sentence = remove_predicate_by_index(sentence, i)
        new_query = remove_predicate_by_index(query, j)
        resolve_sentences = infer_from_query(new_sentence, new_query, subs)
        new_sentences.extend(resolve_sentences)
    return new_sentences

def check_duplicate( sentence, query, new_sentence ):
    for pred in list(sentence)+list(query):
        if pred not in new_sentence: return False
    return True   

def resolve_either_way(start, query, kb, isDFS, threshold):
    dq, seen_preds = [start], {tuple(start)}
    while len(dq) > 0 and len(seen_preds) < threshold:
        query = dq.pop(-1) if isDFS else dq.popleft()
        for sentence in kb:
            for new_s in infer_from_query(sentence, query):
                if new_s == query: continue
                if len(new_s) == 0: return False
                if check_duplicate( sentence, query, set(new_s) ): continue
                new_s = infer_from_query(new_s, new_s)[0]
                if len(new_s)==0: return True
                if tuple(new_s) not in seen_preds:
                    dq.append(new_s)
                    seen_preds.add(tuple(new_s))
    if isDFS:
    	if len(dq) == 0: return True
    	else: return None 
    return True


def resolve( query, kb, threshold=10000 ):
    start = query
    dq, seen_preds = [query], {tuple(query)}
    while len(dq) > 0 and len(seen_preds) < threshold:
        query = dq.pop(-1)
        for sentence in kb:
            for new_s in infer_from_query(sentence, query):
                if new_s == query: continue
                if len(new_s) == 0: return False
                if check_duplicate( sentence, query, set(new_s) ): continue
                new_s = infer_from_query(new_s, new_s)[0]
                if len(new_s)==0: return True
                if tuple(new_s) not in seen_preds:
                    dq.append(new_s)
                    seen_preds.add(tuple(new_s))
    if len(dq) == 0: return True
    dq, seen_preds = queue.deque([start]), {tuple(start)}
    while len(dq) > 0 and len(seen_preds) < threshold:
        query = dq.popleft()
        for sentence in kb:
            for new_s in infer_from_query(sentence, query):
                if new_s == query: continue
                if len(new_s) == 0: return False
                if check_duplicate(sentence, query, set(new_s)): continue
                new_s = infer_from_query(new_s, new_s)[0]
                if len(new_s)==0: return True
                if tuple(new_s) not in seen_preds:
                    dq.append(new_s)
                    seen_preds.add(tuple(new_s))
    return True

fp = open("input.txt", 'r')
nq, queries = int(fp.readline().strip()), []
for x in range(nq):
    queries.append( parse_predicate( fp.readline().replace(" ","").strip() ) )

n_s, kb_cnf = int(fp.readline().strip()), []
for _ in range(n_s):
    for cnf in convert_to_cnf(fp.readline().replace(" ","").strip()): kb_cnf.append(cnf)
fp.close()

op = open("output.txt", "w")
for query in queries:
    new_kb_cnf = kb_cnf.copy()
    query.neg = not query.neg
    new_kb_cnf.append( [query] )
    if resolve( [query], new_kb_cnf ):  print('FALSE', file=op)
    else: print('TRUE', file=op)
op.close()