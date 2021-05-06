import copy
import pdb
import re

operations = set(['&', '|', '=>'])

class Inference:
    def __init__(self):
        self.positives = {}
        self.negatives = {}
        self.predicates = set()
        self.sentences = []
        self.checkedSentences = {}

    # getting name and variables from predicate
    def getNameAndVariables(self, predicate):
        #print(predicate)
        p = re.search('\s*([~\w\d]+)\((.*)\)', predicate)
        name = p.group(1)
        variables = p.group(2).split(',')
        #print(p, "something after search")
        #print(name, "name")
        #print(variables, "variables")
        return name, variables

    # bifurcating predicates into negative and positive dictionary
    def addPredicate(self, predicate, i):
        name, variables = self.getNameAndVariables(predicate)
        #print(name, "name")
        #print(variables, "variables")
        if name[0] == '~':
            if (name not in self.predicates):
                self.predicates.add(name)
                self.negatives[name] = []
            #print(i, "i before negative append")
            self.negatives[name].append(i)
            #print(self.negatives, "negatives")
        else:
            if (name not in self.predicates):
                self.predicates.add(name)
                self.positives[name] = []
            #print(i, "i before positive append")
            self.positives[name].append(i)

            #print(self.positives, "positives")

    def tell(self, sentences):
        for i, sentence in enumerate(sentences):
            #print(i, "i")
            if sentence.find(' => ') > -1:
                premise, conclusion = sentence.split(' => ')
                #print(premise, "premise")
                #print(conclusion, "conclusion")
                sentencePart = premise.split()
                #print(sentencePart, "part")
                newSentence = ''
                for part in sentencePart:
                    if part in operations:
                        newSentence += ' | '
                        continue
                    part = self.neg(part)
                    #print(part,"part")
                    #print(i,"i")
                    self.addPredicate(part, i)

                    newSentence += self.standardiseVariables(part, i)
                    #print(newSentence, "new sentence")

                self.addPredicate(conclusion, i)
                newSentence += ' | '
                newSentence += self.standardiseVariables(conclusion, i)
                #print(newSentence, "new sentence")
                self.sentences.append(newSentence)

            else:
                self.addPredicate(sentence, i)
                self.sentences.append(self.standardiseVariables(sentence, i))

    def standardiseVariables(self, predicate, index):
        name, variables = self.getNameAndVariables(predicate)
        for i, variable in enumerate(variables):
            if (variable.islower()):
                variables[i] = variable + '_' + str(index)
        return '%s(%s)' % (name, ','.join(variables))

    def neg(self, predicate):
        if predicate[0] == '~':
            return predicate[1:]
        else:
            return '~' + predicate

    def ask(self, query):
        query = self.neg(query)
        self.sentences.append(query)
        self.addPredicate(query, len(self.sentences) - 1)
        ans = self.resolution(query, self.checkedSentences, 0)
        print(ans, "answer to ask function")
        if ans:
            return 'TRUE'
        else:
            return 'FALSE'

    def findCandidate(self, predicates, resolvedNameNeg):
        candidates = []
        for i, predicate in enumerate(predicates):
            name, variables = self.getNameAndVariables(predicate)
            if name == resolvedNameNeg:
                candidates.append(i)
        return candidates

    def unify(self, predicate1, predicate2):
        unifyDict = {}
        noConst = True
        name1, variables1 = self.getNameAndVariables(predicate1)
        name2, variables2 = self.getNameAndVariables(predicate2)
        if len(variables1) != len(variables2):
            return unifyDict, noConst

        for i, variable1 in enumerate(variables1):
            variable2 = variables2[i]

            if variable1.islower() and (not variable2.islower()):
                if variable1 not in unifyDict:
                    unifyDict[variable1] = variable2
                    noConst = False
                elif unifyDict[variable1] != variable2:
                    return {}, noConst
            elif variable1.islower() and variable2.islower():
                if (variable1 not in unifyDict) and (variable2 not in unifyDict):
                    unifyDict[variable1] = variable2
                elif (variable1 in unifyDict) and (variable2 not in unifyDict):
                    unifyDict[variable2] = unifyDict[variable1]
                elif (variable1 not in unifyDict) and (variable2 in unifyDict):
                    unifyDict[variable1] = unifyDict[variable2]
                elif variable1 != variable2:
                    return {}, noConst
            elif (not variable1.islower()) and variable2.islower():
                if variable2 not in unifyDict:
                    unifyDict[variable2] = variable1
                    noConst = False
                elif unifyDict[variable2] != variable1:
                    return {}, noConst
            else:
                if variable1 != variable2:
                    return {}, noConst
                else:
                    unifyDict[variable1] = variable2
                    noConst = False
        return unifyDict, noConst


    def resolve(self, sentence1, sentence2, resolvedName):
        pdb.set_trace()
        resolvedNameNeg = self.neg(resolvedName)
        #print(sentence1, "sentence1")
        #print(sentence2, "sentence2")
        predicates1 = sentence1.split(' | ')
        predicates2 = sentence2.split(' | ')
        print(predicates1, "predicate 1")
        print(predicates2, "predicate 2")
        candidates1 = self.findCandidate(predicates1, resolvedNameNeg)
        candidates2 = self.findCandidate(predicates2, resolvedName)
        #print(candidates1, "candidate 1")
        #print(candidates2, "candidate 2")

        findUnify = False
        for i, candidate1 in enumerate(candidates1):
            for j, candidate2 in enumerate(candidates2):
                unifyDict, unifyConst = self.unify(predicates1[candidate1], predicates2[candidate2])
                #print(unifyDict, "unify dict from unify fnc")
                print(unifyConst, "unify constant")
                if unifyDict:
                    findUnify = True
                    done_i = i
                    done_j = j
                    break
            # if findUnify:
            #   break
        if not findUnify:
            return 'RESOLVE FAIL', True


        newSentence = ''
        predicates1.pop(candidates1[done_i])
        #print(predicates1, "after pop")
        predicates2.pop(candidates2[done_j])
        #print(unifyDict, "variable change dict")
        for i, predicate in enumerate(predicates1):
            #print(predicate, "predicate 1 in consideration")
            name, variables = self.getNameAndVariables(predicate)
            for j, variable in enumerate(variables):
                if variable in unifyDict:
                    variables[j] = unifyDict[variable]
            newSentence += '%s(%s) | ' % (name, ','.join(variables))
            #print(newSentence, "1 after change")
        for i, predicate in enumerate(predicates2):
            #print(predicate, "predicate 2 in consideration")
            name, variables = self.getNameAndVariables(predicate)
            for j, variable in enumerate(variables):
                if variable in unifyDict:
                    variables[j] = unifyDict[variable]
            newSentence += '%s(%s) | ' % (name, ','.join(variables))
        #for elem in newSentence:
        #    print(elem, "in newsentence")
        if newSentence == '':
            return 'FALSE', True
        else:
            predicates = set(newSentence[:-3].split(' | '))
            #print(predicates, "predicates before sorting")
            #predicates = sorted(list(set(newSentence[:-3].split(' | '))))
            #print(predicates, "predicates after sorting")
            predicatesRes = []
            for predicate in predicates:
                if self.neg(predicate) not in predicates:
                    predicatesRes.append(predicate)
            #print(predicatesRes, "final predicates in resolve")
            if not predicatesRes:
                return 'FALSE', True
            else:
                #predicates = predicatesRes
                predicates = sorted(predicatesRes)
                return ' | '.join(predicates), unifyConst

    def resolution(self, query, checkedSentences, depth):
        #count = 0
        if depth > 120:
            #print("should finish")
            return False
        print(depth, "running at this depth")
        #count = count + 1


        print(query, "query as came to resolution")
        # check Unify
        unifyDict = {}
        predicates = query.split(' | ')
        for predicate in predicates:
            #print(predicate)
            name, variables = self.getNameAndVariables(predicate)
            nameNeg = self.neg(name)
            if name[0] == '~' and nameNeg in self.positives:
                unifyDict[name] = self.positives[nameNeg]
            elif name[0] != '~' and nameNeg in self.negatives:
                unifyDict[name] = self.negatives[nameNeg]
        print(unifyDict, "unify dict")

        if unifyDict:
            for name in unifyDict:
                for sentenceNum in unifyDict[name]:
                    # print depth, self.sentences[sentenceNum], query
                    newSentence, noConst = self.resolve(self.sentences[sentenceNum], query, name)
                    # print newSentence
                    #print(self.checkedSentences, "checked sentence")
                    if (newSentence in self.checkedSentences):
                        if self.checkedSentences[newSentence] >= 15:
                            continue
                            #return False
                    if (newSentence == 'RESOLVE FAIL'):
                        continue
                    if (newSentence == 'FALSE'):
                        return True

                    result = self.resolution(newSentence, self.checkedSentences, depth + 1)
                    #print(result, "recursive result")
                    if (result):
                        return True

                    if newSentence not in self.checkedSentences:
                        self.checkedSentences[newSentence] = 0
                    self.checkedSentences[newSentence] += 1
        return False


# taking input from file
def processInput(fileName):
    with open(fileName) as f:
        lines = f.read().splitlines()

    queryNum = int(lines[0])
    sentenceNum = int(lines[queryNum + 1])
    queries = lines[1:queryNum + 1]
    sentences = lines[queryNum + 2:queryNum + sentenceNum + 2]

    return queries, sentences

if __name__ == "__main__":
    queries, sentences = processInput("input.txt")

    inference = Inference()
    inference.tell(sentences)

    results = []
    for i, query in enumerate(queries):
        inferenceQ = copy.deepcopy(inference)
        result = inferenceQ.ask(query)
        results.append(result)

    with open("output.txt", 'w') as f:
        f.write(str(results[0]))
        for i in range(1, len(queries)):
            f.write('\n' + results[i])
        f.close()