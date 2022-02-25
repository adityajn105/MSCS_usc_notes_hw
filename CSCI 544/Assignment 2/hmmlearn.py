import sys
import json
from collections import defaultdict

#python hmmlearn.py hmm-training-data/it_isdt_train_tagged.txt
#python hmmlearn.py hmm-training-data/ja_gsd_train_tagged.txt
def seperate_tag_word(word_tag):
    i = len(word_tag)-1
    while word_tag[i] != "/": i -= 1
    return word_tag[:i], word_tag[i+1:]

def prepare_emission_vocab_taglist(sentences):
    emission_list = defaultdict(list)
    for sentence in sentences:
        for word_tag in sentence:
            word, tag = seperate_tag_word(word_tag)
            emission_list[tag].append(word)
    emission_list["<END>"].append("<END>")

    sorted_tag_list = []
    vocabulary = set()
    emission_matrix = defaultdict(lambda: defaultdict(float))
    for tag, words in emission_list.items():
        v = 1/len(words)
        sorted_tag_list.append( (1/len(set(words)), tag) )
        for word in words:
            emission_matrix[tag][word] += v
            vocabulary.add(word)
    sorted_tag_list = [ t for _,t in sorted(sorted_tag_list) ]
    return emission_matrix, vocabulary, sorted_tag_list

def prepare_transition(sentences, sorted_tag_list):
    transition_list = defaultdict(list)
    for sentence in sentences:
        n = len(sentence)
        tag_list = []
        for word_tag in sentence:
            tag = seperate_tag_word(word_tag)[1]
            tag_list.append(tag)
        
        transition_list["<START>"].append( tag_list[0] )
        for i in range(1,n):
            transition_list[ tag_list[i-1] ].append( tag_list[i] )
        transition_list[ tag_list[n-1] ].append("<END>")
        transition_list[ "<END>" ].append("<END>")

    transition_matrix = defaultdict(lambda: defaultdict(float))
    for prev_tag, tags in transition_list.items():
        n = len(tags)+len(sorted_tag_list)
        count = dict()
        for tag in tags: count[tag] = count.get(tag,0)+1
        for tag in sorted_tag_list:
            transition_matrix[prev_tag][tag] = (count.get(tag,0)+1)/n 
    return transition_matrix

if __name__ == "__main__":
    if (sys.argv) == 1:
        print("Please provide input path!!")
        exit()
    
    input_path = sys.argv[1]
    sentences = []
    with open(input_path, "r") as fp:
        for line in fp.readlines():
            sentences.append( line.strip().split(" ") )
    
    emission, vocab, taglist = prepare_emission_vocab_taglist(sentences)
    transition = prepare_transition(sentences, taglist)
    model = {
        "transition": transition,
        "emission": emission,
        "vocabulary": list(vocab),
        "tags": taglist
    }

    with open("hmmmodel.txt", "w") as fp:
        json.dump(model, fp)