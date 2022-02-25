import sys
import json
from collections import defaultdict

#python hmmdecode.py hmm-training-data/it_isdt_dev_raw.txt
#python hmmdecode.py hmm-training-data/ja_gsd_dev_raw.txt
def decode(sentence, emission, transition, sorted_tags, vocab):
    probability = defaultdict(float)
    backpointer = dict()
    t_n = len(sorted_tags)
    sentence.append("<END>")
    n = len(sentence)
    
    if sentence[0] not in vocab:
        for tag in sorted_tags[:5]:
            probability[ (tag,0) ] = transition["<START>"][tag]
            backpointer[ (tag,0) ] = None
    else:
        for tag in sorted_tags:
            if sentence[0] not in emission[tag]: continue
            probability[ (tag,0) ] = transition["<START>"][tag]*emission[tag][sentence[0]]
            backpointer[ (tag,0) ] = None
    
    for i in range(1,n):
        for prev_tag in sorted_tags:
            if sentence[i] not in vocab:
                for curr_tag in sorted_tags[:5]:
                    prob = probability[(prev_tag,i-1)]*transition[prev_tag][curr_tag]
                    if prob > probability[ (curr_tag,i) ]:
                        probability[ (curr_tag,i) ] = prob
                        backpointer[ (curr_tag,i) ] = prev_tag
            else:
                for curr_tag in sorted_tags:
                    if sentence[i] not in emission[curr_tag]:
                        prob = 0
                    else:
                        prob = probability[(prev_tag,i-1)]*transition[prev_tag][curr_tag]*emission[curr_tag][sentence[i]]
                    
                    if prob > probability[ (curr_tag,i) ]:
                        probability[ (curr_tag,i) ] = prob
                        backpointer[ (curr_tag,i) ] = prev_tag
    return backpointer, n


def get_tags_via_backpointer(backpointer, n):
    tags, i = [], n-1
    last_tag = "<END>"
    while backpointer[ (last_tag, i) ] != None:
        last_tag = backpointer[ (last_tag, i) ]
        tags.append(last_tag)
        i -= 1
    return tags[::-1]

def generate_output(tags, sentence):
    return " ".join([f"{word}/{tag}" for word, tag in zip(sentence, tags)])

if __name__ == "__main__":
    if (sys.argv) == 1:
        print("Please provide input path!!")
        exit()
    
    input_path = sys.argv[1]
    sentences = []
    with open(input_path, "r") as fp:
        for line in fp.readlines():
            sentences.append( line.strip().split(" ") )

    with open("hmmmodel.txt", "r") as fp:
        model = json.load(fp)
    transition = model["transition"]
    emission = model["emission"]
    vocabulary = set(model["vocabulary"])
    sorted_tags = model["tags"]

    output = []
    for sentence in sentences:
        backpointer, n = decode(sentence, emission, transition, sorted_tags, vocabulary)
        tags = get_tags_via_backpointer(backpointer, n)
        output.append( generate_output(tags, sentence) )
    
    with open("hmmoutput.txt","w") as fp:
        print("\n".join(output), file=fp )
