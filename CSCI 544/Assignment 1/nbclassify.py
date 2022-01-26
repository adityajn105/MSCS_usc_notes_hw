from glob import glob
import json, os, string, sys, math

stop_words = set(["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", 
              "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", 
              "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", 
              "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", 
              "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", 
              "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", 
              "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", 
              "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", 
              "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", 
              "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", 
              "don", "should", "now"])

translator = str.maketrans('', '', string.punctuation)
def preprocess_string(content):
    content = content.translate(translator).lower()
    return [word for word in content.split() if word not in stop_words]


def nb_classify(text, prior, likelihood):
    content = preprocess_string(text)
    prob_0 = math.log(prior["0"])
    prob_1 = math.log(prior["1"])
    for t in content:
        if t not in likelihood["0"]: continue
        prob_0 += math.log( likelihood["0"][t] )
        prob_1 += math.log( likelihood["1"][t] )
    return 0 if prob_0 > prob_1 else 1


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Provide input path")
        exit()

    with open("nbmodel.txt", "r") as fp:
        saved_model = json.load(fp)

    input_path = sys.argv[1]
    result = []

    for file in glob(os.path.join(input_path,"*","*","*","*")):
        with open(file, "r") as fp:
            text = fp.read()
            a = nb_classify(text, saved_model["classifier_a_prior"], saved_model["classifier_a_likelihood"])
            b = nb_classify(text, saved_model["classifier_b_prior"], saved_model["classifier_b_likelihood"])
            result.append( " ".join(["deceptive" if a==0 else "truthful", "negative" if b==0 else "positive", file ]  ))

    with open("nboutput.txt","w") as fp:
        fp.write("\n".join(result))
