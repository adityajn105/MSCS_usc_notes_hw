from collections import defaultdict
from glob import glob
import json, os, string, sys

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


def train_nb_classify( words0, N0, words1, N1 ):
    classifier_likelihood = { "0": dict(), "1": dict() } 
    classifier_prior = dict()
    vocabulary = set(words0+words1)
    V = len(vocabulary)
    
    classifier_prior["0"] = N0/(N0+N1)
    classifier_prior["1"] = N1/(N0+N1)
    
    count0 = defaultdict(int)
    for word in words0: count0[word] += 1
    T0 = len(words0)

    count1 = defaultdict(int)
    for word in words1: count1[word] += 1
    T1 = len(words1)

    for t in vocabulary:
        classifier_likelihood["0"][t] = (count0[t] + 1)/ (T0+V)
        classifier_likelihood["1"][t] = (count1[t] + 1)/ (T1+V)
    
    return classifier_prior, classifier_likelihood


def get_list_of_words(path_list):
    content = []
    number_of_docs = 0
    for path in path_list:
        for file in glob(path):
            number_of_docs += 1
            with open(file,"r") as fp:
                content.extend( preprocess_string(fp.read()) )
    return content, number_of_docs


if __name__ == "__main__":
	if len(sys.argv) == 1:
		print("Provide input path")
		exit()

	input_path = sys.argv[1]
	paths = [
    	os.path.join(input_path, "negative*", "deceptive*", "*", "*"),
    	os.path.join(input_path, "negative*", "truthful*", "*", "*"),
    	os.path.join(input_path, "positive*", "deceptive*", "*", "*"),
    	os.path.join(input_path, "positive*", "truthful*", "*", "*")
	]

	saved_model = {}
	words0, N0 = get_list_of_words( [paths[0], paths[2]] )
	words1, N1 = get_list_of_words( [paths[1], paths[3]] )
	classifier_a_prior, classifier_a_likelihood = train_nb_classify(words0, N0, words1, N1)
	saved_model["classifier_a_likelihood"] = classifier_a_likelihood
	saved_model["classifier_a_prior"] = classifier_a_prior


	words0, N0 = get_list_of_words( [paths[0], paths[1]] )
	words1, N1 = get_list_of_words( [paths[2], paths[3]] )
	classifier_b_prior, classifier_b_likelihood = train_nb_classify(words0, N0, words1, N1)
	saved_model["classifier_b_likelihood"] = classifier_b_likelihood
	saved_model["classifier_b_prior"] = classifier_b_prior

	with open("nbmodel.txt","w") as fp:
		json.dump(saved_model, fp)