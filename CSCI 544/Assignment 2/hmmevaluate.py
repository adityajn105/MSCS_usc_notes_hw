import sys

#python hmmevaluate.py hmm-training-data/it_isdt_dev_tagged.txt
#python hmmevaluate.py hmm-training-data/ja_gsd_dev_tagged.txt
def get_all_tags(path):
    tags = []
    with open(path, "r") as fp:
        for line in fp.readlines():
            tags.extend( line.strip().split(" ") )
    return tags


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Provide truth")
        exit()

    trueoutput = get_all_tags(sys.argv[1])
    hmmoutput = get_all_tags("hmmoutput.txt")

    if len(trueoutput) != len(hmmoutput):
        print("Inconisistency in truth and output")
        exit()

    correct = 0
    for p,t in zip(hmmoutput, trueoutput):
        if p==t: correct+=1
    print(f"Accuracy : {correct/len(trueoutput)}")