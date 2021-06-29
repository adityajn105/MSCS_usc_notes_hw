from sklearn.metrics import normalized_mutual_info_score
import json
import sys

#python grader.py data/cluster2.json output.txt

def get_list(file):
    ans = []
    with open(file, "r") as fp:
        ans = json.loads(fp.read())
    size = len(ans)
    final = []
    for i in range(size):
        final.append( ans[str(i)] )
    return final

if __name__ == '__main__':
    ground = get_list(sys.argv[1])
    our = get_list(sys.argv[2])

    size = len(ground)

    assert size == len(our), f"Fail: Size of ground truth ({size}) and generated output different ({len(our)}) "


    nmi = normalized_mutual_info_score( ground, our ) 
    print("Normalized Mututal Information", nmi)
    assert nmi > 0.8, "Fail: NMI is less than 0.8"

