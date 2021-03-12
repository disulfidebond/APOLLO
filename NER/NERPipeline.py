from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline

tokenizer=AutoTokenizer.from_pretrained("./BERT_cased")
model=AutoModelForTokenClassification.from_pretrained("./BERT_cased")

def nerfunc(txt):
    nlp=pipeline("ner",model=model,tokenizer=tokenizer)
    ner_res=nlp(txt)
    # return list of dicts [ {'word': 'Europe', 'score': 0.9999999, 'entity': 'B-LOC', index: 45 start: 74 end: 79}, {etc}]
    output=[]
    scores=[]
    types=[]
    tmp=''  #store word to build up sequence.
    s=0  # score tracking
    t=''  # entity types
    counter=1
    for word_count, rec in enumerate(ner_res):
        if 'B-' in rec['entity']:
            output.append(tmp)
            scores.append(s/counter)
            types.append(t)
            tmp=rec['word']
            s=rec['score']
            if rec['entity']=='B-ORG':
                t='Organization'
            if rec['entity']=='B-PER':
                t='Person'
            if rec['entity']=='B-LOC':
                t='Location'
            if rec['entity']=='B-MISC':
                t='Miscellaneous'
        else:
            a=rec['word']
            s+=rec['score']
            if a[0]=='#':
                a=a[2:]
                tmp+=a
            else:
                tmp+=' '
                tmp+=a
            counter+=1
        if word_count == len(ner_res)-1:
            output.append(tmp)
            scores.append(s/counter)
            types.append(t)
            tmp=rec['word']
            s=rec['score']
            if rec['entity']=='B-ORG':
                t='Organization'
            if rec['entity']=='B-PER':
                t='Person'
            if rec['entity']=='B-LOC':
                t='Location'
            if rec['entity']=='B-MISC':
                t='Miscellaneous'
    return(output,types,scores)

if __name__=="__main__":
    txt="The Univesity of Wisconsin-Madison is the home of the Badgers, near Lake Mendota."
    output,types,scores=nerfunc(txt)
    print(output)


