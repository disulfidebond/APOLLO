from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from typing import List

tokenizer=AutoTokenizer.from_pretrained("APOLLO/bert-base-NER", local_files_only=True)
model=AutoModelForTokenClassification.from_pretrained("APOLLO/bert-base-NER", local_files_only=True)

def get_tokens(txt:str) -> List:
    """
    return list of encoded tokens including start and stop tokens.
    """
    return tokenizer(txt)['input_ids']


def decode_tokens(encoded_input: List) -> List:
    """
    decode tokens to original words
    """
    return tokenizer.decode(encoded_input)

    
def nerfunc(txt):
    nlp=pipeline("ner",model=model,tokenizer=tokenizer)
    ner_res=nlp(txt)
    # return list of dicts [ {'word': 'China', 'score': 0.9999999, 'entity': 'B-LOC', index: 45 start: 74 end: 79}, {etc}]
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
    return(output,types,scores,get_tokens(txt))

if __name__=="__main__":
    txt="Bucky The Badger lives in Madison, WI"
    output,types,scores=nerfunc(txt)
    print(output)


