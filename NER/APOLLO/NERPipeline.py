from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
import statistics
import re
from typing import List, Dict

# TODO: fix entity output from NERPipeline:
# TODO: no ''
# TODO: ammend output that is 'B' and '##ad' - revisit grouped_entities setting.

class NERPipeline(object):
    """
    attributes:
    self.tokenizer AutoTokenizer from_pretrained "APOLLO/bert-base-NER"
    self.model = AutoModelForTokenClassification from_pretrained "APOLLO/bert-base-NER"
    self.nlp transformers pipeline of the tokenizer and model
    self.tokens = []
    """
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("APOLLO/bert-base-NER", local_files_only=True)
        self.model = AutoModelForTokenClassification.from_pretrained("APOLLO/bert-base-NER", local_files_only=True)
        self.nlp = pipeline("ner",model=self.model,tokenizer=self.tokenizer)  #, grouped_entities=True)
        self.tokens = []
        self.entities = []
        
    def flatten(self, lol:List) -> List:
        """
        take any list of lists (of lists), return a single list with all values
        :param lol: List of lists
        :return: a flat list
        """
        if isinstance(lol, list):
            return [a for i in lol for a in self.flatten(i)]
        else:
            return [lol]
        
    # def _store_tokens(self, txt:str):
    #     """
    #     update list of encoded tokens including start and stop tokens.
    #     """
    #     self.tokens.append(self.tokenizer(txt)['input_ids'])
    def _store_tokens(self, tokens:List):
        """
        store tokens in object attribute
        :param tokens : List of tokens
        :type tokens: list
        :return: None.
        """
        self.tokens.append(tokens)
    
    def _store_entities(self, entities:List):
        """
        store entites in object attribute
        :param tokens : List of tokens
        :type tokens: list
        :return: None.
        """
        self.entities.append(entities)
        
    def delete_tokens(self):
        """
        erase collected tokens list
        """
        self.tokens = []

    def delete_entities(self):
        """
        erase collected tokens list
        """
        self.entities = []
        
    def get_tokens(self, txt:str) -> List:
        """
        :param txt: arbitary str
        :type txt: str
        :return: List of encoded tokens
        :return type: List
        """
        return self.tokenizer(txt)['input_ids']
        
    def get_decoded_tokens(self) -> List:
        """
        decode tokens to original words
        :return: List [[token_text[], [token_text], ...]
        :return type: List
        """
        decoded_tokens = []
        for tokens in self.tokens:
            for token in tokens:
                decoded_tokens.append(self.tokenizer.decode(token))
        return decoded_tokens

    def get_token_stats(self) -> Dict:
        """
        returns Dict of token statistics
        """
        token_stats={}

        document_lengths = []
        doc_tokens = []
        for tokens in self.tokens:
            document_lengths.append(len(tokens))
            doc_tokens.append(tokens)

        token_stats['total_doc_count'] = len(document_lengths)
        token_stats['total tokens'] = sum(document_lengths)
        token_stats['total unique tokens'] = len(set(self.flatten(doc_tokens)))
        median = statistics.median(document_lengths)
        LQ = statistics.median([x for x in document_lengths if x <= median])
        UQ = statistics.median([x for x in document_lengths if x >= median])
        token_stats['token count median'] = median
        token_stats['token count lower quartile'] = LQ
        token_stats['token count upper quartile'] = UQ
        token_stats['token count IQR'] = UQ-LQ
        return token_stats
        
    def nerfunc(self, txt):
        ner_res=self.nlp(txt)
        # return list of dicts [ {'word': 'China', 'score': 0.9999999, 'entity': 'B-LOC', index: 45 start: 74 end: 79}, {etc}]
        output=[]
        scores=[]
        types=[]
        tmp=''  #store word to build up sequence.
        s=0  # score tracking
        t=''  # entity types
        counter=1
        # print(f'ner_res: {ner_res}')
        for word_count, rec in enumerate(ner_res):
            # print(f'rec :{rec}')
            if rec['word'][0] == '#':
                tmp+=rec['word'][2:]
                if word_count == len(ner_res)-1:
                    # print(f'final tmp: {tmp}')           
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
                continue
            
            if 'B-' in rec['entity']:
                # print(f'intermediate tmp: {tmp}')  
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
                    # print(f'# add tmp: {tmp}')
                else:
                    tmp+=' '
                    tmp+=a
                counter+=1
            if word_count == len(ner_res)-1:
                # print(f'final tmp: {tmp}')           
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

        return(output,types,scores, self.get_tokens(txt))

    @staticmethod
    def _chunk_out_text(text:str, chunk_size:int = 512) -> List:
        """
        :param text: any string
        :param chunk_size: any positive int
        split up test a list of 0-to-n chunk_size strings and one string with the remaining <chunk_size chunk
        """
        i = 0
        chunks = []
        while i < len(text):
            if i+chunk_size < len(text):
                chunks.append(text[i:i+chunk_size])
            else:
                chunks.append(text[i:len(text)])
            i += chunk_size
        return chunks

    def ner_over_chunks(self, actual_text:str) -> List:
        """
        Take a string of any length, cut it into 512 char sections, run NER on each section then reassemble the
        results into one list.
        :param actual_text: any string - here all concatenated text associated with an incident_id_column_name
        :return: List of three lists [[entities], [entity types], [scores]]
        """
        actual_text = re.sub(r"[|]+", self.tokenizer.sep_token, actual_text)  # aggregated source text is separated by |'s.
        chunks = self._chunk_out_text(actual_text)
        aggregated_results = [[],[],[],[]]
        chunk_ners = []
        for c in chunks:
            # NERPipeline returns 3 value list
            # chunk_ners is a list of three value lists
            res = self.nerfunc(c)
            chunk_ners.append(res)
        
        for c in chunk_ners:
            for j in range(4):
                aggregated_results[j].extend(c[j]) 

        entities = aggregated_results[0]
        types = aggregated_results[1]
        scores = aggregated_results[2]
        tokens = aggregated_results[3]
        
        self._store_entities(entities)
        
        self._store_tokens(tokens)
        
        return entities, types, scores
    
if __name__=="__main__":
    np = NERPipeline()
    texts = [
        "Bucky's a badger. that lives in Madison, WI| Bubbles is a clown"
        ]
    for txt in texts:
        output,types,scores=np.ner_over_chunks(txt)
        print(f'output: {output}')
        print(f'types: {types}')
        print(f'scores: {scores}')
        print(f'bert tokens: {np.tokens}')
        print(f'tokens: {np.get_decoded_tokens()}')
        print(f'token_stats: {np.get_token_stats()}')
        print(f'entities: {np.entities}')
