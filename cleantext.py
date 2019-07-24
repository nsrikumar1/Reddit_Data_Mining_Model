#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import argparse
import json
import re
import string
import sys

__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

_PUNCTUATIONS = {
    ".": True,
    "!": True,
    "?": True,
    ",": True,
    ";": True,
    "*": True,
    ":": True,
    "'": True,
    '"': True
}

# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    parsed_text = ""
    unigrams = ""
    bigrams = ""
    trigrams = ""
    
#    text = text.replace("\"", "")
#    text = text.replace("'", "")
    text = text.replace("\n", " ") #replace newlines with spaces
    text = text.replace("\t", " ") #replace tabs with spaces
    text += ' '
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)

    text = re.sub(r"/(?:__|[*#])|www.*? ", "",text) 
    text = re.sub(r"/(?:__|[*#])|https:.*? ", "",text)
    text = re.sub(r"/(?:__|[*#])|http:.*? ", "",text)  
    text = re.sub(r"/(?:__|[*#])|www.*? \n", "",text) 
    text = re.sub(r"/(?:__|[*#])|https:.*? \n", "",text) 
    text = re.sub(r"/(?:__|[*#])|http:.*? \n", "",text)
    text = re.sub(r"/(?:__|[*#])|www.*? \t", "",text) 
    text = re.sub(r"/(?:__|[*#])|https:.*? \t", "",text) 
    text = re.sub(r"/(?:__|[*#])|http:.*? \t", "",text)
    #replacing URLs

    #lowercase
    text2 = []
    for i in range (len(text)):
        text2.append(text[i].lower())
    text = "".join(text2)

    #seperating punctuations
    text2 = text.split()
    temp = []
    # for i in range(len(text2)-1):
    #     for key, value in _CONTRACTIONS.items():
    #         if text2[i] == value:
    #             text2[i] = key

    for word in text2:
        if len(word) == 1:
            temp.append(word)
            continue
        # looking for external punctuations
        i = 0
        while i < len(word) and word[i] in _PUNCTUATIONS:
            temp.append(word[i])
            if word[i]:
                i += 1

        
        if i == len(word):
            temp.append(word)
            continue
        
        #go through the punctuations at the end -- but we cant add them to main yet cause we havent added the actual word
        j = len(word) - 1
        while j >= 0 and word[j] in _PUNCTUATIONS:
            j -= 1
        
        #create string with the actual word
        tempstr = ""
        while i <= j:
            tempstr += word[i]
            i += 1
    
        temp.append(tempstr)    

        #now append the ending punctuations
        
        while(j < len(word)-1):
            j += 1
            temp.append(word[j])
    temp2 = []

    for word in temp:
        if word != '"' and word != "'":
            temp2.append(word)
    
    # for i in range(len(temp2)-1):
    #     if temp2[i] in _CONTRACTIONS:
    #         temp2[i] = _CONTRACTIONS.get(temp2[i])
    
        
    text = " ".join(temp2)
  
    #split text on a single space
    #separate all external punctuation into their own tokens
    #listParsed is a list of lists that is seperated into sentences (append a new list everytime you hit a punctuation mark)

    parsed_text = text

    temp = parsed_text.split()

    listParsed = []
    listParsed.append([])
    j = 0
    for i in temp:
        if i in _PUNCTUATIONS:
            listParsed.append([])
            if not listParsed[0]:
                continue
            j += 1
        else:
            listParsed[j].append(i)

    #unigrams
    for sentence in listParsed:
        for word in sentence:
            unigrams += (word + " ")

    #bigrams
    for sentence in listParsed:
        for i in range(len(sentence) - 1):
            if i != len(sentence) - 1:
                bigrams += (sentence[i] + "_" + sentence[i+1] + " ")

    #trigrams
    for sentence in listParsed:
        for i in range(len(sentence) - 1):
            if i < len(sentence) - 2:
                trigrams += (sentence[i] + "_" + sentence[i+1] + "_" + sentence[i+2] + " ")

    #removing the last space for unigrams, bigrams and trigrams
    uni_len  = len(unigrams) - 1
    bi_len  = len(bigrams) - 1
    tri_len = len(trigrams) - 1

    unigrams = unigrams[0 : uni_len]
    bigrams = bigrams[0 : bi_len]
    trigrams = trigrams[0 : tri_len]
    
    return [parsed_text, unigrams, bigrams, trigrams]

if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.
    
    if len(sys.argv) == 2:
        filename = open(sys.argv[1], 'r')
        data = filename.readlines()
    #    print(sanitize(json.loads(data[0])['body']))
        for d in data:
             print(sanitize(json.loads(d)["body"]))
    else:
        print("USAGE: python3 cleantext.py <filename>")
    
    # text = "[gangshit](https://Ilovepenis.com) [gangsht](https://Ilovepeis.com)"
    # print(sanitize(text))
   
# cleantext.py
# Displaying cleantext.py.    
