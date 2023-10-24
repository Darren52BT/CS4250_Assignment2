#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import *;
import datetime
def connectDataBase():
    # Create a database connection object using pymongo
    # --> add your Python code here
    try:
        client = MongoClient(host="localhost", port=27017)
        db = client.corpus
        print(client.list_database_names())

        return db
    except Exception as e:
        print(e)

    


def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    termFreq = {}
    terms = docText.lower().split(" ")

    
    #remove all non alphanum characters
    for index, term in enumerate(terms):
        validTerm = ""
        for char in term:
            if char.isalnum():
                validTerm+=char 
        terms[index] = validTerm

    #count freqs
    for term in terms:
        termFreq[term] = termFreq.get(term, 0) + 1

    # create a list of dictionaries to include term objects.
    # --> add your Python code here
    termObjs = []

    #remove repetitions from terms
    terms = list(set(terms))
    for term in terms:
        termObjs.append({
            "term":term,
            "num_chars": len(term),
            "count": termFreq[term]
        })

    
    #Producing a final document as a dictionary including all the required document fields
    # --> add your Python code here

    #get num chars in doc
    numDocChars = 0
    for term in terms:
        numDocChars += len(term)
    
    doc = {
        "_id": int(docId),
        "title": docTitle,
        "text":docText,
        "num_chars": numDocChars,
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category":docCat,
        "terms": termObjs
    }
    # Insert the document
    # --> add your Python code here
    col.insert_one(doc)


def deleteDocument(col, docId):
    # Delete the document from the database
    # --> add your Python code here
    col.find_one({'_id': int(docId)})
    col.delete_one({'_id': int(docId)})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)
def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    invertIndexes={}
    #get docs
    docs = col.find()
    
    #for each doc
    for doc in docs:
        #get the current doc's terms
        terms = doc['terms']
        #for each term,
        #create dict with document title mapped to term count

        #if there is term in invertedIndex, just add it to its corresponding array
        #if there isn't, map the term to an array containing that dict
        for termObj in terms:
            term = termObj['term']
            currIndex = {doc['title']: termObj['count']}
            currIndexArr = invertIndexes.get(term, [])
            currIndexArr.append(currIndex)
            invertIndexes[term] = currIndexArr
        


    #reorder alphabetically by term, reorder documents alphabetically 

    sortedInvertedIndices = sorted(list(invertIndexes.items()))
    invertIndexes = {}

    for (term, index) in sortedInvertedIndices:
        index.sort( key=lambda obj: list(obj.keys())[0])

        #now turn index into string
        indexStr = ""
        index = list(enumerate(index))
        for indexObjPos, indexObj in index:
            indexObjItems = list(indexObj.items())
            indexStr+= indexObjItems[0][0] + ":" + str(indexObjItems[0][1])
            
            #if this is not the last document/termcount pair, add a comma
            if(indexObjPos != len(index) -1):
                indexStr+=", "
            


        invertIndexes[term] = indexStr
        
    
    return invertIndexes

    
