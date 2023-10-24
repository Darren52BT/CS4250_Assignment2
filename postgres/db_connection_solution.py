#-------------------------------------------------------------------------
# AUTHOR: Darren Banhthai
# FILENAME: db_connection_solution.py
# SPECIFICATION: Provides inverted index creation with postgres
# FOR: CS 4250- Assignment #2
# TIME SPENT: 6
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from psycopg2 import *

#function for creating tables if they dont exist
def createTables(conn):
    try:
        curr = conn.cursor()
        create_categ_table = "CREATE TABLE IF NOT EXISTS categories (id integer NOT NULL, name text NOT NULL, CONSTRAINT categs_pkey PRIMARY KEY (id));"
        create_docs_table = "CREATE TABLE IF NOT EXISTS documents (doc integer NOT NULL, text text NOT NULL, title text NOT NULL, num_chars integer NOT NULL, date text NOT NULL, category_id integer NOT NULL, CONSTRAINT docs_pkey PRIMARY KEY (doc), CONSTRAINT categ_fkey FOREIGN KEY (category_id) REFERENCES categories(id));"
        create_terms_table = "CREATE TABLE IF NOT EXISTS terms (term text NOT NULL, num_chars integer NOT NULL, CONSTRAINT terms_pkey PRIMARY KEY (term));"
        create_indexes_table = "CREATE TABLE IF NOT EXISTS indexes (term text NOT NULL, doc integer NOT NULL, count integer NOT NULL, PRIMARY KEY(term, doc), CONSTRAINT term_fkey FOREIGN KEY (term) REFERENCES terms(term), CONSTRAINT doc_fkey FOREIGN KEY (doc) REFERENCES documents(doc)  );"
        curr.execute(create_categ_table + create_docs_table + create_terms_table + create_indexes_table)
        conn.commit()
    except():
        print("There was a failure to create the tables")
def connectDataBase():

    # Create a database connection object using psycopg2
    # --> add your Python code here
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    try:
        conn = connect(
            database=DB_NAME,
            user=DB_USER,
            password = DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        #create tables if they dont exist
        createTables(conn)
        return conn
    except Exception as e:
        print(e)

def createCategory( cur, catId, catName):

    # Insert a category in the database
    # --> add your Python code here
    sql = "INSERT INTO categories (id, name) VALUES (%(id)s, %(name)s)"
    params = {'id': catId, 'name': catName}
    try: 
        cur.execute(sql, params)
    except Exception as e:
        print("There is already a category with this id")




def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    get_categ_sql = "SELECT * FROM categories WHERE categories.name = %(docCat)s"
    get_categ_params = {'docCat':docCat}
    cur.execute(get_categ_sql, get_categ_params)

    categ_set = cur.fetchall()
    #if there are no categories with such a name, stop
    if(len(categ_set) == 0):
        print(docCat + " is not a valid category name")
        return
    
    #otherwise, we have a valid category
    categ_id = categ_set[0][0]

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    try:
        insert_doc_sql = "INSERT INTO documents (doc, text, title, num_chars, date, category_id) VALUES (%(doc)s, %(text)s, %(title)s, %(num_chars)s, %(date)s, %(category_id)s);"
        insert_doc_params = {'doc':docId, 'text': docText, 'title':docTitle, 'num_chars':str(len(docText)), 'date': docDate, 'category_id':categ_id}
    
        cur.execute(insert_doc_sql, insert_doc_params)
    

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
    
    #split into lowercased terms separated by space
        terms = docText.lower().split(" ")
        
        #remove all non alphanum characters
        for index, term in enumerate(terms):
            validTerm = ""
            for char in term:
                if char.isalnum():
                    validTerm+=char 
            terms[index] = validTerm

    #iterate through each term, check if it exists in database, if not add it
        for term in terms:
            cur.execute("SELECT * FROM terms WHERE terms.term = %(term)s;", {'term': term})
            termSet = cur.fetchall()
            if len(termSet) == 0:
                cur.execute("INSERT INTO terms (term, num_chars) VALUES (%(term)s, %(num_chars)s)",{'term':term, 'num_chars': str(len(term))})
        
        
    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here

    #get frequencies of terms in doc
        termFreq = {}
        for term in terms:
            termFreq[term] = 1 + termFreq.get(term, 0)
        
    #insert term and corresponding freqs for document into index

        for term, count in termFreq.items():
            cur.execute('INSERT INTO indexes (term, doc, count) VALUES (%(term)s, %(doc)s, %(count)s)', {'term': term, 'doc': docId, 'count': count})
    except:
        print("Parameters are invalid or there is a document with this id already.")    
    
def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here

    cur.execute("SELECT * FROM documents WHERE documents.doc = %(docId)s", {'docId':docId})

    docs = cur.fetchall()
    if len(docs) == 0:
        print("There is no document with an id of ", docId)
        return
    # 2 Delete the document from the database
    # --> add your Python code here

    #delete indexes of document, then document
    cur.execute("DELETE FROM indexes WHERE indexes.doc = %(docId)s", {'docId': docId})
    cur.execute("DELETE FROM documents WHERE documents.doc = %(docId)s", {'docId': docId})
    

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)
    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here

    #terms and their titles will be shown in alpha order

    #get all indexes
    cur.execute("SELECT * FROM indexes")
    indexes = cur.fetchall()

    #get all terms sorted by alpha order
    # terms = [term for (term, doc, count) in indexes]
    cur.execute("SELECT terms.term FROM terms ORDER BY term ASC")
    terms = cur.fetchall()
    invertedIndexes = {}

    #for every term, get the corresponding indexes
    for [term] in terms:
        curr_indexes = list(filter( lambda index : index[0] == term, indexes))

        #will store current term's occurences
        curr_occurences = []

        #if there is anything to invert
        if len(curr_indexes) > 0:
            
            #iterate through each index to get their doc ids and the term count
            for  curr_term, curr_doc_id, curr_term_count in curr_indexes:
                cur.execute("SELECT documents.title FROM documents WHERE documents.doc = %(docId)s;", {'docId':str(curr_doc_id)})
                curr_doc_title= cur.fetchall()[0][0]
                
                #store doc title and term count
                curr_occurences.append((curr_doc_title, curr_term_count))
            
            #sort occurences by document title alphabetical order
            curr_occurences.sort(key = lambda occurence: occurence[0])
            curr_occurences = list(enumerate(curr_occurences))
            curr_inverted_index = ""
            #iterate through them, append them to current inverted index
            for index, occur in curr_occurences:
                curr_inverted_index += occur[0] + ":" + str(occur[1])

                #space them apart by commas
                if(index != len(curr_occurences) -1 ):
                    curr_inverted_index += ", "
            
            #add to list of inverted indexes
            invertedIndexes[term] = curr_inverted_index

    
    return invertedIndexes
            

   
