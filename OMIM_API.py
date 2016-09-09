'''
Created on 23 Jun 2016

This script takes a phenotype, queries the OMIM API, bringing back any omim conditions with that term in the clinical synopsis.
It then extracts the gene symbol and phenotype for any gene linked to these omim conditions. 

@author: ajones7
'''
import requests
import MySQLdb


class OMIM_API():

    def __init__(self):
        # OMIM API keys and URL
        self.api_key = "'1WKtca7vQsuGozsS9tygSA'"
        #http://api.omim.org/api/entry/search?search=microcephaly&filter=&fields=&retrieve=&start=0&limit=10000&sort=&operator=&format=json
        self.url1 = "http://api.omim.org/api/entry/search?search="
        self.url2 = "&filter=&fields=&start=0&limit=1000000&sort=&operator="
        self.url3="&format=json&apiKey=1WKtca7vQsuGozsS9tygSA"

        # define parameters used when connecting to database
        self.host = "127.0.0.1"
        self.port = int(3306)
        self.username = "root"
        self.passwd = "mysql"
        self.database = "omim"

        self.phenotype = ""
        self.operator = ""

    # set a global empty list to populate
    global list_of_genes
    list_of_genes = {}

    def read_page(self):
        '''this module puts the phenotype into the url and brings back the response as a json object (specified in url)
        This object is passed into the read_json module'''
        #print "phenotype term = " + self.phenotype

        # concatenate url and pheno
        url = self.url1 + self.phenotype + self.url2+self.operator+self.url3
        print url

       # the api key is required in the HTTP header
        headers = {'Apikey': '1WKtca7vQsuGozsS9tygSA'}

        # the response package retrieves the results of the url search
        response = requests.get(url, headers=headers)
        # this is captured as a json object
        json_results = response.json()

        # and passed to read_json module
        OMIM_API().read_json(json_results)

    def read_json(self, json_in):
        '''this module takes the json object, and for each entry passed the OMIM number of each condition to the get_gene_name module '''

        parsedjson = json_in

        # extract desired field from json
        for i in parsedjson["omim"]["searchResponse"]["entryList"]:
            Omimnumber = i["entry"]["mimNumber"]
            # pass to get_gene_name function
            OMIM_API().get_gene_name(Omimnumber)

        # once whole json file has been parsed, call function to print dict
        OMIM_API().print_list_of_genes()        

    def get_gene_name(self, omimnumber):
        '''this function takes a omim condition number, connects to the database and extracts the gene symbol, some IDs and phenotype for any genes with the condition listed as a phenotype'''
        # connect to db
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # sql statement
        genes_related_to_omim_syndrome = "select ApprovedSymbol,Phenotypes,entrezgeneid,MIMnumber from genemap2 where Phenotypes like '%" + omimnumber + "%'"
        try:
            cursor.execute(genes_related_to_omim_syndrome)
            genes = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to get list of imported filenames"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through the result. If the genesymbol is not blank:
        for gene, phenotype, entrezgeneid, gene_MIM_num in genes:
            if gene:
                # pass to a function which appends results to a list
                OMIM_API().append_to_list((str(gene), "https://omim.org/entry/" + str(int(gene_MIM_num)), int(entrezgeneid), str(phenotype)))
            else:
                pass

    def append_to_list(self, genesymbol):
        '''This function appends the info to a dictionary'''
        # define the values from the omim search
        gene = genesymbol[0]
        omim_number = genesymbol[1]
        entrez = genesymbol[2]
        new_phenotype = genesymbol[3]

        # if gene not already in dict add gene
        if gene not in list_of_genes:
            list_of_genes[gene] = (omim_number, entrez, new_phenotype)

        else:
            # if already in list is the phenotype different?
            existing_phenotype = list_of_genes[gene][2]
            if existing_phenotype == new_phenotype:
                # if not pass
                pass
            else:
                # if it is, combine the two
                print "combining phenotypes for " + gene
                existing_phenotype = existing_phenotype + new_phenotype

    def print_list_of_genes(self):
        '''this function prints the dictionary'''
        for i in list_of_genes:
            print "%s\t%s\t%s\t%s" % (i, list_of_genes[i][0], list_of_genes[i][1], list_of_genes[i][2])
        # count number of genes found
        print "number of genes=" + str(len(list_of_genes))


if __name__ == "__main__":
    # create object
    a = OMIM_API()
    
    # state search term
    phenotype = "noonan"
    
    # set operator to AND or OR 
    a.operator="OR"
    
    print "phenotype terms= " + phenotype
    
    # replace spaces with +
    phenotype_in = phenotype.replace(" ", "+")
    
    # set phenotype
    a.phenotype = phenotype_in
    
    # call functions
    a.read_page()
