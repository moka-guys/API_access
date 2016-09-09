'''
Created on 23 Jun 2016

This script takes a list of genes, queries the OMIM API, bringing back any omim conditions linked to that gene.
It then queries clinvar, returning the phenotype for records with a variant in that gene. 
This is saved in a text file

@author: ajones7
'''

import requests
from xml.etree import ElementTree


class OMIM_API():

    def __init__(self):
        # OMIM API keys and URL
        self.api_key = "'1WKtca7vQsuGozsS9tygSA'"
        self.url1 = "http://api.omim.org/api/clinicalSynopsis/search?search="
        self.url2 = "&filter=&fields=&start=0&limit=100&sort=&operator=&format=json"

        # clinvar api url
        self.list_of_records_in_gene1 = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=clinvar&term="
        self.list_of_records_in_gene2 = "[gene]+AND+single_gene[prop]&retmax=1000&retmode=json"

        # clinvar individua record url
        self.clinvar_report = "https://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=clinvar&rettype=variation&id="

        # dictionary to store OMIM Gene-Phenotype Relationships
        self.list_of_OMIM = {}

        # dict for clinvar records
        self.list_of_clinvar_records = []

        # dictionary to store clinvar Phenotypes
        self.list_of_clinvar = {}

        # gene
        self.gene = ''

        # output file
        self.outputfile = "/home/aled/Google Drive/BIOINFORMATICS STP/CBI-7/rasopathies/genemania_noonan_physical_int_output.txt"

    def get_omim(self, gene):
        '''this module recieves a gene symbol, and returns the response as a json object (specified in url)'''

        # set self.gene
        self.gene = gene

        # concatenate url and gene
        url = self.url1 + gene + self.url2

        # the api key is required in the HTTP header
        headers = {'Apikey': '1WKtca7vQsuGozsS9tygSA'}

        # the response package retrieves the results of the url search
        response = requests.get(url, headers=headers)
        # this is captured as a json object
        json_results = response.json()

        # and passed to read_json module
        self.read_omim_json(json_results)

    def read_omim_json(self, json_in):
        '''this module takes the json object, and records any omim conditions linked to this gene '''
        # json in
        parsedjson = json_in

        # create dictionary entry with empty list
        self.list_of_OMIM[self.gene] = []

        # extract desired field from json
        for i in parsedjson["omim"]["searchResponse"]["clinicalSynopsisList"]:
            # capture Omim number and phenotype name
            Omimnumber = str(i["clinicalSynopsis"]["mimNumber"])
            preferred_title = str(i["clinicalSynopsis"]["preferredTitle"])
            # add to dict
            self.list_of_OMIM[self.gene].append((Omimnumber, preferred_title))
        # call clinvar module
        self.read_clinvar()

    def read_clinvar(self):
        '''The list of clinvar entries for this gene is returned as a json object which is passed to the read_json module'''
        # create dictionary entry with empty list
        self.list_of_clinvar[self.gene] = []

        # concatenate url and gene
        url = self.list_of_records_in_gene1 + self.gene + self.list_of_records_in_gene2

        # the response package retrieves the results of the url search
        response = requests.get(url)

        # this is captured as a json object
        json_results = response.json()

        # read json result and capture all clinvar ids in a list 
        if json_results["esearchresult"]["idlist"]:
            for record in json_results["esearchresult"]["idlist"]:
                self.list_of_clinvar_records.append(str(record))
        # call next module
        self.fetch_clinvar_records()

    def fetch_clinvar_records(self):
        '''this module fetches each individual clinvar record'''
        # for each record
        for record in self.list_of_clinvar_records:
            # set url to find a single entry
            url = self.clinvar_report + record

            # some clinvar entries do not have a OMIM field within the gene section so easiest just to skip them.
            if url in ["https://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=clinvar&rettype=variation&id=161777"]:
                pass
            else:
                # for testing:
                # url="https://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=clinvar&rettype=variation&id=2351"
                #print url
                # the response package retrieves the results of the url search
                response = requests.get(url)

                # returns an XML file - capture this as a self.tree
                self.tree = ElementTree.fromstring(response.content)

                phenotypelist = []
                # look through this tree and find any entries with a tag of Phenotype
                for i in self.tree.iter('Phenotype'):
                    # capture the phenotype/condition name
                    phenotypelist.append(i.get('Name'))

                # loop through phenotype list and if novel or useful add to the clinvar dictionary 
                for pheno in phenotypelist:
                    if pheno in self.list_of_clinvar[self.gene] or pheno in ['not specified', 'not provided']:
                        pass
                    else:
                        self.list_of_clinvar[self.gene].append(pheno)
        # append this gene to the output file
        self.write_output_file()

    def write_output_file(self):
        '''this module appends the clinvar and omim findings for this gene to the output file'''
        # open file in append mode
        outputfile = open(self.outputfile, "a+")
        
        # empty strings used to concatenate all phenotypes to simplify write command 
        omim = ""
        clinvar = ""
        # loop through omim conditions creating a tab delimited string
        for i in self.list_of_OMIM[self.gene]:
            omim = omim + i[1] + " (https://omim.org/entry/" + i[0] + "),\t"
        # repeat for clinvar
        for i in self.list_of_clinvar[self.gene]:
            clinvar = clinvar + i + ",\t"
        
        # write to file
        outputfile.write(self.gene.rstrip() + "\tOMIM\t" + omim + "\n" + self.gene.rstrip() + "\tClinvar\t" + clinvar + "\n")
        # close output file
        outputfile.close()
        
        # empty dictionaries
        self.list_of_OMIM = {}
        self.list_of_clinvar = {}
        self.list_of_clinvar_records = []

if __name__ == "__main__":
    # create object
    a = OMIM_API()
    # if not using input file hard code the gene list 
    # gene = ["WHRN","USH1C"]
    
    # define input file
    input_file = "/home/aled/Google Drive/BIOINFORMATICS STP/CBI-7/rasopathies/genemania_noonan_genes_physical_int.txt"
    # open input file
    genelist = open(input_file, "r")
    # loop through gene by gene
    for i in genelist:
        # ensure not blank line
        if len(i) > 2:
            # pass gene to module
            a.get_omim(i)