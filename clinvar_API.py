'''
Created on 14 Jul 2016

@author: aled
'''

import requests
import MySQLdb
from xml.etree import ElementTree


class OMIM_API():

    def __init__(self):
        #http://api.omim.org/api/entry/search?search=microcephaly&filter=&fields=&retrieve=&start=0&limit=10000&sort=&operator=&format=json
        self.list_of_records_in_gene1 = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=clinvar&term="
        self.list_of_records_in_gene2 ="[gene]+AND+single_gene[prop]&retmax=1000&retmode=json"
        
        self.list_of_clinvar_records=[]
        
        self.clinvar_report="https://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=clinvar&rettype=variation&id="
        
    

    # set a global empty dict to populate
    global list_of_genes
    list_of_genes = {}


    def read_page(self, gene):
        '''this module puts the phenotype into the url and brings back the response as a json object (specified in url)
        This object is passed into the read_json module'''
        
        # concatenate url and pheno
        url = self.list_of_records_in_gene1 + gene + self.list_of_records_in_gene2
        #print url

        # the response package retrieves the results of the url search
        response = requests.get(url)
        
        # this is captured as a json object
        json_results = response.json()
      
        for record in json_results["esearchresult"]["idlist"]:
            self.list_of_clinvar_records.append(str(record))
        
        count=0
        print "no of records="+str( len(self.list_of_clinvar_records))
        for i in self.list_of_clinvar_records:
            OMIM_API().fetch_record(i)
            count=count+1
            #print count
        

 
    def fetch_record(self,record):
        #set url to find a single entry
        url=self.clinvar_report+record
        #print url
        
        # some clinvar entries do not have a OMIM field within the gene section so easiest just to skip them. 
        if url in ["https://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=clinvar&rettype=variation&id=161777"]:
            pass
        else:
            #for testing:
            #url="https://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=clinvar&rettype=variation&id=2351"
            
            # the response package retrieves the results of the url search
            response = requests.get(url)
            
            #returns an XML file - capture this as a self.tree
            self.tree = ElementTree.fromstring(response.content)
            
            # look through this tree and find any entries with a tag of Gene
            for Gene in self.tree.iter('Gene'):
                # capture the OMIM ID from the OMIM field
                Gene_symbol=Gene.get('Symbol')
                #print Gene.find('OMIM').text
                OMIM_genesymbol= Gene.find('OMIM').text
                Gene_info=Gene_symbol+" ("+str(OMIM_genesymbol)+")"
                 
                #create empty lists for phenotypes to avoid duplication
                pheno_list=[]
                #create a list to hold all the observed phenotypes
                list_of_tuples=[]
                #loop through the tree and look for each phenotype recorded for this variant
                for Phenotype in self.tree.iter('Phenotype'):
                    #if have already seen this phenotype pass
                    if Phenotype.get('Name') in pheno_list:
                        pass
                    #if have not seen this phenotype add to the list
                    else:
                        pheno_list.append(Phenotype.get('Name'))
                        # each phenotype can have multiple identifiers - capture the OMIM one
                        # within this phenotype section find all the xreference lists
                        xreflist=Phenotype.findall('XRefList')
                        #loop through this list 
                        for xref in xreflist:
                            for line in xref:
                                #and if it's an omim entry
                                if line.get('Type') == "MIM":
                                    #store the id
                                    MIM_ID= line.get('ID')
                                     
                                    #create a tuple of phenotype name and OMIM ID and add to list
                                    list_of_tuples.append((Phenotype.get('Name'),MIM_ID))
                 
                # look to see if this gene symbol has already been added to dict
                if Gene_info in list_of_genes:
                    #if it has loop through the list of tuples just created and check if it has already been seen
                    for i in list_of_tuples:
                        if i in list_of_genes[Gene_info]:
                            #if it has pass
                            pass
                        else:
                            #if not add the tuple to the list of tuples
                            list_of_genes[Gene_info].append(i)
                else:
                    #if the gene has not been seen yet add to dict
                    list_of_genes[Gene_info]=list_of_tuples
        
        
    def write_output_file(self,gene_list):
        outputfile = open("/home/aled/Google Drive/BIOINFORMATICS STP/CBI-7/Ush2A/clinvar/genemania_clinvar_results.txt","w")
        for i in gene_list:
            outputfile.write(str(i)+"\t")
        
        for j in list_of_genes:
            outputfile.write("\n"+j+"\t"+str(list_of_genes[j]))
        outputfile.close()

if __name__ == "__main__":
    # create object
    a = OMIM_API()
    
    # state search term
    gene = ["CYLC1","SLC17A4","CRYAA","RS1","SLC6A3","PAPPA","TAS2R46","EPX","SHCBP1L","HGF","GDF10","NOX3","LMO7","KRT18","GABRR1","ATXN3L","GPR88","IL9","NKAIN1","MSTN"]#,"ADGRV1"]
    #gene = ["USH1C","KCTD3","NINL","USH2A","CLRN1","HLA-G","DFNB31","PCDH15","PDZD7","CDH23"]
    #gene = ["WHRN"]
    for i in gene:
        a.read_page(i)
    a.write_output_file(gene)
