'''
The API cannot be used on the trust network and MOKA cannot be accessed from outside the trust network so two scripts are required.
This script takes the output of the read_api script.
Each panel has a stable identifer (hash) and a human readable name, colour and version.
Using the hash, for each panel it checks if it is already in MOKA, and if the panel has been updated.
If it's a new panel the gene panel and genes are inserted into MOKA.
This script was designed to be run repeatedly over time.

steps in more detail:
1) Look to see if 'NGS Panel Version' is in the lookup (item) table. if not insert it.
2) Loop through the API result creating a list of version numbers 
3) Pull out all versions in the item table. Insert any version numbers that are not already in the database
4) Pull out the existing panels and versions from database. This creates a dictonary with panel_hash as key and the values as a list of versions eg {panel_hash_green:[0.1,0.2]}
5) Loop through the API result:
    - check if the panel name is already in dict
        - checks if the version number from API is > than that in the db
            - If newer insert new panel name and version, deactivating the older version
6) Add the genes to the NGSpanelsGenes table
7) A check is then done using the list if gene symbols from each panel to check all the genes are imported. This uses a translation copy of the HGNC_current table which has been manually curated to get around the outdated symbols in panelapp. 

NB when run for the very first time there must be one version in the item table (imported manually) otherwise the script fails.

created by Aled 18 Oct 2016
'''
import pyodbc

class insert_PanelApp:
    def __init__(self):
        # the file containing the result of the API query
        self.API_result = "\\\\gstt.local\\apps\\Moka\\Files\\Software\\PanelApp\\20180828_PanelAppOut_modified.txt"
        self.API_symbol_result = "\\\\gstt.local\\apps\\Moka\\Files\\Software\\PanelApp\\20180828_PanelAppOut_symbols.txt"
        
        # list to hold the API response
        self.API_list = []
        # list to hold the database contents
        self.db_list = []

        # variables for the database connection
        #self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=mokadata;")
        self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=devdatabase;")
        self.cursor = self.cnxn.cursor()

        # name of category in item category
        self.category_name = "NGS Panel version"

        # list of versions for item table
        self.versions_in_api = [0.0]

        # query used when returning a key after select statement
        self.fetch_key_qry = ""
        self.exception_message = ""

        # query used for select_query
        self.select_qry = ""
        self.select_qry_exception = ""

        # used in insert query
        self.insert_query = ""
        self.insert_query_exception = ""

        # value for item_category_NGS_panel in item table
        self.item_category_NGS_panel = "48"

        # variables for the panel name and version
        self.panel_name_colour = ''
        self.panel_hash_colour=''

        # variable to hold all the panels currently in the database
        self.all_panels_in_db = {}

        # key assigned to panel in item table
        self.VersionItemCategory = ''

        # all versions in database
        self.versions_in_db = []

        # a list to hold the api gene symbols output
        self.API_symbols = {}

        # new version key
        self.version_key = ""

        # key of newly inserted panel
        self.inserted_panel_key = ""

        # variable to hold the key for the existing gene panel
        self.panel_key = ""
        
        #ignore exception flag
        self.ignore=False
        self.notfound="not found"
        
        #id for the moka user
        self.moka_user="1201865448"

    def check_item_category_table(self):
        '''this module checks the item category table to find the key that marks an row as a NGS panel version.
        If not present inserts it. This should only be required the first time the script is run for a database'''
        
        # query to extract all rows in table
        self.select_qry = "select itemcategory from itemcategory"
        self.select_qry_exception = "cannot retrieve all itemcategories from itemcategory table"
        item_cat = self.select_query()
        
        # loop through the table contents and append to a list
        item_cat_list = []
        for i in item_cat:
            item_cat_list.append(i[0])
        
        # If there is no entry for NGS Panel version insert it 
        if self.category_name not in item_cat_list:
            self.insert_query = "Insert into Itemcategory(itemcategory) values ('%s')" % (self.category_name)
            self.insert_query_function()

    def get_list_of_versions(self):
        '''The API results are parsed to identify all the version numbers'''
        # Parse the API result to find all the version numbers
        with open(self.API_result, 'r') as API_output:
            self.API_list = API_output.readlines()
        
        # split to capture the version number
        for panel in self.API_list:
            split1 = panel.split(':')
            names = split1[0].split('_')
            version = names[2]
            # add version to the list
            self.versions_in_api.append(version)
        
        # condense this to a unqiue list
        self.versions_in_api = set(self.versions_in_api)
        
        # Call module to insert any new version numbers to the database
        self.insert_versions()

    def insert_versions(self):
        '''If a version number from the API result is not in the database insert it'''
        
        # first need to get the key used to mark version numbers in the item table
        self.fetch_key_qry = "select ItemCategoryID from ItemCategory where ItemCategory = '%s'" % self.category_name
        self.exception_message = "Cannot return the itemcategoryID from itemcategory table when looking for 'ngs panel version'"

        # call fetch key module
        version_item_cat = self.fetch_key()
        self.VersionItemCategory = version_item_cat[0]

        # get list of versions already in table
        self.select_qry = "select Item from item where itemcategoryindex1ID= %s" % (self.VersionItemCategory)
        self.select_qry_exception = "cannot find any versions in item table"
        list_of_db_versions = self.select_query()
        
        #loop through results 
        for version in list_of_db_versions:
            # Even though only one column is returned in the query a tuple is returned
            # append the the first item in the tuple to the list
            self.versions_in_db.append(version[0])
        
        # if version in API result not already in db:
        for version in self.versions_in_api:
            if version not in self.versions_in_db:
                # add the version to the item table
                self.insert_query = "Insert into Item(Item,itemcategoryindex1ID) values (%s,%s)" % (version, self.VersionItemCategory)
                self.insert_query_function()

    def all_existing_panels(self):
        '''This module extracts all the panels and the version numbers from the database'''
        
        # extract all the existing panels from the database
        # retrun the panelhash_colour and version number
        self.select_qry = "select ItemA.item, ItemB.item from Item itemA, Item ItemB, NGSPanel where ItemA.ItemID=dbo.NGSPanel.Category and ItemB.itemID = dbo.NGSPanel.subCategory and itemA.ItemCategoryIndex1ID = %s and itemB.ItemCategoryIndex1ID = %s" % (self.item_category_NGS_panel, self.VersionItemCategory)
        self.select_qry_exception = "cannot extract all panels and versions"
               
        # execute query to pull out all panels and version in db
        self.db_list = self.select_query()
        
        # if no panels in gene all_panels will be empty- looping through this will error!
        if len(self.db_list) > 0:
            # loop through db results result and create a dict with the panel name as key and list of version numbers as value
            for i in self.db_list:
                # i[0] is panel name, i[1] is version number
                # if panel_name already in dict, just add the version number
                # convert panelhash to upper to get around mismatched colour capitalisation in panelapp (green and Green)
                panelhash = i[0].upper()
                version = i[1]
                # if multiple versions the panelhash may already be in the dict - if so append the version number
                if panelhash in self.all_panels_in_db:
                    self.all_panels_in_db[panelhash].append(version)
                else:
                    # otherwise create dictionary entry
                    self.all_panels_in_db[panelhash] = [version]


    def parse_PanelAPP_API_result(self):
        ''' This module loops through the API result. If the panel name is not in the database it inserts it. 
        If the panel already exists it checks the version number to see if the panel has been updated and if so the updated verison is inserted'''
        # open and parse the text file containing the api query result
        
        #loop through and extract required info
        for panel in self.API_list:
            # split - example line  = panelhash_Epidermolysis bullosa_0.8_amber:[list,of,ensemblids]
            split1 = panel.split(':')
            
            # take everything before the colon and split on underscore
            names = split1[0].split('_')
            # panel hash is first item
            panel_hash = str(names[0])
            # panel name is second item removing any "'" from panel_name (messes up the sql query)
            panel_name = str(names[1].replace("'",""))
            # version in third item
            version = str(names[2])
            # panel colour is fourth item
            colour = str(names[3])
            
            # define the unique panel identifier as panel hash _ panel colour - this is imported into item table and should be one of these for multiple versions
            self.panel_hash_colour = panel_hash + "_" + colour
            
            # human readable panel name is panel name (Panel App Green v1.0) - this goes into ngspanel.panel
            self.panel_name_colour = panel_name + " (Panel App " + colour + " v" + version + ")"
            
            # check if panel is already in the item table.
            # convert to upper case as all keys in self.all_panels_in_db were converted to upper case when adding
            if self.panel_hash_colour.upper() in self.all_panels_in_db:
                # capture the itemid for that panel from the items table
                # there may be two entries in the item table if there is Green and green - take the highest
                # this will be stored in the category column in ngspanels table
                self.fetch_key_qry = "select top 1 itemid from item where item = '" + self.panel_hash_colour + "' order by itemid desc"
                self.panel_key = self.fetch_key()[0]

            # if panel not in items table already insert it
            else:               
                # set insert query
                self.insert_query_return_key = "insert into item(item,ItemCategoryIndex1ID) values ('%s',%s)" % (self.panel_hash_colour,self.item_category_NGS_panel)
                # insert and capture the key (itemid)
                self.panel_key=self.insert_query_return_key_function()[0]

            #### Has the panel been updated?
            # need to calculate if the version numbers have increased - 1.4 is < 1.10 (1 point four is less than 1 point 10)
            # if directly compare numbers this would fail eg 1.102 < 1.2 where actually 1.102 is 100 releases higher
            
            # set default max_value at to -1
            max_version = "-1.0"
            
            # if the panel exists in moka
            if self.panel_hash_colour.upper() in self.all_panels_in_db:
                # loop through all versions in the database for that panel
                for db_version in self.all_panels_in_db[self.panel_hash_colour.upper()]:
                    # set the major and minor release numbers (splitting on ".") for max_version and db version
                    db_major_version = int(db_version.split(".")[0])
                    db_minor_version = int(db_version.split(".")[1])
                    max_major_version = int(max_version.split(".")[0])
                    max_minor_version = int(max_version.split(".")[1])
                    # if the db major version is higher than the current max major version then this version number is higher
                    if db_major_version > max_major_version:
                        max_version = db_version
                    # if same major release but increased minor release eg 1.2 -> 1.102 change the version number
                    elif db_major_version == max_major_version and db_minor_version > max_minor_version:
                        max_version = db_version
                    # else this version number is lower than a previously seen version
                    else:
                        pass

            # To determine if the panel needs adding one of the below is true:
            # max version = "-1.0" (the panel_hash_colour was not found in db). 
            # the major version number has increased
            # the major number is the same but minior version number has increased
            if max_version == "-1.0" or int(version.split(".")[0]) > int(str(max_version).split(".")[0]) or int(version.split(".")[0]) == int(str(max_version).split(".")[0]) and int(version.split(".")[1]) > int(str(max_version).split(".")[1]):
                # first, if it exists need to deactivate the existing panel(s)
                self.insert_query = "update ngspanel set active = 0 where category in (select itemid from dbo.Item where item = '%s')" % self.panel_hash_colour
                self.insert_query_function()
                
                # then get the itemid of the version 
                self.select_qry = "select itemid from item where item  = '%s' and ItemCategoryIndex1ID = %s" % (version, self.VersionItemCategory)
                self.select_qry_exception = "Cannot get key for version number"
                version_key = self.select_query()
                self.version_key = version_key[0][0]

                # Insert the NGSpanel, returning the key 
                self.insert_query_return_key = "insert into ngspanel(category, subcategory, panel, panelcode, active, checker1,checkdate,PanelType) values (%s,%s,'%s','Pan',1,%s,CURRENT_TIMESTAMP,2)" % (self.panel_key,self.version_key,self.panel_name_colour,self.moka_user) 
                self.insert_query_exception = "cannot get the key when inserting panel %s" % (self.panel_name_colour)
                key = self.insert_query_return_key_function()
                self.inserted_panel_key = key[0]
                
                # update the table so the Pan number is created.
                self.insert_query = "update NGSPanel set PanelCode = PanelCode+cast(NGSPanelID as VARCHAR) where NGSPanelID = %s" % self.inserted_panel_key
                self.insert_query_function()

                # Call module to insert gene list to NGSPanelGenes.
                self.add_genes_to_NGSPanelGenes(split1[1])

            # if not a new version ignore
            else:
                pass
    
    def add_genes_to_NGSPanelGenes(self, list_of_genes):
        '''This module inserts the list of genes into the NGSGenePanel. The HGNC table is queried to find the symbol and HGNCID from the ensembl id'''
        # list of cleaned gene ids:
        list_of_genes_cleaned = []

        # convert the string containing gene list into a python list
        # split and remove all unwanted characters
        for gene in list_of_genes.split(","):
            gene = gene.replace("\"","").replace("[","").replace("]","").replace(" ","").rstrip()
            # append to list
            list_of_genes_cleaned.append(gene)

        #loop through gene list
        for ensbl_id in list_of_genes_cleaned:
            if len(ensbl_id)>5:
                # for each ensemblid get the HGNCID and PanelAppSymbol from use the hgnc translation table.
                #set ignore flag to ignore exception
                self.ignore = True
                self.select_qry = "select HGNCID,PanelApp_Symbol from dbo.GenesHGNC_current_translation where EnsemblID_PanelApp = %s" % ensbl_id.replace("u","")
                self.select_qry_exception = "%s: can't find the gene from ensembl_id: %s " %(self.panel_name_colour ,ensbl_id)
                # if no result pass
                gene_info = self.select_query()
                if gene_info == self.notfound:
                    pass
                # if found a match import the row into NGSpanelGenes table
                else:
                    HGNCID = gene_info[0][0]
                    PanelApp_Symbol = gene_info[0][1]
                    
                    # insert each gene into the NGSPanelGenes table
                    self.insert_query = "insert into NGSPanelGenes(NGSPanelID,HGNCID,symbol,checker,checkdate) values (%s,'%s','%s',%s,CURRENT_TIMESTAMP)" % (self.inserted_panel_key, HGNCID, PanelApp_Symbol, self.moka_user)
                    self.insert_query_exception = "can't insert gene into the NGSPanelGenes table"
                    self.insert_query_function()
        
        #unset ignore variable
        self.ignore=False
        
        # Call module to insert any gene symbols which do not have an ensemblID in panel app, or in the HGNC_translation table.
        self.check_for_missing_genes()
    
    def check_for_missing_genes(self):
        """
        Compare the genes that were imported to the database with those in the api symbols list
        """
        # pull out all the PanelApp gene symbols for the genes in that panel
        self.select_qry = "select PanelApp_Symbol from dbo.GenesHGNC_current_translation, ngspanelgenes, dbo.NGSPanel where NGSPanel.NGSPanelID = NGSPanelGenes.NGSPanelID and NGSPanelGenes.HGNCID=GenesHGNC_current_translation.HGNCID and Panel = '%s'" % self.panel_name_colour
        self.select_qry_exception = "Cannot find the genes in this panel:%s. This is probably because the ensembl ID is missing for this panel in the panelapp out file. Copy the ensembl id from the HGNC snapshot table." % self.panel_name_colour
        imported_genes = self.select_query()
                
        # create and populate list to hold symbols
        db_list = []
        # lop through the select query reponse
        for gene in imported_genes:
            # add the gene symbol to the list
            db_list.append(str(gene[0]))
        
        # create a list to populate with error messages to print
        to_print = []
        
        
        # assess if any symbols from the api aren't in moka
        # loop through the genes for this panel from the dictionary populated from the api symbol list
        for api_gene in self.API_symbols[self.panel_hash_colour]:
            # if the gene symbol is not in the list of gene symbols
            if api_gene not in db_list:
                # add the error statement to the list
                to_print.append(api_gene + " missing from moka. is there a ensembl ID for this gene?")
        
        # check if there are any symbols imported to Moka that aren't present in the API
        for db_gene in db_list:
            # if this gene symbol not in the API list
            if db_gene not in self.API_symbols[self.panel_hash_colour]:
                # add the error statement to the list
                to_print.append(db_gene + " imported to moka but symbol not in API. is there a errant ensembl ID in the api? (look if the panelapp_ensemblid in moka for this gene is in the API)")
        
        # if there are any print statements
        if len(to_print) > 0:
            # print the panel info
            print "\n" + self.panel_name_colour, self.panel_hash_colour
            print "NB if errant genes are reported in Moka AND genes are missing from the API it's probably that moka.panelapp_symbol == panelapp symbol "
            # loop through the print statements and print
            print "\n".join(to_print)

    def populate_api_symbols_dict(self):
        '''
        This module parses the list of symbols retrieved from the API and checks each one is in the database.
        This check uses a selection of manually curated genes (defined as PanelAppGeneSymbolCheck is not null).
        Any that are missing are reported and may need manual curation in the future
        '''
        # open and parse the text file containing the api query result
        with open(self.API_symbol_result, 'r') as API_symbol_file:
            for line in API_symbol_file.readlines():
                # split - example line  = panelhash_Epidermolysis bullosa_0.8_amber:[list,of,genesymbols]
                splitline = line.split(':')
                # everything before the colon
                names = splitline[0].split('_')
                # panelhash
                panel_hash = names[0]
                # panel name
                panel_name = names[1].replace("'","")# removing any "'" from panel_name (messes up the sql query)
                # version number
                version = names[2]
                # colour
                colour = names[3]
                # panel hash colour 
                panel_hash_colour = panel_hash + "_" + colour
                #rebuild the panelapp name
                panel_name_colour = panel_name + " (Panel App " + colour + " v" + str(version) + ")"
                
                # create a entry in the dictionary with the panel hash with empty list as value
                self.API_symbols[panel_hash_colour] = []
                # create a list of the symbols after the colon (splitting on ',')
                genes = splitline[1].split(',')
                # loop through list
                for gene in genes:
                    # remove all unwanted characters
                    gene = gene.replace("[","").replace("'","").replace("[","").replace("'","").replace("]","").replace(" ","").replace("\n","").rstrip()
                    # append the gene symbol to the list
                    self.API_symbols[panel_hash_colour].append(gene)

    def fetch_key(self):
        '''This function is called to retrieve a single entry from a select query'''
        # Perform query and fetch one
        result = self.cursor.execute(self.fetch_key_qry).fetchone()

        # return result
        if result:
            return(result)
        else:
            raise Exception(self.exception_message)

    def select_query(self):
        '''This function is called to retrieve the whole result of a select query '''
        # Perform query and fetch all
        result = self.cursor.execute(self.select_qry).fetchall()

        # return result
        if result:
            return(result)
        elif self.ignore:
            return(self.notfound)
        else:
            raise Exception(self.select_qry_exception)

    def insert_query_function(self):
        '''This function executes an insert query'''
        # execute the insert query
        self.cursor.execute(self.insert_query)
        self.cursor.commit()

    def insert_query_return_key_function(self):
        '''This function executes an insert query and returns the key of the newly created row'''
        # Perform insert query and return the key for the row
        self.cursor.execute(self.insert_query_return_key)
        self.cursor.commit()
        #capture key
        self.cursor.execute("SELECT @@IDENTITY")
        key = self.cursor.fetchone()
 
        # return result
        if key:
            return(key)
        elif self.ignore:
            return(self.notfound)
        else:
            raise Exception(self.insert_query_exception)


if __name__ == "__main__":
    a = insert_PanelApp()
    a.populate_api_symbols_dict()
    a.check_item_category_table()
    a.get_list_of_versions()
    a.all_existing_panels()
    a.parse_PanelAPP_API_result()
