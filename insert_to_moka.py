'''
The API cannot be used on the trust network and MOKA cannot be accessed from outside the trust network so two scripts are required.
This script takes the output of the read_api script.
For each panel it checks if it is already in MOKA, and if the panel has been updated.
If it's a new panel the gene panel and genes are instered into MOKA.
This script was designed to be run subsequently.
created by Aled 18 Oct 2016
'''
import pyodbc

class insert_PanelApp:
    def __init__(self):
        # the file containing the result of the API query
        self.API_result = "S:\\Genetics_Data2\\Array\\Audits and Projects\\161014 PanelApp\\PanelAppOut.txt"

        # variables for the database connection
        self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=devdatabase;")
        self.cursor = self.cnxn.cursor()

        # name of category in item category
        self.category_name = "NGS Panel version"

        # list of versions for item table
        self.list_of_versions = []

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

        # variable for the panel name and version
        self.panel_name_version = ''

        # variable to hold all the panels currently in the database
        self.all_panels = {}

        # key assigned to panel in item table
        self.VersionItemCategory = ''

        # all versions in database
        self.versions_in_db = []

        # new version key
        self.version_key = ""

        # list of genes within panel
        self.ensembl_ids = ""

        # key of newly inserted panel
        self.inserted_panel_key = ""

        # variable to hold the key for the existing gene panel
        self.panel_key = ""
        
        #ignore exception flag
        self.ignore=False
        self.notfound="not found"

    def check_item_category_table(self):
        '''this module checks the itemcategory table to find the key that marks an row as a NGS panel version.
        If not present inserts it. This should only be required the first time'''
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
            self.insert_query = "Insert into Itemcategory(itemcategory) values ('" + self.category_name + "')"
            self.insert_query_function()

    def get_list_of_versions(self):
        '''The API results are parsed to identify all the version numbers'''
        # need to parse the API result to find all the version numbers
        API_result = open(self.API_result, 'r')
        # split to capture the version number
        for i in API_result:
            split1 = i.split(':')
            names = split1[0].split('_')
            version = float(names[1])

            # create a list of unique version numbers
            if version not in self.list_of_versions:
                self.list_of_versions.append(version)

        # Call module to insert any new version numbers to the database
        self.insert_versions()

    def insert_versions(self):
        '''If a version number from the API result is not in the database insert it'''
        
        # first need to get the key used to mark version numbers in the item table
        self.fetch_key_qry = "select ItemCategoryID from ItemCategory where ItemCategory = '" + self.category_name + "'"
        self.exception_message = "Cannot return the itemcategoryID from itemcategory table when looking for 'ngs panel version'"

        # call fetch key module
        version_item_cat = self.fetch_key()
        self.VersionItemCategory = version_item_cat[0]

        # get list of versions already in table
        self.select_qry = "select Item from item where itemcategoryindex1ID=" + str(self.VersionItemCategory)
        self.select_qry_exception = "cannot find any versions in item table"
        list_of_versions = self.select_query()
        
        #loop through results and append to a list
        for i in list_of_versions:
            self.versions_in_db.append(float(i[0]))
        
        # if version in API result not already in db:
        for version in self.list_of_versions:
            if version not in self.versions_in_db:
                # add the version to the item table
                self.insert_query = "Insert into Item(Item,itemcategoryindex1ID) values (" + str(version) + "," + str(self.VersionItemCategory) + ")"
                self.insert_query_function()

    def all_existing_panels(self):
        '''This module extracts all the panels and the version numbers from the database'''
        # extract all the existing panels from the database
        self.select_qry = "select ItemA.item, ItemB.item from Item itemA, Item ItemB, NGSPanel where ItemA.ItemID=dbo.NGSPanel.Category and ItemB.itemID = dbo.NGSPanel.subCategory and itemA.ItemCategoryIndex1ID = " + str(self.item_category_NGS_panel) + " and itemB.ItemCategoryIndex1ID=" + str(self.VersionItemCategory)
        self.select_qry_exception = "cannot extract all panels and versions"
        
        # set flag so exception is not raised - this exception will only occur upon the very first insert 
        self.ignore=True
        all_panels = self.select_query()
        # if no panels in gene all_panels will be empty- looping through this will error!
        if all_panels == self.notfound:
            pass
        else:
            # loop through db results result and create a dict with the panel name as key and list of version numbers as value
            for i in all_panels:
                #i[0] is panel name, i[1] is version number
                #if panel_name already in dict, just add the version number
                if i[0] in self.all_panels:
                    self.all_panels[i[0]].append(i[1])
                else:
                    # otherwise add dictionary entry
                    self.all_panels[i[0]] = [i[1]]
        # unset ignore flag
        self.ignore=False

    def parse_PanelAPP_API_result(self):
        ''' This module loops through the API result. If the panel name is not in the database it inserts it. 
        If the panel already exists it checks the version number to see if the panel has been updated and if so the updated verison is inserted'''
        # open and parse the text file containing the api query result
        API_result = open(self.API_result, 'r')
        #loop through and extract required info
        for i in API_result:
            # split - example line  = Epidermolysis bullosa_0.8_amber:[list,of,genes]
            split1 = i.split(':')
            self.ensembl_ids = split1[1]
            names = split1[0].split('_')
            # removing any "'" from panel_name (messes up the sql query)
            panel_name = names[0].replace("'","")
            version = names[1]
            colour = names[2]

            #define the panel name as disease name and panel colour
            self.panel_name_version=panel_name+"_"+colour

            # check if panel is already in the database
            if self.panel_name_version in self.all_panels:
                self.fetch_key_qry = "select itemid from item where item = '" + self.panel_name_version + "'"
                panel_id = self.fetch_key()
                self.panel_key = panel_id[0]

            # if not insert to items table
            else:
                self.insert_query_return_key = "insert into item(item,ItemCategoryIndex1ID) values ('" + self.panel_name_version+ "'," + self.item_category_NGS_panel + ")"
                print self.insert_query_return_key
                key=self.insert_query_return_key_function()
                self.panel_key=key[0]

            #### Has the panel been updated?
            # get the maximum version number from the existing panels in db
            if self.panel_name_version in self.all_panels:
                max_version = max(self.all_panels[self.panel_name_version])
                exists=True
            else:
                max_version=0
                exists=False
            # if this panel is newer get the key for this version number from item table (all versions were inserted above)
            if version > max_version:
                if exists:
                    # first, if it exists need to deactivate the existing panel
                    self.insert_query="update ngspanel set active = 0 where category = "+ str(self.panel_key)
                    self.insert_query_function()
                # then get the itemid of the version 
                self.select_qry = "select itemid from item where item in ('" + str(version) + "') and ItemCategoryIndex1ID = " + str(self.VersionItemCategory)
                self.select_qry_exception = "Cannot get key for version number"
                version_key = self.select_query()
                self.version_key = version_key[0][0]

                # Insert the NGSpanel, returning the key 
                self.insert_query_return_key = "insert into ngspanel(category, subcategory, panel, panelcode, active) values (" + str(self.panel_key) + "," + str(self.version_key) + ",'" + self.panel_name_version + "','Pan',1)" 
                self.insert_query_exception="cannot get the key when inserting this panel"
                key=self.insert_query_return_key_function()
                self.inserted_panel_key= key[0]
                # update the table so the Pan number is created.
                self.insert_query = "update NGSPanel set PanelCode = PanelCode+cast(NGSPanelID as VARCHAR) where NGSPanelID = "+str(self.inserted_panel_key)
                self.insert_query_function()

                # Call module to insert gene list to NGSPanelGenes.
                self.add_genes_to_NGSPanelGenes()
            
            # if not a new version ignore
            else:
                pass
    
    def add_genes_to_NGSPanelGenes(self):
        '''This module inserts the list of genes into the NGSGenePanel. The HGNC table is queried to find the symbol and HGNCID from the ensembl id'''
        # list of cleaned gene ids:
        list_of_genes_cleaned=[]
        
        # convert the string containing gene list into a python list
        # split and remove all unwanted characters
        list_of_genes=self.ensembl_ids.split(",")
        for i in list_of_genes:
            i=i.replace("\"","").replace("[","").replace("]","").replace(" ","").rstrip()
            # append to list
            list_of_genes_cleaned.append(i)
        
        #loop through gene list
        for ensbl_id in list_of_genes_cleaned:
        # for each gene get the HGNCID and ApprovedSymbol
            #set ignore flag to ignore exception
            self.ignore=True
            self.select_qry="select HGNCID,ApprovedSymbol from dbo.GenesHGNC_current where EnsemblIDmapped="+ensbl_id
            self.select_qry_exception=self.panel_name_version+" can't find the gene from ensembl_id: "+ensbl_id
            gene_info=self.select_query()
            if gene_info==self.notfound:
                print self.select_qry_exception
            else:
                HGNCID=gene_info[0][0]
                ApprovedSymbol=gene_info[0][1]
    
                # insert each gene into the NGSPanelGenes table
                self.insert_query="insert into NGSPanelGenes(NGSPanelID,HGNCID,symbol) values ("+str(self.inserted_panel_key)+",'"+HGNCID+"','"+ApprovedSymbol+"')"
                self.insert_query_exception="can't insert gene into the NGSPanelGenes table"
                self.insert_query_function()
        
        self.ignore=False
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
    a.check_item_category_table()
    a.get_list_of_versions()
    a.all_existing_panels()
    a.parse_PanelAPP_API_result()
