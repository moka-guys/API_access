'''
Created on 22 Jul 2016

@author: aled
'''

from xml.etree import ElementTree

class ReadXML():
    def __init__(self):
        self.file =  "/home/aled/Google Drive/BIOINFORMATICS STP/CBI-7/pubmed_ebot/microcephaly_genes_in_literature"
        self.MIM_list=[]
    
    def read_XML(self):
        self.tree = ElementTree.parse(self.file)
        
        for DocumentSummary in self.tree.iter('DocumentSummary'):
            MIM=DocumentSummary.find('Mim')
            mim_num=MIM.find('int').text
            self.MIM_list.append(mim_num)
            
        print len(set(self.MIM_list))
    
if __name__ =="__main__":
    a=ReadXML()
    a.read_XML()

