import unittest

from test.DataCleanerImplTest import DataCleanerImplTest
from test.FolderStructureLocalTest import FolderStructureLocalTest
from test.GraphDrugsTest import GraphDrugsTest
from test.WorkflowImplTest import WorkflowImplTest


class MyTestCase(unittest.TestSuite):

     def __init__(self):
         super().__init__()
         self.addTest(DataCleanerImplTest())
         self.addTest(FolderStructureLocalTest())
         self.addTest(GraphDrugsTest())
         self.addTest(WorkflowImplTest())

if __name__ == '__main__':
    unittest.main()
