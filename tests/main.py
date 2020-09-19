import unittest

import eop
import pandas as pd
import numpy as np

class TestDataInstance(unittest.TestCase):
    test_data_a = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
    test_data_b =  pd.DataFrame({"doi": [77, 88, 99]})
    
    def test_sub(self):
        sub = eop.DataInstance(self.test_data_a.copy())
        main = eop.DataInstance(self.test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main["sub"], eop.DataInstance)
        
    def test_sub_loc(self):
        sub = eop.DataInstance(self.test_data_a.copy())
        main = eop.DataInstance(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.loc[[0, 2]]["sub"]["x"].iloc[1], 3)

    def test_sub_col(self):
        sub = eop.DataInstance(self.test_data_a.copy())
        main = eop.DataInstance(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main[("sub", "x")].loc[0], 1)

    def test_sub_type(self):
        sub = eop.B(self.test_data_a.copy())
        main = eop.A(self.test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main, eop.A)
        self.assertIsInstance(main["sub"], eop.B)
        
    def test_dtypes(self):
        sub = eop.Point3d(self.test_data_a.copy())
        main = eop.X(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.df.dtypes[("doi", np.NaN)], np.dtype("int64"))
        self.assertEqual(main["sub"].df.dtypes[("x",)], np.dtype("float64"))

if __name__ == '__main__':
    unittest.main()
