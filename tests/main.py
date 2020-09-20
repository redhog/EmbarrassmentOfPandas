import unittest

import eop
import pandas as pd
import numpy as np

        
class A(eop.DataInstance): pass

class B(eop.DataInstance): pass

class Point3D(eop.DataInstance):
    dtypes = {"x": np.dtype("float64"),
              "y": np.dtype("float64"),
              "z": np.dtype("float64")}

    def summary(self):
        return eop.DataInstance(pd.DataFrame({"summary": (["/".join(str(item) for item in row) for idx, row in self.df.iterrows()])}))
            
class X(eop.DataInstance):
    dtypes = {"doi": np.dtype("int64")}


class TestDataInstance(unittest.TestCase):
    test_data_a = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
    test_data_b =  pd.DataFrame({"doi": [77, 88, 99]})
    test_data_c =  pd.DataFrame({"nana": [2, 3, 5]})
    
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
        sub = B(self.test_data_a.copy())
        main = A(self.test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main, A)
        self.assertIsInstance(main["sub"], B)
        
    def test_method_type(self):
        sub = B(self.test_data_a.copy())
        main = A(self.test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main.head(), A)
        self.assertIsInstance(main.head()["sub"], B)
        
    def test_dtypes(self):
        sub = Point3D(self.test_data_a.copy())
        main = X(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.df.dtypes[("doi", "")], np.dtype("int64"))
        self.assertEqual(main["sub"].df.dtypes[("x",)], np.dtype("float64"))


    def test_summary(self):
        sub = Point3D(self.test_data_a.copy())
        main = X(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.summary()[("sub", "[Point3D]", "summary")].loc[0], "1.0/4.0/7.0")

    def test_summary_single_col(self):
        main = eop.DataInstance(self.test_data_a.copy())
        sub = X(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.summary()[("sub", "[X]", "doi", "[int64]")].loc[0], 77)

    def test_sub_sub(self):
        main = eop.DataInstance(self.test_data_b.copy())
        sub = A(self.test_data_a.copy())
        sub2 = B(self.test_data_c.copy())
        main["sub"] = sub
        main[("sub", "xxx")] = sub2
        self.assertIsInstance(main[("sub", "xxx")], B)

    def test_attributes(self):
        sub = eop.DataInstance(self.test_data_a.copy())
        main = eop.DataInstance(self.test_data_b.copy())
        sub.gazonk = 47
        main["sub"] = sub
        self.assertEqual(main["sub"].gazonk, 47)
        
if __name__ == '__main__':
    unittest.main()
