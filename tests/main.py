import unittest

import eop
import pandas as pd

class TestDataInstance(unittest.TestCase):
    test_data_a = pd.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
    test_data_b =  pd.DataFrame({"fie": [7, 8, 9]})
    
    def test_sub(self):
        sub = eop.DataInstance(self.test_data_a.copy())
        main = eop.DataInstance(self.test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main["sub"], eop.DataInstance)
        
    def test_sub_loc(self):
        sub = eop.DataInstance(self.test_data_a.copy())
        main = eop.DataInstance(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.loc[[0, 2]]["sub"]["foo"].iloc[1], 3)

    def test_sub_col(self):
        sub = eop.DataInstance(self.test_data_a.copy())
        main = eop.DataInstance(self.test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main[("sub", "foo")].loc[0], 1)

    def test_sub_type(self):
        sub = eop.B(self.test_data_a.copy())
        main = eop.A(self.test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main, eop.A)
        self.assertIsInstance(main["sub"], eop.B)

if __name__ == '__main__':
    unittest.main()
