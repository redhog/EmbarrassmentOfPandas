import unittest

import eop
import pandas as pd
import numpy as np

        
class A(eop.DataInstance): pass

class B(eop.DataInstance): pass

class Point3D(eop.DataInstance):
    DTypes = {"x": np.dtype("float64"),
              "y": np.dtype("float64"),
              "z": np.dtype("float64")}
    def summary(self):
        return eop.DataInstance(pd.DataFrame({"summary": (["/".join(str(item) for item in row) for idx, row in self.df.iterrows()])}))
            

class X(eop.DataInstance):
    DTypes = {"doi": np.dtype("int64")}


class Y(eop.DataInstance):
    DTypes = {"doi": np.dtype("int64"), "sub": Point3D}


test_data_a = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 8, 9]})
test_data_b =  pd.DataFrame({"doi": [77, 88, 99]})
test_data_c =  pd.DataFrame({"nana": [2, 3, 5]})

class TestDataInstance(unittest.TestCase):
    
    def test_sub(self):
        sub = eop.DataInstance(test_data_a.copy())
        main = eop.DataInstance(test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main["sub"], eop.DataInstance)
        
    def test_sub_loc(self):
        sub = eop.DataInstance(test_data_a.copy())
        main = eop.DataInstance(test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.loc[[0, 2]]["sub"]["x"].iloc[1], 3)

    def test_sub_type(self):
        sub = B(test_data_a.copy())
        main = A(test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main, A)
        self.assertIsInstance(main["sub"], B)
        
    def test_method_type(self):
        sub = B(test_data_a.copy())
        main = A(test_data_b.copy())
        main["sub"] = sub
        self.assertIsInstance(main.head(), A)
        self.assertIsInstance(main.head()["sub"], B)
        
    def test_dtypes(self):
        sub = Point3D(test_data_a.copy())
        main = X(test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.dtypes["doi"], np.dtype("int64"))
        self.assertEqual(main["sub"].dtypes["x"], np.dtype("float64"))

    @unittest.skip("Not yet implemented")
    def test_summary(self):
        sub = Point3D(test_data_a.copy())
        main = X(test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.summary()[("sub", "[Point3D]", "summary")].loc[0], "1.0/4.0/7.0")

    @unittest.skip("Not yet implemented")
    def test_summary_single_col(self):
        main = eop.DataInstance(test_data_a.copy())
        sub = X(test_data_b.copy())
        main["sub"] = sub
        self.assertEqual(main.summary()[("sub", "[X]", "doi", "[int64]")].loc[0], 77)

    def test_sub_sub(self):
        main = eop.DataInstance(test_data_b.copy())
        sub = A(test_data_a.copy())
        sub2 = B(test_data_c.copy())
        main["sub"] = sub
        main["sub"]["xxx"] = sub2
        self.assertIsInstance(main["sub"]["xxx"], B)

    def test_attributes(self):
        sub = eop.DataInstance(test_data_a.copy())
        main = eop.DataInstance(test_data_b.copy())
        sub.gazonk = 47
        main["sub"] = sub
        self.assertEqual(main["sub"].gazonk, 47)

    def test_flatten(self):
        sub = Point3D(test_data_a.copy())
        main = Y(test_data_b.copy())
        main["sub"] = sub

        unflattened = Y(main.flatten())
        self.assertEqual(str(main), str(unflattened))
        
class TestDataSet(unittest.TestCase):
    def test_contains_tag(self):
        ds = eop.DataSet()
        ds["a", "b"] = "Foo"
        self.assertNotIn("c", ds)
        self.assertIn("a", ds)
        self.assertIn("b", ds)

    def test_contains_instance(self):
        ds = eop.DataSet()
        ds["a", "b"] = "Foo"
        self.assertIn("Foo", ds)

    def test_get_tags(self):
        ds = eop.DataSet()
        ds["a", "b"] = "Foo"
        self.assertIn("a", ds["Foo"])
        self.assertIn("b", ds["Foo"])
        
    def test_no_tag(self):
        ds = eop.DataSet()
        ds[:] = "Notags"
        self.assertIn("Notags", ds)
        self.assertEqual(ds["Notags"].tags, set())

    def test_intersection_query(self):
        ds = eop.DataSet()
        ds["a", "b"] = "Foo"
        ds["a", "c"] = "Bar"
        ds["b", "c"] = "Fie"
        ds["a", "d"] = "Hehe"
        self.assertEqual(ds["b", "c"], {"Fie"})
        self.assertIn(("b", "c"), ds)
        self.assertNotIn(("b", "d"), ds)

    def test_type_query(self):
        ds = eop.DataSet()
        ds["a"] = a1 = eop.A({"foo": [1]})
        ds["a"] = a2 = eop.B({"foo": [2]})
        ds["b"] = a3 = eop.B({"foo": [3]})
        self.assertEqual(ds[eop.A, "a"], {a1})
        self.assertEqual(ds[eop.B, "a"], {a2})
        self.assertEqual(ds[eop.B, "b"], {a3})

    def test_complex_tag(self):
        ds = eop.DataSet()
        ds[{"src": "nanana"}] = "lala"
        self.assertEqual(len(ds["lala"].tags), 1)
        self.assertEqual(list(ds["lala"].tags)[0]["src"], "nanana")

    def test_more_complex_tag(self):
        ds = eop.DataSet()
        a = eop.A({"foo": [1]})
        ds[{"src": a}] = "lala"
        self.assertEqual(len(ds["lala"]), 1)
        self.assertEqual(ds[{"src": a}], {"lala"})

    def test_complex_tag_slice_syntax(self):
        ds = eop.DataSet()
        ds["src": "nanana"] = "lala"
        self.assertEqual(len(ds["lala"]), 1)
        self.assertEqual(list(ds["lala"].tags)[0]["src"], "nanana")

    def test_add_tag(self):
        ds = eop.DataSet()
        ds["a", "b"] = "Foo"
        ds["a", "c"] = "Bar"
        ds["b", "c"] = "Fie"
        ds["a", "b"] += "d"        
        self.assertEqual(ds["d"], {"Foo"})

    def test_remove_tag(self):
        ds = eop.DataSet()
        ds["a", "b"] = "Foo"
        ds["a", "c"] = "Bar"
        ds["b", "c"] = "Fie"
        ds["a", "b"] -= "b"        
        self.assertEqual(ds["b"], {"Fie"})

    def test_triggers(self):
        ds = eop.DataSet()
        trigger_status = {}
        @eop.on(ds["a"])
        def on_a(*tags, **kw):
            trigger_status["result"] = (tags, kw)
            
        ds["a", "b"] = "Foo"
        self.assertIn("result", trigger_status)
        self.assertEqual(trigger_status["result"][1]["action"], "add")
        self.assertEqual(trigger_status["result"][1]["instance"], "Foo")
        del trigger_status["result"]
        
        ds["b", "c"] = "Fie"
        self.assertNotIn("result", trigger_status)
        
if __name__ == '__main__':
    unittest.main()
