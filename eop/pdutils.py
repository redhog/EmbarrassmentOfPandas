import pandas as pd

def square_assign(df, index, columns, repl, rename=False, remove=True, replace=True, add=True):
    if rename:
        assert len(index) == len(repl.index) and len(columns) == len(repl.columns), "Unable to do rename assignement if sizes aren't the same"
        repl = repl.copy()
        df = df.copy()
        repl.index = index
        repl.columns = columns
        df.loc[index, columns] = repl
        return df
    else:
        index_to_add = repl.index.difference(index)
        index_to_replace = index.intersection(repl.index)
        index_to_remove = index.difference(repl.index)

        columns_to_add = repl.columns.difference(columns)
        columns_to_replace = columns.intersection(repl.columns)
        columns_to_remove = columns.difference(repl.columns)

        if not add:
            repl = repl.loc[repl.index.difference(index_to_add), repl.columns.difference(columns_to_add)]
            columns_to_add = []
            index_to_add = []
        if not replace:
            repl = repl.loc[repl.index.difference(index_to_replace), repl.columns.difference(columns_to_replace)]
            columns_to_replace = []
            index_to_replace = []
        if not remove:
            columns_to_remove = []
            columns_to_replace = []
        
        df = df.drop(columns=columns_to_remove, index=index_to_remove)

        new_rows = pd.DataFrame(index=index_to_add, columns=df.columns)
        df = pd.concat([df, new_rows], axis=0)
        new_cols = pd.DataFrame(columns=columns_to_add, index=df.index)
        df = pd.concat([df, new_cols], axis=1)

        df.loc[repl.index, repl.columns] = repl
        return df
