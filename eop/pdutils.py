import pandas as pd

def square_assign(df, index, columns, repl):
    index_to_add = repl.index.difference(index)
    index_to_replace = index.intersection(repl.index)
    index_to_remove = index.difference(repl.index)

    columns_to_add = repl.columns.difference(columns)
    columns_to_replace = columns.intersection(repl.columns)
    columns_to_remove = columns.difference(repl.columns)

    df = df.drop(columns=columns_to_remove, index=index_to_remove)

    new_rows = pd.DataFrame(index=index_to_add, columns=df.columns)
    df = pd.concat([df, new_rows], axis=0)
    new_cols = pd.DataFrame(columns=columns_to_add, index=df.index)
    df = pd.concat([df, new_cols], axis=1)

    df.loc[repl.index, repl.columns] = repl
    return df
