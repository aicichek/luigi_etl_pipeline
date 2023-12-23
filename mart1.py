import luigi
import pandas as pd
from sqlalchemy import create_engine, text
import os
import numpy as np
from connection import server, database_master, database_bi, username, password, port

class ConnectToDatabase(luigi.Task):
    def run(self):
        engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}:{port}/{database_master}')
        self.output()._touchz()

    def output(self):
        return luigi.LocalTarget('conn_comp.txt')

class DropSQLQuery(luigi.Task):
    script_path = luigi.Parameter(default='sql\drop_mart1.sql')

    def requires(self):
        return ConnectToDatabase()

    def run(self):
        with open(self.script_path, 'r', encoding='utf-8') as file:
            query = file.read()

            engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}:{port}/{database_bi}')
            with engine.connect() as connection:
                query_obj = text(query)
                connection.execute(query_obj)
                connection.commit()
        self.output()._touchz()

    def output(self):
        return luigi.LocalTarget(f'{self.script_path}_complete.txt')

class CreateSQLQuery(luigi.Task):
    script_path = luigi.Parameter(default='sql\create_mart1.sql')

    def requires(self):
        return DropSQLQuery()


    def run(self):
        with open(self.script_path, 'r', encoding='utf-8') as file:
            query = file.read()
            engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}:{port}/{database_bi}')
            with engine.connect() as connection:
                query_obj = text(query)
                connection.execute(query_obj)
        self.output()._touchz()

    def output(self):
        return luigi.LocalTarget(f'{self.script_path}_complete.txt')


class SelectData(luigi.Task):
    script_paths = luigi.ListParameter(default=['sql\mart1select1.sql', 'sql\mart1select2.sql', 'sql\mart1select3.sql'])

    def requires(self):
        return [CreateSQLQuery() for _ in range(len(self.script_paths))]

    def run(self):
        dfs = []
        for script_path in self.script_paths:
            with open(script_path, 'r', encoding='utf-8') as file:
                query = file.read()
                engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}:{port}/{database_bi}')
                with engine.connect() as connection:
                    df = pd.read_sql_query(text(query), connection)
                    df = df.drop_duplicates(subset=['Pline', 'pNumber'], keep='first')
                    dfs.append(df)

        result_df = pd.concat(dfs, ignore_index=True)
        result_df = result_df.drop_duplicates(subset=['Pline', 'pNumber'], keep='first')
        result_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget('select_compl.pkl')

class EncodeDecodeData(luigi.Task):
    def requires(self):
        return SelectData()

    def run(self):
        result_df = pd.read_csv(self.input().path)
        
        last_four_columns = result_df.iloc[:, -4:]
        encoded_columns = last_four_columns.apply(lambda col: col.astype(str).apply(lambda x: x.encode('latin1').decode('cp1251')))
        result_df.iloc[:, -4:] = encoded_columns
        
        result_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget('encoded_decoded_data.pkl')


class AddColumn(luigi.Task):
    def requires(self):
        return EncodeDecodeData()

    def run(self):
        result_df = pd.read_csv(self.input().path)
        result_df['rentabelnost'] = np.where(result_df['sumdol'] != 0, (result_df['sumdol'] - result_df['ssdol']) / result_df['sumdol'], np.nan)
        result_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget('data_with_column.pkl')

class SaveToDatabase(luigi.Task):
    def requires(self):
        return AddColumn()

    def run(self):
        engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}:{port}/{database_bi}')
        result_df = pd.read_csv(self.input().path)
        result_df.to_sql(name='MART1', con=engine, index=False, if_exists='replace', schema='dbo')
        try:
            os.remove('conn_comp.txt')
            os.remove('select_compl.pkl')
            os.remove('data_with_column.pkl')
            os.remove('drop_mart1.sql_complete.txt')
            os.remove('encoded_decoded_data.pkl')
            os.remove('create_mart1.sql_complete.txt')
        except: print()
    def output(self):
        return luigi.LocalTarget('save_to_database_complete.txt')
    
if __name__ == '__main__':
    luigi.build([SaveToDatabase()], local_scheduler=False)
