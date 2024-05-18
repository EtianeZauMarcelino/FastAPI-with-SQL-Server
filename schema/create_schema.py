from database.connection import SQl_Server_Connection
# from src.models.user import User
import pandas as pd
import pandera as pa


db = SQl_Server_Connection(dbname='AdventureWorks2019', user='etiane', password='HPeNG4g7D4JbdSk3qgup')

def get_persons(Person_Id:int):
    db.connect()
    df_person = db.get_person(f"""
                         SELECT TOP (1000) [BusinessEntityID]
                            ,[PersonType]
                            ,[NameStyle]
                            ,[Title]
                            ,[FirstName]
                            ,[MiddleName]
                            ,[LastName]
                            ,[Suffix]
                            ,[EmailPromotion]
                            ,[AdditionalContactInfo]
                            ,[rowguid]
                            ,[ModifiedDate]
                        FROM [AdventureWorks2019].[Person].[Person]
                        """)
    
    db.close()
    return df_person


if __name__ == '__main__':
    try:
        # db = SQl_Server_Connection(dbname='AdventureWorks2019', user='etiane', password='HPeNG4g7D4JbdSk3qgup')
        # db.connect()
        # print(db.get_person(Person_Id=8))

        df = get_persons()

        schema_AdventureWorks = pa.infer_schema(df)

        with open(f'schema\schema_AdventureWorks_Person.py', 'w', encoding='utf-8') as schema_Adventure_Works:
            schema_Adventure_Works.write(schema_AdventureWorks.to_script())

    except Exception as e:
        print(e)