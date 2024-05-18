from database.connection import SQl_Server_Connection
from schema.schema_AdventureWorks_Person import schema
import pandas as pd
import pandera as pa
import warnings
warnings.filterwarnings("ignore")


db = SQl_Server_Connection(dbname='AdventureWorks2019', user='my_user', password='my_pass')

async def get_person(Person_Id:int) -> dict:
    db.connect()
    df_person = db.get_persons(f"""
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
                        WHERE [BusinessEntityID] = {Person_Id}
                        """)
    
    db.close()
    return df_person.to_dict()

async def get_person_page(Page_Id:int) -> dict:
    db.connect()
    df_page = db.get_persons(f"""
                         SELECT  
                            [BusinessEntityID]
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
                        ORDER BY BusinessEntityID
                        OFFSET {Page_Id} ROWS
                        FETCH NEXT 10 ROWS ONLY
                        """)
    
    schema.validate(df_page)

    db.close()
    return df_page.to_dict()
