
from fastapi import FastAPI, HTTPException
import random
import time

from controller.person import get_person, get_person_page
#from src.models.user import User

_app = FastAPI()

@_app.get("/")
async def get_randon_number():
    randon_number = random.randint(1,100)
    time.sleep(1)
    return randon_number

@_app.get("/users/{person_id}")
async def get_user(person_id: int):
    db_Person = await get_person(person_id)
    if db_Person is None:
        raise HTTPException(status_code=404, detail="Person not found")
    return db_Person


@_app.get("/page/{Page_Id}")
async def get_pages(Page_Id: int):
    db_Person = await get_person_page(Page_Id)
    if db_Person is None:
        raise HTTPException(status_code=404, detail="Page not exists")
    return db_Person