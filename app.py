from fastapi import FastAPI
from query import Query

application = app = FastAPI()

@app.get("/{iswc_code}")
async def get_right_owners(iswc_code):

    query = Query()
    right_owners = query.find_right_owners(iswc_code)
    return right_owners
