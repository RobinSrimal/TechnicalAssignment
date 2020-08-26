import pymongo
from dotenv import load_dotenv
import os
import string


class Query:

    """Class to query right owner information when provided with an ISWC code.

    To use instantiate the class and use the .find_right_owners() method
    with the ISWC code as parameter in string format.
    """
    def __init__(self):

        load_dotenv()
        self.USER_NAME = os.getenv("USER_NAME")
        self.PASSWORD = os.getenv("PASSWORD")
        self.DATA_BASE = os.getenv("DATA_BASE")

    def find_right_owners(self, iswc_code):

        iswc_code = iswc_code.translate(
            str.maketrans('', '', string.punctuation))

        client = pymongo.MongoClient(
            f"mongodb+srv://{self.USER_NAME}:{self.PASSWORD}@cluster0.e1xus.mongodb.net/{self.DATA_BASE}?retryWrites=true&w=majority")
        db = client.BMAT
        bmat_test = db.BMAT_TEST
        right_owners = bmat_test.find_one({"iswc": iswc_code})["right_owners"]
        return {"right_owners": right_owners}
