import pandas as pd
import pymongo
import numpy as np
import unittest
import string
from dotenv import load_dotenv
import os


class Ingest:

    """Class to ingest a csv file containing musical work information into a
    MongoDB data base.

    Has to be instantiated with the address of the csv file as a
    parameter. Then call the .add() method on the object to add the file
    to the data base.
    """

    def __init__(self, address):

        load_dotenv()
        self.USER_NAME = os.getenv("USER_NAME")
        self.PASSWORD = os.getenv("PASSWORD")
        self.DATA_BASE = os.getenv("DATA_BASE")

        try:

            self.df = pd.read_csv(address)

        except OSError as e:
            
            print(e)

    def infer_id(self):
        """Method to infer the ID SOCIETY value from other rows that share the
        same ISWC value."""
        missing_id = self.df[(self.df["ID SOCIETY"].isnull())
                             & (~self.df["ISWC"].isnull())]

        for index, row in missing_id.iterrows():

            iswc_group = self.df[self.df["ISWC"] == row["ISWC"]]
            values = iswc_group["ID SOCIETY"].unique()

            if len(values) == 2:

                id_society = values[~np.isnan(values)][0]
                self.df["ID SOCIETY"][index] = id_society

    def handle_NaNs(self):
        """Method to deal with NaN-values.

        As the values in the ID Society column serve as the primary key
        it is essential that each row that gets to be ingested into the
        data base has an actual value in this column.Therefore this
        method tries to infer the ID SOCIETY value. If that is not
        possible the row will be dropped. All other NaN-values will be
        replaced by empty strings. This choice seems to be the most user
        friendly solution.
        """
        if self.df["ID SOCIETY"].isnull().sum() > 0:
            self.infer_id()

        self.df.dropna(subset=['ID SOCIETY'], inplace=True)
        self.df = self.df.replace(np.nan, '', regex=True)

    def remove_punctuation(self, iswc_code):

        return iswc_code.translate(str.maketrans('', '', string.punctuation))

    def pad_zeros(self, ipi_number):

        if ipi_number == "":

            return ""

        ipi_number = str(int(ipi_number))

        if len(ipi_number) < 11:

            ipi_number = "0" * (11 - len(ipi_number)) + ipi_number

        return ipi_number

    def clean_df(self):
        """Method to clean the dataset before adding it to the database."""
        # I prefer the df["column name"] way of calling a column as it makes the code more readable.
        # Therefore I do not change the column names to snake casing.
        self.df.rename(
            columns={
                " ALTERNATIVE TITLE 2": "ALTERNATIVE TITLE 2"},
            inplace=True)
        self.handle_NaNs()
        self.df["ISWC"] = self.df["ISWC"].apply(self.remove_punctuation)
        self.df["IPI NUMBER"] = self.df["IPI NUMBER"].apply(self.pad_zeros)

    def generate_groups(self, column):
        """Method to create an iterable of groups of rows that have the same
        value in a column.

        A generator is used to make the method more memory friendly (as
        the test data set is very small the difference to a list is
        probably not noticeable though.)
        """
        for group in self.df.groupby(column):

            yield group

    def create_document(self, group):
        """Method to create the documents to be ingested into the data base."""

        iswc = group[1]["ISWC"].max()

        titles = [
            {"title": group[1]["ORIGINAL TITLE"].max(), "type":"OriginalTitle"}]
        alter_titles = np.unique(group[1][["ALTERNATIVE TITLE 1",
                                           "ALTERNATIVE TITLE 2",
                                           "ALTERNATIVE TITLE 3"]].values)
        alter_titles = np.setdiff1d(alter_titles, [""])
        for alter_title in alter_titles:
            titles.append({"title": alter_title, "type": "AlternativeTitle"})

        right_owners = []
        for index, row in group[1].iterrows():
            right_owners.append(
                {"name": row["RIGHT OWNER"], "role": row["ROLE"], "ipi": row["IPI NUMBER"]})

        document = {"_id": int(group[0]),
                    "iswc": iswc,
                    "titles": titles,
                    "right_owners": right_owners
                    }

        return document

    def add(self):
        """Method to add a csv file to a MongoDB database."""

        client = pymongo.MongoClient(
            f"mongodb+srv://{self.USER_NAME}:{self.PASSWORD}@cluster0.e1xus.mongodb.net/{self.DATA_BASE}?retryWrites=true&w=majority")
        db = client.BMAT
        bmat_test = db.BMAT_TEST

        self.clean_df()

        for group in self.generate_groups("ID SOCIETY"):

            document = self.create_document(group)
            bmat_test.insert_one(document)
