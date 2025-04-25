import os,sys,csv
import json
from datetime import datetime
import random
from faker import Faker
from pydantic import BaseModel, Field

data_faker = Faker()


users_file = "/mnt/d/jegan/prct/data/mock_data/users.csv"
transaction_file = "/mnt/d/jegan/prct/data/mock_data/transactions.csv"

# num_records = 1000
class UsersData(BaseModel):
    user_id: int = Field(default_factory=lambda: random.randint(1000,10000))
    user_name:str = Field(default_factory=data_faker.name)
    user_age : int = Field(default_factory= lambda: random.randint(24,89))
    user_email:str = Field(default_factory=data_faker.email)
    user_city:str = Field(default_factory=data_faker.city)
    created_time : datetime = datetime.now().isoformat()


class TrasnactionData(BaseModel):
    transaction_id: int = Field(default_factory=lambda: random.randint(1000,999999))    
    user_id: int = Field(default_factory=lambda: random.randint(1000,999999))
    amount: float = Field(round(data_faker.random_number(digits=2), 2))
    transaction_type: str = Field(data_faker.random_element(elements=("credit", "debit")))
    date: datetime = Field(data_faker.date_this_year())
    status: str = Field(data_faker.random_element(elements=("completed", "pending", "failed")))

def save_csv_file(input_data, filename: str):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(input_data[0].keys())
        # Write transaction rows
        for row_data in input_data:
            writer.writerow(row_data.values())

# Generate mock data 
def create_user_data(num_records):
    users_data = [UsersData().model_dump() for _ in range(num_records) ]
    return users_data
# print(users_data)

# Generate mock data 
def create_transaction_data(num_records):
    users_data = [TrasnactionData().model_dump() for _ in range(num_records) ]
    return users_data

# print(create_transaction_data(1))

if __name__ == "__main__":
    # Create users data
    users = create_user_data(1000)
    save_csv_file(users,users_file)
    
    transactions = create_transaction_data(1000000)
    save_csv_file(transactions,transaction_file)
    




