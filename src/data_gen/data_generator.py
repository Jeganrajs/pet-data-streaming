import os,sys,csv
import json
from datetime import datetime
import random
from faker import Faker
from faker.providers import BaseProvider
from pydantic import BaseModel, Field

fake = Faker()


users_file = "/mnt/d/jegan/prct/data/mock_data/users.csv"
transaction_file = "/mnt/d/jegan/prct/data/mock_data/transactions.csv"

# Custom provider to generate sequnce number
class  SequenceNoProvider(BaseProvider):
    def __init__(self, generator):
        super().__init__(generator)
        self.current = 999
    
    def seq_no_generator(self):
        self.current +=1
        return self.current

fake.add_provider(SequenceNoProvider)
    

# num_records = 1000
class UsersData(BaseModel):
    # created_time : datetime = datetime.now().isoformat()
    # user_id: int = Field(default_factory=lambda: random.randint(999,1001000))
    user_id: int = Field(default_factory=fake.seq_no_generator)
    first_name:str = Field(default_factory= fake.first_name)
    last_name:str = Field(default_factory= fake.last_name)
    email:str = Field(default_factory= fake.email)
    # address:str = Field(default_factory= fake.address)
    city:str = Field(default_factory=fake.city)
    postal_code:str = Field(default_factory=fake.postcode)
    phone_number:str = Field(default_factory= fake.phone_number)    
    age : int = Field(default_factory= lambda: random.randint(24,89))
    gender:str = Field(default_factory=lambda: random.choice(['Male', 'Female', 'Non-Binary']))
    location:str = Field(default_factory= fake.city)
    status:str = Field(default_factory=lambda: random.choice(['Active', 'Inactive', 'Suspended']))
    ip_address:str = Field(default_factory= fake.ipv4)
    last_login:datetime = Field(default_factory= fake.date_this_year)
    loyalty_level:str = Field( default_factory=lambda: random.choice(['bronze', 'silver', 'gold', 'platinum', 'new']))
    hash_id:str =  Field(default_factory= fake.uuid4)


class TrasnactionData(BaseModel):
    transaction_id: int = Field(default_factory=lambda: random.randint(1000,999999))
    user_id: int = Field(default_factory=lambda: random.randint(1000,4999))
    amount: float = Field(round(fake.random_number(digits=2), 2))
    transaction_type: str = Field(fake.random_element(elements=("credit", "debit")))
    date: datetime = Field(fake.date_this_year())
    location:str = Field(default_factory= fake.city)
    status: str = Field(fake.random_element(elements=("completed", "pending", "failed")))
    ip_address:str = Field(default_factory= fake.ipv4)
    # last_login:str = Field(default_factory= fake.date_this_year)
    hash_id:str =  Field(default_factory= fake.uuid4)

def save_csv_file(input_data, filename: str):
    print("started saving csv file..!")
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(input_data[0].keys())
        # Write transaction rows
        for row_data in input_data:
            writer.writerow(row_data.values())
    print(f"CSV file {filename} saved successfully")

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
    users = create_user_data(10)
    save_csv_file(users,users_file)
    
    # transactions = create_transaction_data(1000000)
    # save_csv_file(transactions,transaction_file)