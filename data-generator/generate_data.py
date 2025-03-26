from faker import Faker
import pandas as pd
import random
from datetime import datetime
import os
import stat

fake = Faker(['en_US','de_DE'])

output_dir = "/app/data"
os.makedirs(output_dir, exist_ok=True)


def generate_customer_data(n):
    data = []
    for _ in range(n):
        current_hour = datetime.now().hour
        profession = random.choice(["Engineer", "Teacher", "Designer", "Doctor", "Developer"])
        if 8 <= current_hour <= 12:
            profession = random.choice(["Barista", "Student", "Office Worker"])
        elif 18 <= current_hour <= 20:
            profession = random.choice(["Artist", "Musician", "Chef"])

        gender = random.choice(["M", "F"])
        name_method = fake.first_name_male if gender == 'M' else fake.first_name_female
        last_name_method = fake.last_name_male if gender == 'M' else fake.last_name_female

        customer = {
            "first_name": name_method(),
            "last_name": last_name_method(),
            "age": random.randint(18, 70),
            "gender": gender,
            "address": fake.address(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "profession": profession,
            "manager": fake.name(),
            "created_at": datetime.now().isoformat()
        }
        data.append(customer)

    return data


if __name__ == "__main__":
    num_records = random.randint(20, 50)
    customers = generate_customer_data(num_records)
    df = pd.DataFrame(customers)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{output_dir}/data_{timestamp}.csv"
    df.to_csv(file_path, index=False)

    os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)

    print(f'file {output_dir}/data_{timestamp}.csv was created!')