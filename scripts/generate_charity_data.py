import csv
import random
from faker import Faker
import os
from datetime import datetime, timedelta

fake = Faker()

def random_date():
    start_date = datetime.now() - timedelta(days=5*365)
    random_days = random.randint(0, 5*365)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

output_dir = "/home/meqlad/pr/charity_data_engineering_project/data"
os.makedirs(output_dir, exist_ok=True)

num_new_donors = 100
num_new_donations = 200
num_new_campaigns = 20
num_new_beneficiaries = 50

# Helper function to append or create a CSV file
def append_or_create_csv(file_path, headers, rows):
    file_exists = os.path.isfile(file_path)
    with open(file_path, mode="a" if file_exists else "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:  # If the file doesn't exist, write headers
            writer.writerow(headers)
        writer.writerows(rows)


donors = []
donor_rows = []
for _ in range(num_new_donors):
    donor_id = fake.uuid4()
    donor_name = fake.name()
    email = fake.email()
    phone = fake.phone_number()
    registration_date = random_date()
    donors.append(donor_id)
    donor_rows.append([donor_id, donor_name, email, phone, registration_date])

donors_file_path = os.path.join(output_dir, "donors.csv")
append_or_create_csv(donors_file_path, ["donor_id", "donor_name", "email", "phone", "registration_date"], donor_rows)

campaigns = []
campaign_rows = []
for _ in range(num_new_campaigns):
    campaign_id = fake.uuid4()
    campaign_name = fake.catch_phrase()
    start_date = random_date()
    end_date = random_date()
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    description = fake.text(max_nb_chars=200)
    campaigns.append(campaign_id)
    campaign_rows.append([campaign_id, campaign_name, start_date, end_date, description])

campaigns_file_path = os.path.join(output_dir, "campaigns.csv")
append_or_create_csv(campaigns_file_path, ["campaign_id", "campaign_name", "start_date", "end_date", "description"], campaign_rows)

beneficiaries = []
beneficiary_rows = []
for _ in range(num_new_beneficiaries):
    beneficiary_id = fake.uuid4()
    beneficiary_name = fake.name()
    age = random.randint(1, 100)
    location = fake.city()
    date_registered = random_date()
    beneficiaries.append(beneficiary_id)
    beneficiary_rows.append([beneficiary_id, beneficiary_name, age, location, date_registered])

beneficiaries_file_path = os.path.join(output_dir, "beneficiaries.csv")
append_or_create_csv(beneficiaries_file_path, ["beneficiary_id", "beneficiary_name", "age", "location", "date_registered"], beneficiary_rows)

donation_rows = []
for _ in range(num_new_donations):
    donation_id = fake.uuid4()
    donor_id = random.choice(donors)
    amount = round(random.uniform(10, 5000), 2)
    donation_date = random_date()
    campaign_id = random.choice(campaigns)
    beneficiary_id = random.choice(beneficiaries)
    donation_rows.append([donation_id, donor_id, amount, donation_date, campaign_id, beneficiary_id])

donations_file_path = os.path.join(output_dir, "donations.csv")
append_or_create_csv(donations_file_path, ["donation_id", "donor_id", "amount", "donation_date", "campaign_id", "beneficiary_id"], donation_rows)

print(f"Data successfully appended or created in {output_dir}")
