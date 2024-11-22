import csv
import random
import faker
import os
from datetime import timedelta , datetime

fake = faker()

def random_date():
      start_date = datetime.now() - timedelta(days=5*365)
      random_days = random.randint(0, 5*365)
      return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
output_dir = "/home/meqlad/pr/charity_data_engineering_project/data"
os.makedirs(output_dir, exist_ok=True)

num_donors = 500
num_donations = 1000
num_campaigns = 100
num_beneficiaries = 200

donors_file_path = os.path.join(output_dir, "donors.csv")
with open(donors_file_path, mode="w", newline="") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["donor_id", "donor_name", "email", "phone", "registration_date"])
    for _ in range(num_donors):
        donor_id = fake.uuid4()
        donor_name = fake.name()
        email = fake.email()
        phone = fake.phone_number()
        registration_date = random_date()
        writer.writerow([donor_id, donor_name, email, phone, registration_date])

donations_file_path = os.path.join(output_dir, "donations.csv")
with open(donations_file_path, mode="w", newline="") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["donation_id", "donor_id", "amount", "donation_date", "campaign_id", "beneficiary_id"])
    for _ in range(num_donations):
        donation_id = fake.uuid4()
        donor_id = fake.uuid4()
        amount = round(random.uniform(10, 5000), 2)
        donation_date = random_date()
        campaign_id = fake.uuid4()
        beneficiary_id = fake.uuid4()
        writer.writerow([donation_id, donor_id, amount, donation_date, campaign_id, beneficiary_id])
