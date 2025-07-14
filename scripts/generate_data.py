import csv
import os
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Fixed lists of values
PROJECT_IDS = [fake.uuid4() for _ in range(5)]
PROJECT_NAMES = [fake.bs().title() for _ in range(5)]
CAMPAIGN_IDS = [fake.uuid4() for _ in range(3)]
CAMPAIGN_TITLES = [fake.catch_phrase() for _ in range(3)]
VOLUNTEER_IDS = [fake.uuid4() for _ in range(30)]
VOLUNTEER_NAMES = [fake.name() for _ in range(30)]
BENEFICIARY_IDS = [fake.uuid4() for _ in range(30)]
BENEFICIARY_NAMES = [fake.name() for _ in range(30)]

def _ensure_dir(filepath):
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

def generate_projects(filepath='projects.csv'):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['project_id', 'project_name', 'description', 'region'])
        for pid, pname in zip(PROJECT_IDS, PROJECT_NAMES):
            writer.writerow([pid, pname, fake.sentence(8), fake.city()])

def generate_campaigns(filepath='campaigns.csv'):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['campaign_id', 'title', 'start_date', 'end_date', 'target_amount'])
        for cid, title in zip(CAMPAIGN_IDS, CAMPAIGN_TITLES):
            start = fake.date_between(start_date='-1y', end_date='today')
            end = start + timedelta(days=random.randint(30, 90))
            writer.writerow([cid, title, start.isoformat(), end.isoformat(), random.randint(10000, 50000)])

def generate_donations(filepath='donations.csv'):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'donation_id', 'donor_id', 'donor_name', 'amount', 'currency', 'region',
            'donation_method', 'project_id', 'campaign_id', 'donation_date'
        ])
        for _ in range(60):
            writer.writerow([
                fake.uuid4(),
                fake.uuid4(),
                fake.name(),
                round(random.uniform(20, 1000), 2),
                fake.currency_code(),
                fake.city(),
                random.choice(['Cash', 'Card', 'Bank Transfer']),
                random.choice(PROJECT_IDS),
                random.choice(CAMPAIGN_IDS),
                fake.date_between('-1y', 'today').isoformat()
            ])

def generate_volunteers(filepath='volunteers.csv'):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['volunteer_id', 'name', 'age', 'join_date', 'project'])
        for vid, vname in zip(VOLUNTEER_IDS, VOLUNTEER_NAMES):
            writer.writerow([
                vid,
                vname,
                random.randint(18, 65),
                fake.date_between('-2y', 'today').isoformat(),
                random.choice(PROJECT_NAMES)
            ])

def generate_volunteer_shifts(filepath='volunteer_shifts.csv'):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['shift_id', 'volunteer_id', 'project_id', 'date', 'start_time', 'end_time'])
        for _ in range(40):
            shift_id = fake.uuid4()
            vid = random.choice(VOLUNTEER_IDS)
            pid = random.choice(PROJECT_IDS)
            date = fake.date_between('-6m', 'today')
            start = fake.time_object()
            end_dt = datetime.combine(datetime.today(), start) + timedelta(hours=random.randint(2, 6))
            end = end_dt.time()
            writer.writerow([shift_id, vid, pid, date.isoformat(), start.strftime('%H:%M:%S'), end.strftime('%H:%M:%S')])

def generate_beneficiaries(filepath='beneficiaries.csv'):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['beneficiary_id', 'name', 'age', 'location', 'project_id', 'aid_type'])
        for bid, bname in zip(BENEFICIARY_IDS, BENEFICIARY_NAMES):
            writer.writerow([
                bid,
                bname,
                random.randint(10, 70),
                fake.city(),
                random.choice(PROJECT_IDS),
                random.choice(['Medical', 'Food', 'Education', 'Housing'])
            ])

def generate_transactions(filepath='transactions.csv'):
    _ensure_dir(filepath)
    # To simplify, generate transactions linked to random donations
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['transaction_id', 'donation_id', 'timestamp', 'payment_provider', 'status'])
        # We create some fake donation IDs on the fly here (not linked to donations.csv)
        for _ in range(60):
            writer.writerow([
                fake.uuid4(),
                fake.uuid4(),
                fake.date_time_this_year().isoformat(),
                random.choice(['Stripe', 'PayPal', 'Bank', 'Cash']),
                random.choice(['Success', 'Failed', 'Refunded'])
            ])

if __name__ == "__main__":
    generate_projects()
    generate_campaigns()
    generate_donations()
    generate_volunteers()
    generate_volunteer_shifts()
    generate_beneficiaries()
    generate_transactions()
