import os
import csv
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)

# ----- Internal caches for foreign keys -----
_project_refs = []
_donor_refs = []
_volunteer_refs = []
_donation_refs = []
_campaign_refs = []

# ----- Utilities -----
def _ensure_dir(filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

def _add_project_ref(pid, pname):
    _project_refs.append({'id': pid, 'name': pname})

def _add_donor_ref(donor_id, name):
    _donor_refs.append({'id': donor_id, 'name': name})

def _add_volunteer_ref(vid):
    _volunteer_refs.append(vid)

def _add_donation_ref(did):
    _donation_refs.append(did)

def _add_campaign_ref(cid):
    _campaign_refs.append(cid)

# ----- 1. Projects -----
def generate_projects(filepath):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['project_id', 'project_name', 'description', 'region'])
        for _ in range(5):
            pid = fake.uuid4()
            pname = fake.bs().title()
            writer.writerow([pid, pname, fake.sentence(8), fake.city()])
            _add_project_ref(pid, pname)

# ----- 2. Campaigns -----
def generate_campaigns(filepath):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['campaign_id', 'title', 'start_date', 'end_date', 'target_amount'])
        for _ in range(3):
            cid = fake.uuid4()
            title = fake.catch_phrase()
            start = fake.date_between(start_date='-1y', end_date='today')
            end = start + timedelta(days=random.randint(30, 90))
            writer.writerow([cid, title, start, end, random.randint(10000, 50000)])
            _add_campaign_ref(cid)

# ----- 3. Donations -----
def generate_donations(filepath):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'donation_id', 'donor_id', 'donor_name', 'amount', 'currency', 'region',
            'donation_method', 'project_id', 'campaign_id', 'donation_date'
        ])
        for _ in range(60):
            did = fake.uuid4()
            donor_id = fake.uuid4()
            dname = fake.name()
            _add_donation_ref(did)
            _add_donor_ref(donor_id, dname)
            writer.writerow([
                did,
                donor_id,
                dname,
                round(random.uniform(20, 1000), 2),
                fake.currency_code(),
                fake.city(),
                random.choice(['Cash', 'Card', 'Bank Transfer']),
                random.choice([p['id'] for p in _project_refs]),
                random.choice(_campaign_refs)['id'],
                fake.date_between('-1y', 'today')
            ])

# ----- 4. Volunteers -----
def generate_volunteers(filepath):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['volunteer_id', 'name', 'age', 'join_date', 'project'])
        for _ in range(30):
            vid = fake.uuid4()
            _add_volunteer_ref(vid)
            writer.writerow([
                vid,
                fake.name(),
                random.randint(18, 65),
                fake.date_between('-2y', 'today'),
                random.choice([p['name'] for p in _project_refs])
            ])

# ----- 5. Volunteer Shifts -----
def generate_volunteer_shifts(filepath):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['shift_id', 'volunteer_id', 'project_id', 'date', 'start_time', 'end_time'])
        for _ in range(40):
            shift_id = fake.uuid4()
            vid = random.choice(_volunteer_refs)
            pid = random.choice(_project_refs)['id']
            date = fake.date_between('-6m', 'today')
            start = fake.time_object()
            end = (datetime.combine(datetime.today(), start) + timedelta(hours=random.randint(2, 6))).time()
            writer.writerow([shift_id, vid, pid, date, start.strftime('%H:%M:%S'), end.strftime('%H:%M:%S')])

# ----- 6. Beneficiaries -----
def generate_beneficiaries(filepath):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['beneficiary_id', 'name', 'age', 'location', 'project_id', 'aid_type'])
        for _ in range(30):
            writer.writerow([
                fake.uuid4(),
                fake.name(),
                random.randint(10, 70),
                fake.city(),
                random.choice([p['id'] for p in _project_refs]),
                random.choice(['Medical', 'Food', 'Education', 'Housing'])
            ])

# ----- 7. Transactions -----
def generate_transactions(filepath):
    _ensure_dir(filepath)
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['transaction_id', 'donation_id', 'timestamp', 'payment_provider', 'status'])
        for did in _donation_refs:
            writer.writerow([
                fake.uuid4(),
                did,
                fake.date_time_this_year(),
                random.choice(['Stripe', 'PayPal', 'Bank', 'Cash']),
                random.choice(['Success', 'Failed', 'Refunded'])
            ])
