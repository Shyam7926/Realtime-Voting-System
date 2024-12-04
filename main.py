import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=in'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
BIOGRAPHIES = [
    "A seasoned politician with 20 years of experience in public service.",
    "A young and dynamic leader committed to social justice and equality.",
    "A tech-savvy visionary aiming to modernize governance and infrastructure."
]
CAMPAIGN_PLATFORMS = [
    "Focusing on economic growth and job creation for all.",
    "Prioritizing education reform and healthcare accessibility.",
    "Promoting digital innovation and sustainable development."
]
TELUGU_NAMES = [
    "Aadhya Reddy", "Bhanu Prasad", "Chandra Sekhar", "Divya Bharati", "Eswar Rao",
    "Falguni Devi", "Gopala Krishna", "Harini Devi", "Indira Lakshmi", "Jagadish Kumar",
    "Krishna Veni", "Lavanya Reddy", "Madhavi Latha", "Nagarjuna Rao", "Omkar Babu",
    "Pranavi Chitra", "Rajeshwari Devi", "Sanjay Varma", "Tanuja Kumari", "Uma Mahesh",
    "Venkatesh Goud", "Yamini Priya", "Ananya Krishna", "Bharath Kumar", "Charan Teja",
    "Dharani Reddy", "Eesha Priya", "Feroz Khan", "Ganesh Babu", "Hemanth Kumar",
    "Ishitha Rao", "Jayanth Reddy", "Kalyani Devi", "Lakshmi Prasad", "Manohar Babu",
    "Nithya Rao", "Omkareshwar Reddy", "Padmavathi Devi", "Raghunath Varma", "Sailaja Devi",
    "Tara Kumari", "Uday Bhaskar", "Vasudha Priya", "Yashwanth Kumar", "Aditya Reddy",
    "Bhargavi Latha", "Chaitanya Rao", "Dhanush Kumar", "Eshwar Prasad", "Farzana Banu",
    "Gautham Krishna", "Himaja Reddy", "Ishwar Chandra", "Jayalakshmi Devi", "Kiran Kumar",
    "Lalitha Devi", "Meenakshi Rao", "Nandini Priya", "Omkara Chitra", "Padmanabha Rao",
    "Rakesh Reddy", "Savitri Devi", "Tarun Teja", "Usha Latha", "Varsha Priya",
    "Yogendra Varma", "Anand Rao", "Bala Krishna", "Chinna Babu", "Dinesh Kumar",
    "Eshitha Devi", "Fathima Begum", "Ganapati Babu", "Harinath Rao", "Indu Latha",
    "Jyothi Kumari", "Karthik Krishna", "Lohitha Devi", "Mahesh Babu", "Navya Latha",
    "Om Prakash", "Padmini Devi", "Ravi Varma", "Sanjana Reddy", "Tanmay Rao",
    "Upendra Varma", "Vamsi Krishna", "Yogitha Rao", "Anusha Kumari", "Bhuvanesh Rao",
    "Chandana Devi", "Deeksha Reddy", "Ekambaram Rao", "Fazil Khan", "Girish Babu",
    "Haritha Devi", "Ila Reddy", "Jahnavi Priya", "Kamesh Varma"
]

random.seed(42)

def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": "Indian",
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": random.choice(["Vijayawada", "Guntur", "Tirupati", "Visakhapatnam", "Rajahmundry"]),
                "state": "Andhra Pradesh",
                "country": "India",
                "postcode": str(random.randint(500000, 535000))
            },
            "email": user_data['email'],
            "phone_number": f"+91 {random.randint(7000000000, 9999999999)}",
            "cell_number": f"+91 {random.randint(7000000000, 9999999999)}",
            "picture": user_data['picture']['large'],
            "registered_age": random.randint(18, 80)
        }
    else:
        return "Error fetching data"

def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        party_index = candidate_number % total_parties
        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": TELUGU_NAMES[candidate_number],
            "party_affiliation": PARTIES[party_index],
            "biography": BIOGRAPHIES[party_index],
            "campaign_platform": CAMPAIGN_PLATFORMS[party_index],
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'


def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()

def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()


if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=Shikar@42")
    cur = conn.cursor()

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    create_tables(conn, cur)

    # get candidates from db
    cur.execute("""
        SELECT * FROM candidates
    """)
    candidates = cur.fetchall()
    print(candidates)

    if len(candidates) == 0:
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            print(candidate)
            cur.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
            conn.commit()

    for i in range(1000):
        voter_data = generate_voter_data()
        insert_voters(conn, cur, voter_data)

        producer.produce(
            voters_topic,
            key=voter_data["voter_id"],
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )

        print('Produced voter {}, data: {}'.format(i, voter_data))
        producer.flush()