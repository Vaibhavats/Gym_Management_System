import mysql.connector 
import pandas as pd 
import uuid # For generating unique payment IDs
from datetime import date, timedelta # For date manipulations

# ------------------- MySQL Connection -------------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root@123",
        database="gym_db"
    )

# ------------------- Insert Member -------------------
def insert_member(name, age, gender, contact, membership_type, trainer_id):
    conn = get_connection()
    cursor = conn.cursor()

    # Convert gender to single character
    gender_char = gender[0].upper() if gender else None  # 'M', 'F', 'O'

    # Get membership duration
    cursor.execute( 
        "SELECT validity_months FROM Membership_Types WHERE membership_type=%s",
        (membership_type,)
    )
    validity = cursor.fetchone()
    months = int(validity[0]) if validity else 0

    start_date = date.today()
    end_date = start_date + timedelta(days=30*months)

    cursor.execute("""
        INSERT INTO Members (name, age, gender, contact, membership_type, start_date, end_date, trainer_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (name, int(age), gender_char, int(contact), membership_type, start_date, end_date, int(trainer_id)))

    member_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return member_id

# ------------------- Insert Payment -------------------


def insert_payment(member_id, amount, mode, status='Paid'):
    conn = get_connection()
    cursor = conn.cursor()

    # Generate unique payment_id using UUID (safe, avoids duplicates)
    payment_id = "P" + str(uuid.uuid4().hex[:6]).upper()  # e.g., P1A2B3

    payment_date = date.today()

    cursor.execute("""
        INSERT INTO Payments (payment_id, member_id, amount, payment_date, mode, status)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (payment_id, int(member_id), float(amount), payment_date, mode, status))

    conn.commit()
    conn.close()
    return payment_id

# ------------------- Fetch Members -------------------
def fetch_members():
    conn = get_connection()
    query = """
        SELECT 
            m.member_id,
            m.name AS Member_Name,
            m.age,
            m.gender,
            m.contact,
            m.membership_type,
            m.start_date,
            m.end_date,
            t.name AS Trainer_Name,
            t.specialization AS Trainer_Specialization,
            p.amount AS Payment_Amount,
            p.status AS Payment_Status,
            p.mode AS Payment_Mode
        FROM Members m
        LEFT JOIN Trainers t ON m.trainer_id = t.trainer_id
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY payment_date DESC, payment_id DESC) AS rn
                FROM Payments
            ) sub
            WHERE rn = 1
        ) p ON m.member_id = p.member_id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# ------------------- Fetch Membership Types -------------------
def fetch_membership_types():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Membership_Types", conn)
    conn.close()
    return df

# ------------------- Fetch Trainers -------------------
def fetch_trainers():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Trainers", conn)
    conn.close()
    return df

# ------------------- Delete Member -------------------
def delete_member(member_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        member_id = int(member_id)

        conn.start_transaction()

        # Delete payments first
        cursor.execute("DELETE FROM Payments WHERE member_id=%s", (member_id,))
        print(f"Deleted {cursor.rowcount} payment(s) for member {member_id}")

        # Delete member
        cursor.execute("DELETE FROM Members WHERE member_id=%s", (member_id,))
        if cursor.rowcount == 0:
            conn.rollback()
            conn.close()
            print(f"Member {member_id} not found")
            return False

        conn.commit()
        conn.close()
        print(f"Member {member_id} deleted successfully")
        return True

    except Exception as e:
        print(f"Error deleting member {member_id}: {e}")
        try:
            conn.rollback()
        except:
            pass
        return False


def renew_membership(member_id, membership_type, amount, mode, status='Paid', duration_months=3):
    conn = get_connection()
    cursor = conn.cursor()

    # 1️⃣ Generate unique payment_id
    payment_id = "P" + str(uuid.uuid4().hex[:6]).upper()
    payment_date = date.today()

    # 2️⃣ Insert payment
    cursor.execute("""
        INSERT INTO Payments (payment_id, member_id, amount, payment_date, mode, status)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (payment_id, int(member_id), float(amount), payment_date, mode, status))
    conn.commit()  # ✅ commit immediately

    # 3️⃣ Update Members table dates
    cursor.execute("SELECT end_date FROM Members WHERE member_id=%s", (member_id,))
    result = cursor.fetchone()
    if result and result[0]:
        last_end_date = result[0]
        start_date = max(last_end_date + timedelta(days=1), date.today())
    else:
        start_date = date.today()

    end_date = start_date + timedelta(days=int(duration_months)*30)
    cursor.execute("""
        UPDATE Members
        SET membership_type=%s, start_date=%s, end_date=%s
        WHERE member_id=%s
    """, (membership_type, start_date, end_date, member_id))
    conn.commit()

    # 4️⃣ Fetch latest payment for this member (after commit)
    cursor.execute("""
        SELECT amount, payment_date
        FROM Payments
        WHERE member_id=%s
        ORDER BY payment_date DESC
        LIMIT 1
    """, (member_id,))
    last_payment = cursor.fetchone()

    last_amount = last_payment[0] if last_payment else None
    last_payment_date = last_payment[1] if last_payment else None

    conn.close()
    return payment_id, start_date, end_date, last_amount, last_payment_date
