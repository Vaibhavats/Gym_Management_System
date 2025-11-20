import mysql.connector
import pandas as pd
import uuid
from datetime import date, timedelta

# ------------------- MySQL Connection -------------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root@123",
        database="gym_db"
    )

# ------------------- INSERT MEMBER -------------------
def insert_member(name, age, gender, contact, membership_type, trainer_id):
    conn = get_connection()
    cursor = conn.cursor()

    # Ensure gender format
    gender_char = gender[0].upper() if gender else None  

    # Fix trainer ID → convert '' or None to NULL
    trainer_id = int(trainer_id) if trainer_id not in ("", None) else None

    # Fetch membership validity
    cursor.execute(
        "SELECT validity_months FROM Membership_Types WHERE membership_type=%s",
        (membership_type,)
    )
    validity = cursor.fetchone()

    if not validity:
        raise ValueError(f"Membership type '{membership_type}' not found!")

    months = int(validity[0])

    start_date = date.today()
    end_date = start_date + timedelta(days=30 * months)

    # Insert member
    cursor.execute("""
        INSERT INTO Members (name, age, gender, contact, membership_type, start_date, end_date, trainer_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (name, age, gender_char, contact, membership_type, start_date, end_date, trainer_id))

    member_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return member_id

# ------------------- INSERT PAYMENT -------------------
def insert_payment(member_id, amount, mode, status='Paid'):
    conn = get_connection()
    cursor = conn.cursor()

    payment_id = "P" + uuid.uuid4().hex[:6].upper()
    payment_date = date.today()

    cursor.execute("""
        INSERT INTO Payments (payment_id, member_id, amount, payment_date, mode, status)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (payment_id, member_id, amount, payment_date, mode, status))

    conn.commit()
    conn.close()
    return payment_id


# ---------------------------------------------------------------
# 🟢 NEW IMPROVED FUNCTION → FETCH MEMBERS (All + Filter Both)
# ---------------------------------------------------------------
def fetch_members(membership_type="All"):
    conn = get_connection()

    base_query = """
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
                    ROW_NUMBER() OVER (PARTITION BY member_id 
                                       ORDER BY payment_date DESC, payment_id DESC) AS rn
                FROM Payments
            ) sub
            WHERE rn = 1
        ) p ON m.member_id = p.member_id
    """

    if membership_type == "All":
        df = pd.read_sql(base_query, conn)
    else:
        df = pd.read_sql(base_query + " WHERE m.membership_type = %s", conn, params=(membership_type,))

    conn.close()
    return df



# ------------------- FETCH MEMBERSHIP TYPES -------------------
def fetch_membership_types():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Membership_Types", conn)
    conn.close()
    return df

# ------------------- FETCH TRAINERS -------------------
def fetch_trainers():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Trainers", conn)
    conn.close()
    return df

# ------------------- DELETE MEMBER -------------------
def delete_member(member_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        conn.start_transaction()

        cursor.execute("DELETE FROM Payments WHERE member_id=%s", (member_id,))
        cursor.execute("DELETE FROM Members WHERE member_id=%s", (member_id,))

        if cursor.rowcount == 0:
            conn.rollback()
            conn.close()
            return False

        conn.commit()
        conn.close()
        return True

    except:
        try:
            conn.rollback()
        except:
            pass
        return False

# ------------------- RENEW MEMBERSHIP -------------------
def renew_membership(member_id, membership_type, amount, mode, status='Paid', duration_months=3):
    conn = get_connection()
    cursor = conn.cursor()

    payment_id = "P" + uuid.uuid4().hex[:6].upper()
    payment_date = date.today()

    cursor.execute("""
        INSERT INTO Payments (payment_id, member_id, amount, payment_date, mode, status)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (payment_id, member_id, amount, payment_date, mode, status))
    conn.commit()

    cursor.execute("SELECT end_date FROM Members WHERE member_id=%s", (member_id,))
    result = cursor.fetchone()

    if result and result[0]:
        last_end = result[0]
        start_date = max(last_end + timedelta(days=1), date.today())
    else:
        start_date = date.today()

    end_date = start_date + timedelta(days=30 * duration_months)

    cursor.execute("""
        UPDATE Members
        SET membership_type=%s, start_date=%s, end_date=%s
        WHERE member_id=%s
    """, (membership_type, start_date, end_date, member_id))

    conn.commit()

    cursor.execute("""
        SELECT amount, payment_date
        FROM Payments
        WHERE member_id=%s
        ORDER BY payment_date DESC
        LIMIT 1
    """, (member_id,))
    last_payment = cursor.fetchone()

    conn.close()

    return payment_id, start_date, end_date, last_payment[0] if last_payment else None, last_payment[1] if last_payment else None

# db.py - Add these two functions near your other fetch functions

# ---------------------------------------------------------------
# 👤 NEW CLIENT FUNCTION → FETCH SINGLE MEMBER DETAILS
# ---------------------------------------------------------------
def fetch_member_details(member_id):
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
            t.specialization AS Trainer_Specialization
        FROM Members m
        LEFT JOIN Trainers t ON m.trainer_id = t.trainer_id
        WHERE m.member_id = %s
    """
    df = pd.read_sql(query, conn, params=(member_id,))
    conn.close()
    return df

# ---------------------------------------------------------------
# 👤 NEW CLIENT FUNCTION → FETCH MEMBER PAYMENT HISTORY
# ---------------------------------------------------------------
def fetch_member_payments(member_id):
    conn = get_connection()
    query = """
        SELECT
            payment_id,
            amount,
            payment_date,
            mode,
            status
        FROM Payments
        WHERE member_id = %s
        ORDER BY payment_date DESC
    """
    df = pd.read_sql(query, conn, params=(member_id,))
    conn.close()
    return df