import streamlit as st
import pandas as pd
# Import the necessary functions from db.py
from db import get_connection, fetch_member_details, fetch_member_payments

st.set_page_config(page_title="Member Portal", layout="wide")


st.title("👤 Gym Member Self-Service Portal")

# ----------------- Session State Initialization -----------------
if "logged_in_member_id" not in st.session_state:
    st.session_state.logged_in_member_id = None
    st.session_state.logged_in_member_name = None

# --- Function to handle Login ---
def handle_login(member_id_input, contact_input):
    try:
        member_id = int(member_id_input)
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Check for ID and Contact match
        cursor.execute(
            "SELECT member_id, name FROM Members WHERE member_id=%s AND contact=%s",
            (member_id, contact_input)
        )
        result = cursor.fetchone()
        conn.close()

        if result:
            st.session_state.logged_in_member_id = result[0]
            st.session_state.logged_in_member_name = result[1]
            st.success(f"Welcome, {result[1]}!")
            # Corrected: Using st.rerun()
            st.rerun() 
        else:
            st.error("Invalid Member ID or Contact Number.")
            st.session_state.logged_in_member_id = None
    except ValueError:
        st.error("Please enter a valid numeric Member ID.")
    except Exception as e:
        st.error(f"An error occurred during login: {e}")


# ----------------- Display Login Form or Dashboard -----------------

if st.session_state.logged_in_member_id is None:
    # --- Login Form ---
    st.subheader("Login to View Your Details")
    with st.form("client_login_form"):
        member_id_input = st.text_input("Enter Member ID")
        # Use type="password" to hide the contact number input
        contact_input = st.text_input("Enter Contact Number", type="password") 
        login_button = st.form_submit_button("Login")

    if login_button:
        handle_login(member_id_input, contact_input)

else:
    # --- Member Dashboard View ---
    member_id = st.session_state.logged_in_member_id
    member_name = st.session_state.logged_in_member_name

    st.header(f"👋 Welcome, **{member_name}**!")

    if st.button("Logout", help="Click to log out of your portal"):
        st.session_state.logged_in_member_id = None
        st.session_state.logged_in_member_name = None
        st.rerun() # Corrected rerun

    st.markdown("---")

    # 1. Member Profile & Membership Status
    st.subheader("📝 Profile & Membership Status")
    member_details_df = fetch_member_details(member_id)

    if not member_details_df.empty:
        details = member_details_df.iloc[0]

        # Display key status metrics
        col1, col2, col3 = st.columns(3)
        
        # This is safe because it's already a string
        col1.metric("Membership Type", details['membership_type'])
        
        # FIX: Convert datetime.date to string to prevent TypeError
        col2.metric("Start Date (Joined)", str(details['start_date'])) 
        col3.metric("Membership Ends", str(details['end_date'])) 
        
        st.write(f"**Trainer:** {details['Trainer_Name']} (Specialization: {details['Trainer_Specialization']})")
        
        # Display core personal details in a concise table
        personal_info = {
            "Member ID": details['member_id'],
            "Age": details['age'],
            "Gender": details['gender'],
            "Contact": details['contact']
        }
        st.table(pd.DataFrame(personal_info, index=["Details"]).T)


    st.markdown("---")

    # 2. Payment History
    st.subheader("💳 Full Payment History")
    payments_df = fetch_member_payments(member_id)
    if not payments_df.empty:
        # Format columns for better client readability
        payments_df = payments_df.rename(columns={
            'payment_id': 'Payment ID',
            'amount': 'Amount (₹)',
            'payment_date': 'Date',
            'mode': 'Mode',
            'status': 'Status'
        })
        st.dataframe(payments_df)
    else:
        st.info("No payment history found.")