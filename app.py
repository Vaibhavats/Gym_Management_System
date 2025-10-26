import streamlit as st # For interactive UI (if needed)
import pandas as pd
from db import insert_member, insert_payment, fetch_members, fetch_membership_types, fetch_trainers, get_connection, delete_member, renew_membership # DB functions


st.set_page_config(page_title="Gym Management System", layout="wide")
st.title("🏋️ Gym Management System")

menu = ["Register New Member", "View Members", "Delete Member","Renew Membership"]
choice = st.sidebar.selectbox("Menu", menu)

# ------------------- Fetch Membership Plans and Trainers -------------------
membership_df = fetch_membership_types()
membership_plans = membership_df['membership_type'].tolist()

trainers_df = fetch_trainers()
trainers_display = [f"{row['name']} ({row['specialization']})" for _, row in trainers_df.iterrows()]

# ----------------- Register Member -----------------
if choice == "Register New Member":
    st.subheader("📝 Register New Member")

    # Membership selection OUTSIDE the form
    selected_plan_name = st.selectbox("Select Membership Plan", membership_plans)
    selected_plan = membership_df[membership_df['membership_type'] == selected_plan_name].iloc[0]

    st.write(f"💰 Price: ₹{selected_plan['price']}")
    st.write(f"⏳ Duration: {selected_plan['validity_months']} months")

    with st.form("register_form"):
        # Member details
        name = st.text_input("Full Name")
        age = st.number_input("Age", min_value=10, max_value=100)
        gender = st.selectbox("Gender", ["M", "F", "O"])
        contact = st.text_input("Contact Number")

        # Trainer selection
        selected_trainer_display = st.selectbox("Trainer", trainers_display)
        trainer_id = trainers_df.iloc[trainers_display.index(selected_trainer_display)]['trainer_id']

        # Payment details
        payment_mode = st.selectbox("Payment Mode", ["Cash", "Card", "UPI"])
        payment_status = st.selectbox("Payment Status", ["Paid", "Unpaid"])

        submit = st.form_submit_button("Register")

    if submit:
        if name and contact:
            # Check for duplicate member in DB
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT member_id FROM Members WHERE name=%s AND contact=%s",
                (name, contact)
            )
            existing = cursor.fetchone()
            conn.close()

            if existing:
                st.warning("This member is already registered!")
            else:
                # Insert member
                member_id = insert_member(
                    name=name,
                    age=age,
                    gender=gender,
                    contact=contact,
                    membership_type=selected_plan_name,
                    trainer_id=trainer_id
                )
                # Insert payment
                insert_payment(
                    member_id=member_id,
                    amount=selected_plan['price'],
                    mode=payment_mode,
                    status=payment_status
                )
                st.success(f"✅ {name} registered successfully!")
        else:
            st.error("Please enter both Name and Contact Number.")

# ----------------- View Members -----------------
elif choice == "View Members":
    st.subheader("👥 Registered Members")
    # Fetch fresh members data from session or DB
    members_df = st.session_state.get('members_df', fetch_members())

    if not members_df.empty:
        st.dataframe(members_df)
    else:
        st.info("No members found yet.")


# ----------------- Delete Member -----------------
elif choice == "Delete Member":
    st.subheader("🗑️ Delete a Member")

    search_input = st.text_input("Enter Member ID or Name")

    if search_input:
        members_df = fetch_members()
        filtered = members_df[
            (members_df['member_id'].astype(str) == search_input) |
            (members_df['Member_Name'].str.lower() == search_input.lower())
        ]

        if not filtered.empty:
            member_info = filtered.iloc[0]
            st.write("**Member Details:**")
            st.write(f"**ID:** {member_info['member_id']}")
            st.write(f"**Name:** {member_info['Member_Name']}")

            if "confirm_delete" not in st.session_state:
                st.session_state.confirm_delete = False

            if not st.session_state.confirm_delete and st.button("Delete Member"):
                st.session_state.confirm_delete = True

            if st.session_state.confirm_delete:
                st.warning("⚠️ Are you sure you want to delete this member?")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Yes, Delete"):
                        success = delete_member(int(member_info['member_id']))
                        if success:
                            st.success(f"✅ Member {member_info['Member_Name']} deleted successfully!")
                        else:
                            st.error("❌ Error: Could not delete member.")
                        st.session_state.confirm_delete = False
                with col2:
                    if st.button("Cancel"):
                        st.session_state.confirm_delete = False
        else:
            st.info("No member found with this ID or Name.")
    # ------------------- Renew Membership -------------------
elif choice == "Renew Membership":
    st.header("♻️ Renew Membership")

    # Fetch existing members
    members = fetch_members()
    if members.empty:
        st.warning("No members found. Please register a member first.")
    else:
        # Display members in "ID - Name" format
        member_names = [f"{row['member_id']} - {row['Member_Name']}" for _, row in members.iterrows()]
        selected_member = st.selectbox("Select Member", member_names)
        member_id = int(selected_member.split(" - ")[0])

        # Dynamic membership plans
        membership_df = fetch_membership_types()
        membership_plans = membership_df['membership_type'].tolist()
        selected_plan_name = st.selectbox("Select Membership Plan", membership_plans)

        selected_plan = membership_df[membership_df['membership_type'] == selected_plan_name].iloc[0]
        price = selected_plan['price']
        duration_months = selected_plan['validity_months']

        st.write(f"💰 Price: ₹{price}")
        st.write(f"⏳ Duration: {duration_months} months")

        # Payment status and mode
        payment_status = st.selectbox("Payment Status", ["Paid", "Unpaid"])
        if payment_status == "Paid":
            payment_mode = st.selectbox("Payment Mode", ["Cash", "UPI", "Card"])
        else:
            payment_mode = None

        # Renew membership button
        if st.button("Renew Membership"):
            payment_id, new_start, new_end, last_amount, last_payment_date = renew_membership(
                member_id, selected_plan_name, price, payment_mode, payment_status, duration_months
            )

            st.success("Membership renewed successfully! ✅")
            st.write(f"Payment ID: {payment_id}")
            st.write(f"Status: {payment_status}")
            st.write(f"New Start Date: {new_start}")
            st.write(f"New End Date: {new_end}")
            st.write(f"Last Paid Amount: ₹{last_amount}")
            st.write(f"Last Payment Date: {last_payment_date}")
