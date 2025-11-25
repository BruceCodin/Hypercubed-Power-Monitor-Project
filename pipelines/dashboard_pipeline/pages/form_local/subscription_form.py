"""Subscription form page for power cut alerts."""

import streamlit as st

st.set_page_config(
    page_title="Subscription Form",
    page_icon="📧"
)

st.title("Subscription Form")
st.write("Please fill out the form below to subscribe to power cut alerts.")

with st.form(key='subscription_form'):
    full_name = st.text_input("Full Name")
    email = st.text_input("Email Address")
    postcode_list = st.text_area(
        "Postcodes (separate multiple postcodes with commas)")

    submit_button = st.form_submit_button(label='Submit')
    if submit_button:
        st.success(
            f"Thank you {full_name}! You have subscribed with the email "
            f"{email} for postcodes: {postcode_list}.")
        print(f"""Form submitted:
              Name: {full_name}
              Email: {email}
              Postcodes: {postcode_list}""")

    st.write("You can edit your details and resubmit the form at any time.")
