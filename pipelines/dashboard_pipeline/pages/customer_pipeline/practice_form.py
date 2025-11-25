"""
Test script for seeing how Streamlit forms work.
This file is kept for reference only.
"""
import streamlit as st
import requests
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


st.title(":zap: Subscription Form")
st.write("""
Our Power Monitor features two free services to help you stay informed about what you care about most:
- **Power Cut Alerts**: Receive notifications about power cuts in your area. 
- **Daily Summary Emails**: Your personal report on power generation across the country.

To subscribe, please fill out the form below:""")
with st.form(key='subscription_form'):
    first_name = st.text_input("First Name:")
    last_name = st.text_input("Last Name:")
    email = st.text_input("Email Address:")
    postcode = st.text_input("Postcode:")
    submit_button = st.form_submit_button(label='Submit')

    if submit_button:
        details = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "postcode": postcode
        }
        # api_url = 'https://your-api-url.amazonaws.com/your-endpoint'
        # response = requests.post(api_url, json=details)
        response = 200
        if response == 200:
            st.success(
                f"Thank you {first_name}! You have successfully subscribed.")
            st.write("Your subscription details:")
            logger_message = ["Form submitted"]
            for key, value in details.items():
                st.write(f"**{key.replace('_', ' ').title()}**: {value}")
                logger_message.append(
                    f"{key.replace('_', ' ').title()}: {value}")
            logger.info("\n".join(logger_message))
        else:
            st.write(
                f"Oops! Something went wrong. Status code: {response.status_code}")

    st.write("You can resubmit the form at any time to update your subscriptions.")
