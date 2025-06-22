import os, requests, streamlit as st
from common import SettingsInstance as S

API_URL = f"http://recommendation_api:{S.api_port}/recommend"

st.title("📚 Elementary School Book Recommender")
student_id = st.text_input("Student ID", "")
query = st.text_input("Topic / Keyword", "adventure")
n = st.slider("How many books?", 1, 5, 3)

if st.button("Get recommendations") and student_id:
    with st.spinner("Calling API…"):
        r = requests.post(API_URL, params={"student_id": student_id, "n": n, "query": query}, timeout=30)
    if r.ok:
        for item in r.json():
            st.subheader(item["title"])
            st.write(item["librarian_blurb"])
    else:
        st.error(f"Error {r.status_code}: {r.text}") 