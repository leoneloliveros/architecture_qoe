import requests
from requests.auth import HTTPBasicAuth
from data_json_one_ot import register, register_qc, register_close


url = "http://10.60.65.172:5000/itsm_qoe"

username = "user_qoe"
password = "Claro2025;"
response = requests.post(
    url,
    json=register,
    auth=HTTPBasicAuth(username, password)
)
if response.status_code == 201:
    print("Request was successful.")
    print("Response:", response.json())
else:
    print("Failed to process request.")
    print("Status code:", response.status_code)
    print("Response:", response.text)



# url = "http://10.60.65.172:5000/itsm_qoe_qc"
# username = "user_qoe"
# password = "Claro2025;"
# response = requests.post(
#     url,
#     json=register_qc,
#     auth=HTTPBasicAuth(username, password)
# )
# if response.status_code == 200:
#     print("Request was successful.")
#     print("Response:", response.json())
# else:
#     print("Failed to process request.")
#     print("Status code:", response.status_code)
#     print("Response:", response.text)


# url = "http://10.60.65.172:5000/itsm_close"
# username = "user_qoe"
# password = "Claro2025;"
# response = requests.post(
#     url,
#     json=register_close,
#     auth=HTTPBasicAuth(username, password)
# )
# if response.status_code == 200:
#     print("Request was successful.")
#     print("Response:", response.json())
# else:
#     print("Failed to process request.")
#     print("Status code:", response.status_code)
#     print("Response:", response.text)


# url = "http://10.60.65.172:5000/test"

# username = "user_qoe"
# password = "Claro2025;"
# response = requests.post(
#     url,
#     json={"test":"test"},
#     auth=HTTPBasicAuth(username, password)
# )
# if response.status_code == 201:
#     print("Request was successful.")
#     print("Response:", response.json())
# else:
#     print("Failed to process request.")
#     print("Status code:", response.status_code)
#     print("Response:", response.text)
