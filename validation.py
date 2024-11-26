import re
from sys import exception
import pandas as pd
import reduce_file

regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
def validation_data(record):
    if not isinstance(record, dict):
        "Invalid format"

    required_fields = [user_id, email, timestamp, items]

    for field in required_fields:
        if field not in record:
            return False, f"Missing '{field}' in record"

    if not isinstance(record[user_id],str):
        return False, "Must be a non-empty string"

    if not isinstance(record[email], re.fullmatch(email,regex)):
        return False, "Must be in a valid email format."

    if not isinstance(record[timestamp],):
        return False, "Must be in a valid email format."

    item_record = [item_id,quantity,price]

    for item in field[items]:
        if field not in item_record:
            return False, f"Can not be non empty dictionary"

    if not isinstance(item[item_id], str):
        return False, "Need to be non-empty string"

    if not isinstance(item[quantity], int):
        if (item[quantity] > 0):
            return False, "Need to be positive integer value"

    if not isinstance(item[price], float):
        if (item[price] > 0):
            return False, "Need to be positive float value"


    return True, message




data = [
    {
      "user_id": "abc123",
      "email": "user1@example.com",
      "timestamp": "2024-09-03T12:30:00Z",
      "items": [
        {"item_id": "item001", "quantity": 3, "price": 9.99},
        {"item_id": "item002", "quantity": 1, "price": 19.99}
      ]
    },
    {
      "user_id": "xyz789",
      "email": "user2@invalid-email",  # Invalid email
      "timestamp": "2024-09-03T15:45:00Z",
      "items": []
    },
    {
      "user_id": "",
      "email": "user3@example.com",
      "timestamp": "invalid-timestamp", # Invalid timestamp
      "items": [
        {"item_id": "item003", "quantity": 2, "price": -5.99} # Invalid price
      ]
    }
]

for field in data:
    is_valid, message = validation_data(field)

    try:
           if is_valid:
                token = token_implementaion.generate_token()

                reduce_item = reduce_file.ReducearraySize(field.items)

                pd.DataFrame(output).to_csv("Output.csv", index=False, header=["Word", "Frequency"])

                return message
    catch(System.Web.HttpException exception):
        return message
