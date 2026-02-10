from requests import get
response=get('https://images-assets.nasa.gov/image/ACD26-0007-049/ACD26-0007-049~small.jpg')

print(response)
print(response.status_code)
print(response.headers.get("Content-Type"))

with open('test.jpg', 'wb') as file:
    file.write(response.content)