from cryptography.fernet import Fernet

def encrypt(data, key):
    f = Fernet(key.encode())
    encoded = data.encode()
    return f.encrypt(encoded)

def decrypt(encrypted, key):
    f = Fernet(key.encode())
    decrypted = f.decrypt(encrypted)
    return decrypted.decode()