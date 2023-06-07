from cryptography.fernet import Fernet

def encrypt(data, key):
    f = Fernet(key)
    encoded = data.encode()
    return f.encrypt(encoded)

def decrypt(encrypted, key):
    f = Fernet(key)
    decrypted = f.decrypt(encrypted)
    return decrypted.decode()