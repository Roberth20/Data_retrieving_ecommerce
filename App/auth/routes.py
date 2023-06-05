from App.auth import auth
import DATA

@auth.get("/")
def main_auth():
    return "<h1>Main page for authentication. Links to auth and refresh</h1>"
