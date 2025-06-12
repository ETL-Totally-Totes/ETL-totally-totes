from seed import seed_db

env = ".env"

try:
    seed_db()
except Exception as e:
    print(e)
