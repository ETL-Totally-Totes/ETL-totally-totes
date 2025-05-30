from seed import seed_db

env = '.env.dev'

try:
    seed_db(env)
except Exception as e:
    print(e)
    raise e
