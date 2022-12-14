from base.start import *

app = FastAPI()


@app.get('/')
async def root():
    return {'message': 'Robot-2048 bicycle API'}


@app.get('/test/{n}')
async def test(n: int):
    return {'value': n*n}


@app.get('/files')
async def test():
    return {'files': S3.list_files()}


@app.post('/users/new')
async def new_user(user: User):
    if DB.find_user(user.name):
        return {'status': False, 'message': f'{user.name} already exists'}
    else:
        DB.insert_user(user)
        return {'status': True, 'message': f'{user.name} successfully added'}


if __name__ == '__main__':

    uvicorn.run(app, host="0.0.0.0", port=3000)
