import os
import requests
from requests_toolbelt import MultipartEncoderMonitor

BASE_URL = os.getenv("WEBAPP")
if BASE_URL is None:
    BASE_URL = 'https://webapp-amy.azurewebsites.net/'
    # BASE_URL = 'http://localhost:5000/'
INPUTS = BASE_URL + 'inputs'
SPECS = BASE_URL + 'specifications'
POOLS = BASE_URL + 'pools'
JOBS = BASE_URL + 'jobs'

class Job:

    def set_user(self, token):
        self.params = {'token': token}

    def create_input(self, *input_files):
        if not input_files:
            r = requests.post(INPUTS, params=self.params)
        else:
            files = []
            for file_name in input_files:
                files.append(('file[]', (os.path.basename(file_name), open(file_name, 'rb'))))
            r = requests.post(INPUTS, files=files, params=self.params)
        r.raise_for_status()
        self.inputs = r.text

    def update_input(self, file_name, callback=None):

        m = MultipartEncoderMonitor.from_fields(
            fields={'file[]': (os.path.basename(file_name), open(file_name, 'rb'))},
            callback=callback
            )

        r = requests.put('%s/%s' % (INPUTS, self.inputs), data=m, params=self.params, headers={'Content-Type': m.content_type})
        r.raise_for_status()

    def create_pool(self, size):
        self.pool = Pool.create(size, params=self.params)

    def set_pool(self, pool_id):
        self.pool = Pool(id=pool_id, params=self.params)

    def get_input_info(self):
        r = requests.get('%s/%s' % (INPUTS, self.inputs), params=self.params)
        r.raise_for_status()
        return r.json()

    def create_job_spec(self, spec):
        r = requests.post(SPECS, json=spec, params=self.params)
        r.raise_for_status()
        self.spec = r.text

    def get_job_spec(self):
        r = requests.get('%s/%s' % (SPECS, self.spec), params=self.params)
        r.raise_for_status()
        return r.json()

    def submit(self, size, wall_clock, delete_pool=None):
        data = {
            'wall_clock' : wall_clock,
            'size': size,
            'pool_name' : self.pool.id,
            'job_spec' : self.spec
        }
        if delete_pool:
            data['delete_pool'] = delete_pool
        r = requests.post(JOBS, json=data, params=self.params)
        self.id = r.text
        return self.id

    def get_state(self):
        r = requests.get('%s/%s/state' % (JOBS, self.id), params=self.params)
        r.raise_for_status()
        return r.text

    def is_complete(self):
        r = requests.get('%s/%s/state' % (JOBS, self.id), params=self.params)
        r.raise_for_status()
        return r.text == 'JobState.completed'

    def list_outputs(self):
        r = requests.get('%s/%s/outputs' % (JOBS, self.id), params=self.params)
        r.raise_for_status()
        return r.json()

class Pool:

    def __init__(self, id=None, params=None):
        self.id = id
        self.params = params

    @staticmethod
    def create(size, params=None):
        r = requests.post(POOLS, json={'size': size}, params=params)
        r.raise_for_status()
        pool = Pool(r.text, params)
        return pool

    def get_info(self):
        r = requests.get('%s/%s' % (POOLS, self.id), params=self.params)
        r.raise_for_status
        return r.json()

    def is_ready(self):
        return self.get_info()['is_ready']

    def delete(self):
        r = requests.delete('%s/%s' % (POOLS, self.id), params=self.params)
        r.raise_for_status
