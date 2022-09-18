from flask import Flask, jsonify, request
from flask_restful import Resource, Api
import json
import functools
import os
import os.path
import re
from datetime import datetime

script_dir=os.path.dirname(os.path.realpath(__file__))
app = Flask(__name__, instance_path=script_dir)
app.config.from_pyfile('application.cfg')
api = Api(app)

base_path = 'data'
CSV_HEADER="\"unix timestamp\",\"lat\",\"lon\",\"speed\",\"altitude\",\"viewed stat\",\"used sat\",\"accuary\"\n"

def api_required(func):
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        api_key_dict = app.config.get('API_KEYS')
        for device in api_key_dict.keys():
            if api_key == api_key_dict[device]:
                request.api_user = device
                return func(*args, **kwargs)
        return {"message": "API Key invalid"}, 403
    return decorator


def get_last_timestamp(device):
    all_files = []
    for root, dirs, files in os.walk(base_path, device):
        for filename in files:
            match = re.search(r'(\d\d\d\d)-(\d\d)-(\d\d).csv', filename)
            if match:
                all_files.append(os.path.join(root, filename))
    for filename in sorted(all_files, reverse=True):
        tstamp = 0
        f = open(filename, 'r')
        for line in f.readlines():
            linestamp = 0
        try:
            linestamp = int(line.split(',')[0])
        finally:
            pass
        if linestamp > tstamp:
            tstamp = linestamp
        f.close()
        if (tstamp > 0):
            return tstamp
    return 0

def save_line(line, device, last_ts):
  ts_str=line.split(',')[0]
  tstamp=0
  try:
    tstamp = int(ts_str)
  except ValueError:
    tstamp =0
  finally:
    pass
  if tstamp==0:
    print('skipped line %s' % line)
    return
  if (last_ts>=tstamp):
    print('Data is there: %s' % line)
    return
  dt=datetime.utcfromtimestamp(tstamp)
  year=dt.strftime('%Y')
  month=dt.strftime('%m')
  day=dt.strftime('%d')
  basename=dt.strftime('%Y-%m-%d.csv')
  dirname=os.path.join(base_path,device,year,month,day)
  os.makedirs(dirname, exist_ok=True)
  filename=os.path.join(dirname,basename)
  print_header= not(os.path.exists(filename))
  file=open(filename,'a')
  if print_header:
    file.write(CSV_HEADER)
  file.write(line+'\n')
  file.close()

class GetLastUploadedFile(Resource):
    @api_required
    def get(self):
        return(get_last_timestamp(request.api_user))


class UploadFile(Resource):
    @api_required
    def post(self):
        csv_file = request.get_data(as_text=True)
        last_ts=get_last_timestamp(request.api_user)
        for line in csv_file.split("\n"):
            save_line(line, request.api_user, last_ts)
        return "{status=\"OK\"}"


api.add_resource(GetLastUploadedFile, '/last-upload')
api.add_resource(UploadFile, '/f')

if __name__ == '__main__':
    app.run()
