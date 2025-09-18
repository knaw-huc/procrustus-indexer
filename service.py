# -*- coding: utf-8 -*-
from flask import Flask,render_template,request, jsonify, make_response
import sys
import tomllib
import read_and_index
import runpy
import os
import tomllib

app = Flask(__name__)

def stderr(text):
    sys.stderr.write(f'{text}\n')

@app.route('/index-page')
def view_form():
    stderr('view_form')
    return render_template('post_data.html')

@app.route('/index', methods=['GET'])
def get():
    config = request.args.get("config")
    directory = request.args.get("dir")
    filename = request.args.get("file")
    if config==None or config=="":
        return handle_response("No config given",400)
    if (directory==None or directory=="") and (filename==None or filename==""):
        return handle_response("No file or dir given",400)
    res = read_and_index.read_and_index(toml_file=config,input_dir=directory,input_file=filename)
    if res=='OK':
        return handle_response('succes!', 200)
    else:
        return handle_response(f'failed! {res}',200)


def handle_response(text,code=400):
    resp = make_response(render_template('result.html',result=text), code)
    resp.headers['Content-Type'] = 'text/html'
    return resp


@app.route('/index', methods=['POST'])
def post():
    data = {}
    if request.method == 'POST':
        data['configfile'] = request.form['configfile']
        data['config'] = request.form['config']
        data['filetext'] = request.form['filetext']
    res = ''
    config = {}
    configfile = data['configfile'].strip()
    if configfile!='':
        if not configfile.endswith('.toml'):
            configfile = f'{configfile}.toml'
        with open(configfile,'rb') as toml:
            config = tomllib.load(toml)
        extension = config['index']['input']['format']
    else:
        res += "No configfile given\n"
    if config=={}:
        config = data['config'].strip()
        if config=='':
            return handle_response("No config given, can't index")
        extension = tomllib.loads(config)['index']['input']['format']
    tobeindexed = data['filetext'].strip()
    if tobeindexed=='':
        return handle_response(f'No data to be indexed')
    try:
        directory = os.environ['PROCRUSTUS_TEMP_DIR']
    except:
        directory = '/tmp'
    if configfile=='':
        configfile = f'{directory}/config.toml'
        with open(configfile,'w') as uitvoer:
            uitvoer.write(config)
    datafile = f'{directory}/datafile.{extension}'
    with open(datafile,'w') as uitvoer:
        uitvoer.write(tobeindexed)
    res = read_and_index.read_and_index(toml_file=configfile,input_file=datafile,index_name='test-index',force=True)
    if res=='OK':
        return handle_response('succes!', 200)
    else:
        return handle_response(f'failed! {res}'), 400

if __name__ == '__main__':
      app.run(host='0.0.0.0', port=80)

