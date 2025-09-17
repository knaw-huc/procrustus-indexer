# -*- coding: utf-8 -*-
from flask import Flask,render_template,request, jsonify
import sys
import tomllib
sys.path.append("/Users/meindertkroese/git/procrustus-indexer")
import read_and_index
import runpy
import os
import tomllib

app = Flask(__name__)

def stderr(text):
    sys.stderr.write(f'{text}\n')

def read(toml_file):
    with open(toml_file, "rb") as f:
        return tomllib.load(f)

@app.route('/hello/')
def hello_world():
    stderr('hello world')
    return '<p><b>Hello, World</b></p>'

@app.route('/hello/<name>', methods=['GET'])
def hello(name=None):
    data = request.form.to_dict()
    stderr(data)
    stderr(jsonify(data))
    return render_template('hello.html', person=name)

@app.route('/index-page')
def view_form():
    stderr('view_form')
    return render_template('post_data.html')

@app.route('/index', methods=['GET'])
def get():
    config = request.args.get("config")
    directory = request.args.get("dir")
    filename = request.args.get("file")
    stderr(filename.__class__)
    if config==None or config=="":
        return {"response": "No config given"}
    if (directory==None or directory=="") and (filename==None or filename==""):
        return {"response": "No file or dir given"}
    # Nu mbv config en file/dir read_and_index.py aanroepen
    args = " ".join(['-t',config,'-d',directory])
    os.system(f"exec python3 ./read_and_index.py {args}")
    # evt result in reponse opnemen
    return {"response": f"This is a GET request\nusing {config}, index contents of {directory} and/or file {filename}"}

@app.route('/index', methods=['POST'])
def post():
    data = {}
    if request.method == 'POST':
        data['configfile'] = request.form['configfile']
        data['config'] = request.form['config']
        data['filetext'] = request.form['filetext']
    stderr('data read')
#    data = request.get_json()
    #stderr(data['config'])
    res = ''
    config = ''
    configfile = data['configfile'].strip()
    if configfile!='':
        if not configfile.endswith('.toml'):
            configfile = f'{configfile}.toml'
        extension = tomllib.load(f)['index']['input']['format']
#        config = read(configfile)
    else:
        res += "No configfile given\n"
    if config.strip()=='':
        config = data['config'].strip()
        res += f'{config}\n'
        if config=='':
            return f"<p>{res}<br/>No config given, can't index</p>"
        extension = tomllib.loads(config)['index']['input']['format']
    tobeindexed = data['filetext'].strip()
    if tobeindexed=='':
        return f'{res}No data to be indexed'
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
    # indien geen configfile: configdata naar /tmp schrijven
    # tobeindexed naar /tmp schrijven


    #stderr(jsonify(data))
    #res = runpy.run_path(f'read_and_index.py -t {configfile} -d {directory}')
#    res = runpy.run_path(f'read_and_index.py -t {configfile} -f {datafile}')
    sys.argv += ['-f',datafile,'-d',directory,'-i','test-index','--force']
    res = read_and_index.read_and_index(toml_file=configfile,input_file=datafile,index_name='test-index',force=True)
    if res==0:
        result = 'succes!'
    else:
        result = 'failed!'
    return render_template('result.html', result=result)

if __name__ == '__main__':
      app.run(host='0.0.0.0', port=80)



