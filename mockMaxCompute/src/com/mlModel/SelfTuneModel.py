#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 13 17:06:04 2020

@author: vcroopana
"""
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import pandas as pd
import json
from sklearn.model_selection import train_test_split

# x = np.array([5, 15, 25, 35, 45, 55]).reshape((-1, 1))
# y = np.array([5, 20, 14, 32, 22, 38])
from flask import Flask, jsonify, request
import math

def getLinearRegModel(x, y):
    
    model = LinearRegression()
    model.fit(x, y)
    r_sq = model.score(x, y)
    print('coefficient of determination:', r_sq)
    #attributes of model
    print('intercept:', model.intercept_)
    print('slope:', model.coef_)
    
    return model

def getPolyRegModel(x, y):
    x_ = PolynomialFeatures(degree=2, include_bias=False).fit_transform(x)
    poly_model  = LinearRegression().fit(x_, y)
    r_sq = poly_model.score(x_, y)
    print('coefficient of determination:', r_sq)
    print('intercept:', model.intercept_)
    print('coefficients:', model.coef_)

    return poly_model



app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/add/<params>', methods = ['GET'])
def add_numbers(params):
    #params is expected to be a dictionary: {'x': 1, 'y':2}
    print("before printing params")

    params = eval(params)
    print("printing params")
    print(params)

    consistency =0
    
    startTime = params['startTime']
    endTime = params['endTime']
    writeQ = params['writeQ']
    numNodes= params['numNodes']
    requestType = params['requestType']
    if 'consistency' in params:
        consistency = params['consistency']
    latency = endTime - startTime
    
    reqTypeDict = {'INSERT':1,'EDIT':2, 'DELETE':3, 'READ_RESPONSE':4, 'TPC_READ':5}
    requestType = reqTypeDict[requestType]
    
    test_x = np.array([latency, writeQ, numNodes, consistency]).reshape((1, -1))
    y_pred = model.predict(test_x)[0]
    print("Predicted quorum:"+ str(y_pred))
    return str(math.ceil(y_pred))


if __name__ == '__main__':
    filePath = "/Users/vcroopana/Downloads/Fall2020/AdvDistSys/logData/log_data.json"
    json_lines = []
    print('running file')
    with open(filePath) as f:
        for line in f:
            j_content = json.loads(line)
            for single_j in j_content:
                json_lines.append(single_j)
                data_df = pd.DataFrame()   
                
    data_df = pd.DataFrame(json_lines)
    print(data_df.shape)
    data_df.head(25)
    reqTypeDict = {'INSERT':1,'EDIT':2, 'DELETE':3, 'READ_RESPONSE':4, 'TPC_READ':5}
    data_df['reqType'] = data_df['requestType'].apply(lambda x: reqTypeDict[x])
    data_df['latency'] = data_df['endTime'] - data_df['startTime']
    
    train, test = train_test_split(data_df, test_size=0.2, random_state=20)
    
    inputCols = ['latency', 'writeQ', 'numNodes', 'reqType']
    
    model = getLinearRegModel(train[inputCols], train['writeQ'])
    poly_model = getPolyRegModel(train[inputCols], train['writeQ'])
    
    app.run(debug=True)