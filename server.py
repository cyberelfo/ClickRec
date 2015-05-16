#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask, jsonify
import stream_bitarray_recommend as sbr

app = Flask(__name__)

@app.route("/recommend/<doc_id>")
def recommend(doc_id):
	result = sbr.calc(doc_id, 4, 10, 100000, 1)
	return jsonify(result=result)

if __name__ == "__main__":
   	app.run(host="127.0.0.1", port=5001)