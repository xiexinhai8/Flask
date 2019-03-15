# !/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import logging
import json
import redis
from flask import Flask, request, make_response

app = Flask(__name__)

redis_host = sys.argv[1]
redis_port = sys.argv[2]
pool = redis.ConnectionPool(host=redis_host, port=redis_port)
redis_cli = redis.StrictRedis(connection_pool=pool)


def validate(data):
    try:
        data_dic = json.loads(data)
        country = data_dic.get("country", "")
        language = data_dic.get("language", "")
        if not country or not language:
            app.logger.warning("validate data failed country=%s language=%s" % (country, language))
            return False
        if not data_dic("trending_info", ""):
            app.logger.warning("validate data failed trending data is Empty")
            return False
    except Exception, e:
        app.logger.error("validate data error %s" % e)
    return True


@app.route("/dailyhunt/trending/", methods=['POST', 'GET'])
def to_redis():
    data = request.get_data()
    if not validate(data):
        return make_response(json.dumps({"status": 400, "reason": "data invalidate"}), 200)

    response = redis_cli.set("name", value=data)
    app.logger.info("====== %s" % response)
    return make_response(json.dumps({"status": 200}), 200)


if __name__ != "__main__":
    gunicore_logger = logging.getLogger("gunicore.error")
    app.logger.handlers = gunicore_logger.handlers
    app.logger.setLevel(gunicore_logger.level)