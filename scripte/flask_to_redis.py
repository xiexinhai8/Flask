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

COUNTRY = "country"
LANGUAGE = "language"
TRENDING_IDS = "trending_ids"


def validate(data):
    try:
        data_dic = json.loads(data)
        country = data_dic.get(COUNTRY, "")
        language = data_dic.get(LANGUAGE, "")
        if not country or not language:
            app.logger.warning("validate data failed country=%s language=%s" % (country, language))
            return False
        if not data_dic(TRENDING_IDS, ""):
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

    data = json.loads(data)
    response = redis_cli.set("external_hot_topic_mapping_%s_%s" % (data.get(COUNTRY), data.get(LANGUAGE)), value=json.dumps(json.get(TRENDING_IDS)))
    app.logger.info("====== %s" % response)
    return make_response(json.dumps({"status": 200}), 200)


if __name__ != "__main__":
    gunicore_logger = logging.getLogger("gunicore.error")
    app.logger.handlers = gunicore_logger.handlers
    app.logger.setLevel(gunicore_logger.level)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000', debug=True)
