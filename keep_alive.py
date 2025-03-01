# This file is no longer needed as the bot's webhook server keeps the repl alivefrom flask import Flask, request, Response
from threading import Thread

app = Flask(__name__)


@app.route('/', methods=['GET', 'HEAD'])
def home():
  if request.method == 'HEAD':
    # Возвращаем пустой 200 OK без тела,
    # чтобы UptimeRobot видел, что сайт "жив"
    return Response(status=200)
  # При GET-запросе отобразим обычный контент
  return "Хроносфера активна"


def run():
  app.run(host="0.0.0.0", port=8080)


def keep_alive():
  t = Thread(target=run)
  t.start()
