import datetime

from flask import Flask, render_template, request

from generate_html import generate

app = Flask(__name__)


@app.route("/")
def form():
    return render_template("form.html")


@app.route("/data/", methods=["POST", "GET"])
def data():
    if request.method == "GET":
        try:
            with open("templates/table.html", "r") as f:
                data = f.read()
        except Exception as e:
            data = f"<p> submit date first {repr(e)} </p>"
        return data

    if request.method == "POST":
        form_data = request.form
        start, end = form_data["start_date"], form_data["end_date"]

        try:
            datetime.datetime.strptime(start, "%Y-%m-%d")
            datetime.datetime.strptime(end, "%Y-%m-%d")
        except ValueError:
            return "<p> invalid format </p"

        # do the html
        try:
            html = generate(start, end)

        except Exception as e:

            html = f"<p> something went wrong :{repr(e)} </p>"

        return html


if __name__ == "__main__":
    app.run()
