from flask import Flask, render_template, request, redirect, url_for
import json
from kafka import KafkaProducer

app = Flask(__name__, template_folder='/home/ebraheem/Documents/BDA_PROJECT/templates')
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        # Here you might want to add the user to a database or something
        return redirect(url_for('login'))
    return render_template('signup.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Here you might want to verify user credentials
        return redirect(url_for('play_music'))
    return render_template('login.html')

@app.route('/play_music')
def play_music():
    # Dummy user object
    user = {'name': 'Ibrahim'}
    track_id = '123'  # Simulated track ID
    return render_template('play_music.html', user=user, track_id=track_id)

@app.route('/get_recommendations')
def get_recommendations():
    # This could dynamically fetch recommendations based on user's history or preferences
    return render_template('recommendations.html')

@app.route('/final')
def final():
    # The final page might show a summary or final message
    return render_template('final.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
